// Package tool provides a generic, ordered job runner over a set of AVRO schema
// tasks: generate schemas, check backward compatibility against persisted versions,
// persist new schemas, and embed/generate Go types from persisted schemas.
//
// Unlike event-store's variant, this package does not walk an event registry. The
// caller supplies the schemas (via a builder passed to GenerateSchemas) and, for
// code generation, an optional template. This keeps the tool independent of any
// particular event framework while remaining usable as the engine behind one.
package tool

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"go/token"
	"path"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"text/template"

	_avro "github.com/ln80/avro/v2"
	"github.com/ln80/avro/v2/gen"
	"github.com/ln80/struct-avro"
	"github.com/ln80/struct-avro/fs"
	"github.com/ln80/struct-avro/registry"
	internal "github.com/ln80/struct-avro/tool/internal"
)

//go:embed default_template.tmpl
var defaultOutputTemplate string

// SchemaMap maps a namespace to its generated envelope schema.
type SchemaMap map[string]*_avro.RecordSchema

// SchemaBuilder produces the set of schemas to operate on, keyed by namespace.
// The caller implements this (e.g. by assembling envelope schemas from a registry).
type SchemaBuilder func(ctx context.Context) (SchemaMap, error)

// Avro Schema Supported tasks
const (
	GenerateSchemas    = "GenerateSchemas"
	CheckCompatibility = "CheckCompatibility"
	PersistSchemas     = "PersistSchemas"
	EmbedSchemas       = "EmbedSchemas"
)

// score presents the order of tasks execution.
var score = map[string]int{
	GenerateSchemas:    0,
	CheckCompatibility: 1,
	PersistSchemas:     2,
	EmbedSchemas:       3,
}

type GenerateSchemasTask struct {
	internal.Task

	builder SchemaBuilder
	schemas SchemaMap
}

type CheckCompatibilityTask struct {
	internal.Task

	walker registry.Walker
}

type PersistSchemasTask struct {
	internal.Task

	persister registry.Persister
	fetcher   registry.Fetcher
}

type EmbedSchemasTask struct {
	internal.Task

	cfg EmbedConfig

	walker registry.Walker
}

// EmbedConfig configures the EmbedSchemas task.
type EmbedConfig struct {
	// Out is the destination directory. Its base name is the package name of
	// the generated embed.go, and each namespace gets a sub-package
	// Out/<namespace> holding its schema versions and events.go.
	Out string
	// Module, if non-empty, is the Go module path to initialise at Out.
	Module string
	// Initialisms passed to the avro code generator.
	Initialisms []string
	// UnionField is the envelope field name holding the event union (default "Data").
	UnionField string
	// Template overrides the built-in code-generation template.
	Template string
}

type JobExecuter struct {
	tasks   []internal.Task
	done    map[string]bool
	printer internal.TaskPrinter
}

// NewJobExecuter returns an empty job. A nil printer discards the job output.
func NewJobExecuter(printer TaskPrinter) *JobExecuter {
	if printer == nil {
		printer = &NoPrinter{}
	}
	return &JobExecuter{
		tasks:   make([]internal.Task, 0),
		done:    make(map[string]bool),
		printer: printer,
	}
}

// GenerateSchemas registers a task that builds the schemas to operate on using the
// supplied builder. The builder is invoked when the job runs.
func (e *JobExecuter) GenerateSchemas(builder SchemaBuilder) *JobExecuter {
	if t := internal.TaskFrom[*GenerateSchemasTask](e.tasks); t != nil {
		return e
	}

	tt := &GenerateSchemasTask{
		Task:    internal.NewTask(GenerateSchemas),
		builder: builder,
		schemas: make(SchemaMap),
	}

	e.tasks = append(e.tasks, tt)

	return e
}

// CheckCompatibility checks the compatibility of the current generated schemas against their
// previous versions if they already exist.
func (e *JobExecuter) CheckCompatibility(walker registry.Walker) *JobExecuter {
	if t := internal.TaskFrom[*CheckCompatibilityTask](e.tasks); t != nil {
		return e
	}

	tt := &CheckCompatibilityTask{
		Task:   internal.NewTask(CheckCompatibility, GenerateSchemas),
		walker: walker,
	}
	e.tasks = append(e.tasks, tt)
	return e
}

// PersistSchemas registers the current generated schemas in the given registry.
// It uses the fetcher service to deduplicate schemas.
func (e *JobExecuter) PersistSchemas(fetcher registry.Fetcher, persister registry.Persister) *JobExecuter {
	if t := internal.TaskFrom[*PersistSchemasTask](e.tasks); t != nil {
		return e
	}

	tt := &PersistSchemasTask{
		Task:      internal.NewTask(PersistSchemas, GenerateSchemas),
		persister: persister,
		fetcher:   fetcher,
	}
	e.tasks = append(e.tasks, tt)
	return e
}

// EmbedSchemas walks every persisted schema version and writes, under cfg.Out:
//
//   - <namespace>/<version>@<id>.json: a copy of each version;
//   - <namespace>/events.go: Go types generated from the latest version;
//   - embed.go: an EmbedFS function exposing the copies as an embed.FS.
//
// The walker is usually the schema store PersistSchemas writes to. Out may be
// that same directory.
func (e *JobExecuter) EmbedSchemas(walker registry.Walker, cfg EmbedConfig) *JobExecuter {
	if t := internal.TaskFrom[*EmbedSchemasTask](e.tasks); t != nil {
		return e
	}

	if cfg.UnionField == "" {
		cfg.UnionField = "Data"
	}

	tt := &EmbedSchemasTask{
		Task:   internal.NewTask(EmbedSchemas),
		walker: walker,
		cfg:    cfg,
	}
	e.tasks = append(e.tasks, tt)
	return e
}

// Execute the job registered tasks in a logical order.
func (e *JobExecuter) Execute(ctx context.Context) error {
	sort.SliceStable(e.tasks, func(i, j int) bool {
		return score[e.tasks[i].Name()] < score[e.tasks[j].Name()]
	})

	e.printer.Message("\nAbout to run:\n", nil)
	for i, t := range e.tasks {
		e.printer.Task(i+1, t)
	}
	e.printer.Message("\n", nil)

	for i, t := range e.tasks {
		if e.done[t.Name()] {
			continue
		}

		e.printer.Task(i+1, t)

		e.printer.Message("Started...", nil)
		if err := e.executeTask(ctx, t); err != nil {
			e.printer.Error(err, &t)
			return err
		}
		e.printer.Message("Done\n\n", nil)
	}

	e.printer.Message("The job is done", nil)

	return nil
}

func (e *JobExecuter) executeTask(ctx context.Context, t internal.Task) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("task %s panicked: %v\nstack: %s", t.Name(), r, debug.Stack())
			return
		}
	}()

	for _, d := range t.DependsOn() {
		if _, ok := e.done[d]; !ok {
			err = fmt.Errorf("task %s requires task %s", t.Name(), d)
			return
		}
	}

	switch tt := t.(type) {
	case *GenerateSchemasTask:
		if err = tt.Run(ctx); err != nil {
			return
		}

	case *CheckCompatibilityTask:
		t0 := internal.TaskFrom[*GenerateSchemasTask](e.tasks)
		if err = tt.Run(ctx, t0.schemas); err != nil {
			return
		}

	case *PersistSchemasTask:
		t0 := internal.TaskFrom[*GenerateSchemasTask](e.tasks)
		if err = tt.Run(ctx, t0.schemas); err != nil {
			return
		}

	case *EmbedSchemasTask:
		if err = tt.Run(ctx); err != nil {
			return
		}
	}

	e.done[t.Name()] = true

	return
}

func (tt *GenerateSchemasTask) Run(ctx context.Context, deps ...any) error {
	if tt.builder == nil {
		return fmt.Errorf("failed to run '%s': schema builder not found", tt.Name())
	}
	curs, err := tt.builder(ctx)
	if err != nil {
		return err
	}
	for ns, sch := range curs {
		if sch == nil {
			return fmt.Errorf("failed to run '%s': namespace %s has no schema", tt.Name(), ns)
		}
		if sch.Namespace() != ns {
			return fmt.Errorf("failed to run '%s': schema %s is registered under namespace %s", tt.Name(), sch.FullName(), ns)
		}
	}
	if len(curs) > 0 {
		tt.schemas = curs
	}

	return nil
}

func (tt *CheckCompatibilityTask) Run(ctx context.Context, deps ...any) error {
	if len(deps) < 1 {
		return fmt.Errorf("failed to run '%s' dependency missing", tt.Name())
	}
	schemas, ok := deps[0].(SchemaMap)
	if !ok {
		return fmt.Errorf("failed to run '%s' invalid dependency type %T", tt.Name(), deps[0])
	}
	if tt.walker == nil {
		return fmt.Errorf("failed to run '%s' walker not found", tt.Name())
	}

	compat := structavro.NewCompatibilityAPI()

	_, err := tt.walker.Walk(ctx, func(id string, version int64, latest bool, schema *_avro.RecordSchema) error {
		n := schema.Namespace()

		cur, ok := schemas[n]
		if !ok {
			return nil
		}

		if err := compat.Compatible(cur, schema); err != nil {
			return fmt.Errorf("namespace %s: current schema cannot read version %d (%s): %w", n, version, id, err)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (tt *PersistSchemasTask) Run(ctx context.Context, deps ...any) error {
	if len(deps) < 1 {
		return fmt.Errorf("failed to run '%s' dependency missing", tt.Name())
	}
	schemas, ok := deps[0].(SchemaMap)
	if !ok {
		return fmt.Errorf("failed to run '%s' invalid dependency type %T", tt.Name(), deps[0])
	}
	if tt.persister == nil {
		return fmt.Errorf("failed to run '%s' persister not found", tt.Name())
	}

	for _, s := range schemas {
		if tt.fetcher != nil {
			if _, err := tt.fetcher.GetByDefinition(ctx, s); err != nil {
				if !errors.Is(err, registry.ErrSchemaNotFound) {
					return err
				}
				if _, err := tt.persister.Persist(ctx, s); err != nil {
					return err
				}
				continue
			}
		} else {
			if _, err := tt.persister.Persist(ctx, s); err != nil {
				return err
			}
		}
	}

	return nil
}

func (tt *EmbedSchemasTask) Run(ctx context.Context, _ ...any) error {
	if tt.walker == nil {
		return fmt.Errorf("failed to run '%s' walker not found", tt.Name())
	}
	if tt.cfg.Out == "" {
		return fmt.Errorf("failed to run '%s' destination output not found", tt.Name())
	}

	out := filepath.Clean(tt.cfg.Out)
	packageName := filepath.Base(out)
	if err := internal.PackageName(packageName); err != nil {
		return fmt.Errorf("output directory %s: %w", out, err)
	}
	if m := tt.cfg.Module; m != "" && path.Base(m) != packageName {
		return fmt.Errorf("%w: module %s must end with the package name %s", internal.ErrInvalidOutput, m, packageName)
	}

	outTemplate := tt.cfg.Template
	if outTemplate == "" {
		outTemplate = defaultOutputTemplate
	}

	if err := internal.CheckDir(out); err != nil {
		return err
	}
	dest := fs.NewDirAdapter(out)

	n, err := tt.walker.Walk(ctx, func(id string, version int64, latest bool, schema *_avro.RecordSchema) error {
		namespace := schema.Namespace()
		if err := internal.PackageName(namespace); err != nil {
			return fmt.Errorf("namespace of schema %s: %w", id, err)
		}

		// Copy the version. Persisting the same version and ID again is a
		// no-op, so Out may be the store itself.
		opt := func(pc *registry.PersistConfig) {
			pc.Resolver = func(_ *_avro.RecordSchema) (string, int, error) {
				return id, int(version), nil
			}
		}
		if _, err := dest.Persist(ctx, schema, opt); err != nil {
			return err
		}

		if !latest {
			return nil
		}

		records, err := structavro.UnpackUnionRecords(schema, tt.cfg.UnionField)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return fmt.Errorf("schema %s: no records in union field %s", id, tt.cfg.UnionField)
		}
		for _, rec := range records {
			if err := checkCodegenSafe(rec, make(map[string]bool)); err != nil {
				return fmt.Errorf("schema %s: %w", id, err)
			}
		}

		// Generate Go types from the latest version.
		g := gen.NewGenerator(
			namespace, nil,
			gen.WithFullName(false),
			gen.WithEncoders(false),
			gen.WithTemplate(outTemplate),
			gen.WithInitialisms(tt.cfg.Initialisms),
		)
		for _, rec := range records {
			g.Parse(rec)
		}

		var buf bytes.Buffer
		if err = g.Write(&buf); err != nil {
			return err
		}
		src, err := internal.FormatSource(buf.Bytes())
		if err != nil {
			return fmt.Errorf("format generated code for namespace %s: %w", namespace, err)
		}
		return internal.WriteToFile(filepath.Join(out, namespace, "events.go"), src)
	})
	if err != nil {
		return err
	}

	if n == 0 {
		return nil
	}

	// Generate the file that exposes the embedded schemas. It replaces the
	// init.go that older versions generated.
	b, err := internal.RenderCode(embedSchemaTmpl, struct{ PackageName string }{packageName})
	if err != nil {
		return err
	}
	if err := internal.RemoveGenerated(filepath.Join(out, "init.go")); err != nil {
		return err
	}
	if err := internal.WriteToFile(filepath.Join(out, "embed.go"), b); err != nil {
		return err
	}

	if tt.cfg.Module != "" {
		if err := internal.SetupModule(ctx, tt.cfg.Module, out); err != nil {
			return err
		}
	}

	return nil
}

// checkCodegenSafe rejects schema text that would change the meaning of the
// generated code: a doc with a line break ends the Go comment it is written in,
// and a sensitive property value with a quote, backquote, comma or space breaks
// out of the struct tag. Schemas this module generates never contain them; a
// schema read from a shared registry might.
func checkCodegenSafe(sch _avro.Schema, seen map[string]bool) error {
	switch s := sch.(type) {
	case *_avro.RecordSchema:
		if seen[s.FullName()] {
			return nil
		}
		seen[s.FullName()] = true
		if err := checkDoc(s.FullName(), s.Doc()); err != nil {
			return err
		}
		for _, f := range s.Fields() {
			where := s.FullName() + "." + f.Name()
			if err := checkDoc(where, f.Doc()); err != nil {
				return err
			}
			if err := checkTagProp(where, f.Prop("sensitive")); err != nil {
				return err
			}
			if err := checkCodegenSafe(f.Type(), seen); err != nil {
				return err
			}
		}
	case *_avro.RefSchema:
		return checkCodegenSafe(s.Schema(), seen)
	case *_avro.ArraySchema:
		return checkCodegenSafe(s.Items(), seen)
	case *_avro.MapSchema:
		return checkCodegenSafe(s.Values(), seen)
	case *_avro.UnionSchema:
		for _, t := range s.Types() {
			if err := checkCodegenSafe(t, seen); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkDoc(where, doc string) error {
	if strings.ContainsAny(doc, "\r\n") {
		return fmt.Errorf("%w: doc of %s has a line break", internal.ErrInvalidOutput, where)
	}
	return nil
}

func checkTagProp(where string, prop any) error {
	if prop == nil {
		return nil
	}
	// A parsed schema holds the property as a JSON object; a schema built in
	// memory holds the value it was given. Compare both as JSON.
	m, ok := prop.(map[string]any)
	if !ok {
		b, err := json.Marshal(prop)
		if err == nil {
			err = json.Unmarshal(b, &m)
		}
		if err != nil {
			return fmt.Errorf("%w: sensitive property of %s is not an object", internal.ErrInvalidOutput, where)
		}
	}
	parts := []any{m["ID"], m["Name"]}
	if opts, ok := m["Options"].(map[string]any); ok {
		for k, v := range opts {
			parts = append(parts, k, v)
		}
	} else if m["Options"] != nil {
		return fmt.Errorf("%w: sensitive options of %s are not an object", internal.ErrInvalidOutput, where)
	}
	for _, p := range parts {
		str, ok := p.(string)
		if !ok || !tagTextRe(str) {
			return fmt.Errorf("%w: sensitive property of %s cannot be written as a struct tag", internal.ErrInvalidOutput, where)
		}
	}
	if id := m["ID"].(string); !token.IsIdentifier(id) {
		return fmt.Errorf("%w: sensitive property of %s cannot be written as a struct tag", internal.ErrInvalidOutput, where)
	}
	return nil
}

// tagTextRe reports whether s can sit in a struct tag value as written by the
// template: printable ASCII without quote, backquote, backslash, comma or space.
func tagTextRe(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c > '~' || c == '"' || c == '`' || c == '\\' || c == ',' {
			return false
		}
	}
	return true
}

var embedSchemaTmpl = template.Must(template.New("embed-schema-tmpl").Parse(internal.GeneratedHeader + `

package {{.PackageName}}

import "embed"

//go:embed */*.json
var schemas embed.FS

// EmbedFS returns every persisted schema version, stored as
// <namespace>/<version>@<id>.json.
func EmbedFS() embed.FS {
	return schemas
}
`))
