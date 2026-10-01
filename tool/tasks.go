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
	"errors"
	"fmt"
	"go/format"
	"html/template"
	"runtime/debug"
	"sort"
	"strings"

	_avro "github.com/ln80/avro/v2"
	"github.com/ln80/avro/v2/gen"
	stravro "github.com/ln80/struct-avro"
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

type EmbedSchemasTask_EmbedDestination struct {
	Out    string
	Module string
}

type EmbedSchemasTask struct {
	internal.Task

	dest EmbedSchemasTask_EmbedDestination

	initialisms []string
	// unionField is the envelope field holding the union of event records.
	unionField string
	// genTemplate is the code-generation template; empty means the built-in default.
	genTemplate string

	persister registry.Persister

	walker registry.Walker
}

// EmbedConfig configures the EmbedSchemas task.
type EmbedConfig struct {
	// Out is the destination directory for generated code.
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
		Task:   internal.NewTask(CheckCompatibility),
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
		Task:      internal.NewTask(PersistSchemas),
		persister: persister,
		fetcher:   fetcher,
	}
	e.tasks = append(e.tasks, tt)
	return e
}

// EmbedSchemas walks through the existing schemas in the registry, fetches each
// version, persists it via the given persister, and generates Go types + an embed.FS
// accessor at cfg.Out.
func (e *JobExecuter) EmbedSchemas(walker registry.Walker, persister registry.Persister, cfg EmbedConfig) *JobExecuter {
	if t := internal.TaskFrom[*EmbedSchemasTask](e.tasks); t != nil {
		return e
	}

	unionField := cfg.UnionField
	if unionField == "" {
		unionField = "Data"
	}

	tt := &EmbedSchemasTask{
		Task:      internal.NewTask(EmbedSchemas),
		walker:    walker,
		persister: persister,
		dest: EmbedSchemasTask_EmbedDestination{
			Out:    cfg.Out,
			Module: cfg.Module,
		},
		initialisms: cfg.Initialisms,
		unionField:  unionField,
		genTemplate: cfg.Template,
	}
	e.tasks = append(e.tasks, tt)
	return e
}

// Execute the job registered tasks in a logical order.
func (e *JobExecuter) Execute(ctx context.Context) error {
	sort.Slice(e.tasks, func(i, j int) bool {
		return score[e.tasks[i].Name()] <= score[e.tasks[j].Name()]
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
			err = fmt.Errorf("run task panicked: %s \nstack: %v", r, string(debug.Stack()))
			return
		}
	}()

	for _, d := range t.DependsOn() {
		if _, ok := e.done[d]; !ok {
			err = fmt.Errorf("task %s depends on %s", t, d)
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

	compat := stravro.NewCompatibilityAPI()

	_, err := tt.walker.Walk(ctx, func(id string, version int64, latest bool, schema *_avro.RecordSchema) error {
		n := schema.Namespace()

		cur, ok := schemas[n]
		if !ok {
			return nil
		}

		if err := compat.Compatible(cur, schema); err != nil {
			return fmt.Errorf("namespace: %s, incompatible with version: %d, err: %v", n, version, err)
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
	if tt.persister == nil {
		return fmt.Errorf("failed to run '%s' persister not found", tt.Name())
	}
	if tt.walker == nil {
		return fmt.Errorf("failed to run '%s' walker not found", tt.Name())
	}
	if tt.dest.Out == "" {
		return fmt.Errorf("failed to run '%s' destination output not found", tt.Name())
	}

	if err := internal.CheckDir(tt.dest.Out); err != nil {
		return err
	}

	splits := strings.Split(tt.dest.Out, "/")
	packageName := splits[len(splits)-1]

	if m := tt.dest.Module; m != "" && !strings.HasSuffix(m, packageName) {
		return fmt.Errorf("packageName '%s' must be the same as module suffix '%s'", packageName, m)
	}

	outTemplate := tt.genTemplate
	if outTemplate == "" {
		outTemplate = defaultOutputTemplate
	}

	n, err := tt.walker.Walk(ctx, func(id string, version int64, latest bool, schema *_avro.RecordSchema) error {
		opt := func(pc *registry.PersistConfig) {
			pc.Resolver = func(_ *_avro.RecordSchema) (string, int, error) {
				return id, int(version), nil
			}
		}
		if _, err := tt.persister.Persist(ctx, schema, opt); err != nil {
			return err
		}

		if !latest {
			return nil
		}

		namespace := schema.Namespace()
		dir := tt.dest.Out + "/" + namespace

		// generate event types from the latest
		g := gen.NewGenerator(
			schema.Namespace(), nil,
			gen.WithFullName(false),
			gen.WithEncoders(false),
			gen.WithTemplate(outTemplate),
			gen.WithInitialisms(tt.initialisms),
		)

		schemas, err := stravro.UnpackUnionRecords(schema, tt.unionField)
		if err != nil {
			return err
		}
		for _, sc := range schemas {
			g.Parse(sc)
		}

		var buf bytes.Buffer
		if err = g.Write(&buf); err != nil {
			return err
		}
		pretty, err := format.Source(buf.Bytes())
		if err != nil {
			return err
		}
		if err := internal.CheckDir(dir); err != nil {
			return err
		}
		if err := internal.WriteToFile(dir+"/"+"events.go", pretty); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}

	if n == 0 {
		return nil
	}

	// generate code that exposes the embedded file system
	data := struct {
		PackageName string
	}{
		PackageName: packageName,
	}
	b, err := internal.RenderCode(embedSchemaTmpl, data)
	if err != nil {
		return err
	}
	if err := internal.WriteToFile(tt.dest.Out+"/"+"init.go", b); err != nil {
		return err
	}

	// setup go module
	if tt.dest.Module != "" {
		if err := internal.SetupModule(tt.dest.Module, tt.dest.Out); err != nil {
			return err
		}
	}

	return nil
}

var embedSchemaTmpl = template.Must(template.New("embed-schema-tmpl").Parse(`
/*
 * Code Generated by ln80/struct-avro. DO NOT EDIT.
 */
package {{.PackageName}}

import "embed"

var (
	//go:embed */*.json
	schemas embed.FS
)

func EmbedFS() embed.FS {
	return schemas
}
`))
