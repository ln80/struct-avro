package tool_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	avro "github.com/ln80/avro/v2"
	stravro "github.com/ln80/struct-avro"
	"github.com/ln80/struct-avro/fs"
	bankv1 "github.com/ln80/struct-avro/testdata/v1"
	"github.com/ln80/struct-avro/tool"
	internal "github.com/ln80/struct-avro/tool/internal"
)

// envelope mirrors event-store's avroEvent exported fields, tags and order.
type envelope struct {
	FStreamID         string        `avro:"StmID"`
	FRawGlobalVersion string        `avro:"GVer"`
	FRawVersion       string        `avro:"Ver"`
	FID               string        `avro:"ID"`
	FType             string        `avro:"Type"`
	FAt               int64         `avro:"At"`
	FUser             string        `avro:"User"`
	FIPAddr           string        `avro:"IPAddr"`
	FDests            []string      `avro:"Dests"`
	FTTL              time.Duration `avro:"TTL"`
	FRawEvent         any           `avro:"Data" ev:",inject=union"`
}

func bankV1Schema(t *testing.T) *avro.RecordSchema {
	t.Helper()
	sch, err := stravro.BuildEnvelopeSchema(stravro.NewAPI(), stravro.EnvelopeConfig{
		Namespace: "bank",
		RootType:  reflect.TypeOf(envelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries: []stravro.RecordEntry{
			{Name: "bank.AccountOpened", Type: reflect.TypeOf(bankv1.AccountOpened{}), Sample: bankv1.AccountOpened{}},
			{Name: "bank.MoneyDeposited", Type: reflect.TypeOf(bankv1.MoneyDeposited{}), Sample: bankv1.MoneyDeposited{}},
			{Name: "bank.MoneyWithdrawn", Type: reflect.TypeOf(bankv1.MoneyWithdrawn{}), Sample: bankv1.MoneyWithdrawn{}},
		},
		Validate: true,
	})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	return sch
}

// silentPrinter satisfies internal.TaskPrinter without emitting output.
type silentPrinter struct{}

func (silentPrinter) Message(string, *internal.Task) {}
func (silentPrinter) Error(error, *internal.Task)    {}
func (silentPrinter) Task(int, internal.Task)        {}

func TestJobExecuterEndToEnd(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := filepath.Join(root, "registry")
	if err := os.MkdirAll(store, 0o750); err != nil {
		t.Fatal(err)
	}
	adapter := fs.NewDirAdapter(store)

	out := filepath.Join(root, "schemas")

	sch := bankV1Schema(t)

	job := tool.NewJobExecuter(silentPrinter{})
	job.
		GenerateSchemas(func(context.Context) (tool.SchemaMap, error) {
			return tool.SchemaMap{"bank": sch}, nil
		}).
		CheckCompatibility(adapter).
		PersistSchemas(adapter, adapter).
		EmbedSchemas(adapter, adapter, tool.EmbedConfig{Out: out})

	if err := job.Execute(ctx); err != nil {
		t.Fatalf("execute job: %v", err)
	}

	// The schema must have been persisted under the registry store.
	id, err := adapter.GetByDefinition(ctx, sch)
	if err != nil {
		t.Fatalf("schema not persisted: %v", err)
	}
	if id == "" {
		t.Fatal("empty schema id after persist")
	}

	// Embed must have generated Go event types + an embed.FS accessor.
	eventsGo := filepath.Join(out, "bank", "events.go")
	b, err := os.ReadFile(eventsGo)
	if err != nil {
		t.Fatalf("read generated events.go: %v", err)
	}
	src := string(b)
	for _, want := range []string{"package bank", "type AccountOpened struct", "type MoneyDeposited struct"} {
		if !strings.Contains(src, want) {
			t.Fatalf("generated events.go missing %q:\n%s", want, src)
		}
	}
	// The default (generic) template must NOT pull in event-store.
	if strings.Contains(src, "event-store") {
		t.Fatalf("generated code unexpectedly references event-store:\n%s", src)
	}

	if _, err := os.Stat(filepath.Join(out, "init.go")); err != nil {
		t.Fatalf("init.go (embed.FS accessor) not generated: %v", err)
	}
}
