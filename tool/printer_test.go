package tool_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ln80/struct-avro/tool"
)

func TestDefaultPrinterReportsTasksAndErrors(t *testing.T) {
	var out, errOut bytes.Buffer
	job := tool.NewJobExecuter(&tool.DefaultPrinter{Output: &out, Err: &errOut})

	wantErr := errors.New("boom")
	err := job.GenerateSchemas(func(context.Context) (tool.SchemaMap, error) {
		return nil, wantErr
	}).Execute(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("want %v, got %v", wantErr, err)
	}
	if !strings.Contains(out.String(), "1. "+tool.GenerateSchemas) {
		t.Fatalf("task not reported:\n%s", out.String())
	}
	if want := tool.GenerateSchemas + ": error: boom"; !strings.Contains(errOut.String(), want) {
		t.Fatalf("want %q in error output, got:\n%s", want, errOut.String())
	}
}

func TestNilPrinterDefaultsToNoPrinter(t *testing.T) {
	err := tool.NewJobExecuter(nil).GenerateSchemas(func(context.Context) (tool.SchemaMap, error) {
		return tool.SchemaMap{}, nil
	}).Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}
