package tool

import (
	"fmt"
	"io"

	internal "github.com/ln80/struct-avro/tool/internal"
)

// Task is a unit of work run by the JobExecuter. It is re-exported so that callers
// outside this module can implement TaskPrinter.
type Task = internal.Task

// TaskPrinter reports the job progress. Use NoPrinter, DefaultPrinter, or your own.
type TaskPrinter = internal.TaskPrinter

// NoPrinter discards all job output.
type NoPrinter struct{}

// Task implements TaskPrinter.
func (*NoPrinter) Task(int, Task) {}

// Error implements TaskPrinter.
func (*NoPrinter) Error(error, *Task) {}

// Message implements TaskPrinter.
func (*NoPrinter) Message(string, *Task) {}

var _ TaskPrinter = &NoPrinter{}

// DefaultPrinter writes the job progress to Output and errors to Err.
type DefaultPrinter struct {
	Output io.Writer
	Err    io.Writer
}

// Task implements TaskPrinter.
func (p *DefaultPrinter) Task(i int, t Task) {
	_, _ = fmt.Fprintf(p.Output, "%d. %s\n", i, t.Name())
}

// Error implements TaskPrinter.
func (p *DefaultPrinter) Error(err error, t *Task) {
	if t == nil {
		_, _ = fmt.Fprintf(p.Err, "error: %v\n", err)
		return
	}
	_, _ = fmt.Fprintf(p.Err, "%s: error: %v\n", (*t).Name(), err)
}

// Message implements TaskPrinter.
func (p *DefaultPrinter) Message(msg string, t *Task) {
	if t == nil {
		_, _ = fmt.Fprintf(p.Output, "%s\n", msg)
		return
	}
	_, _ = fmt.Fprintf(p.Output, "%s: %s\n", msg, (*t).Name())
}

var _ TaskPrinter = &DefaultPrinter{}
