// Package internal provides generic task-orchestration and code-generation helpers
// for the struct-avro schema tooling. It has no dependency on event-store.
package internal

import (
	"context"
	"strings"
)

type Task interface {
	Name() string
	DependsOn() []string
	Run(ctx context.Context, deps ...any) error
}

type task struct {
	Task
	name      string
	dependsOn []string
}

func (t *task) Name() string {
	return t.name
}

func (t *task) DependsOn() []string {
	return t.dependsOn
}

func (t *task) String() string {
	str := t.Name()

	if deps := t.dependsOn; len(deps) > 0 {
		str += " (" + strings.Join(deps, ", ") + ")"
	}

	return str
}

// NewTask returns a task named name that runs after the tasks in dependsOn.
func NewTask(name string, dependsOn ...string) Task {
	return &task{name: name, dependsOn: append([]string{}, dependsOn...)}
}

func TaskFrom[T any](ts []Task) T {
	var tt T
	for _, t := range ts {
		if _t, ok := t.(T); ok {
			tt = _t
			break
		}
	}
	return tt
}

type TaskPrinter interface {
	Message(msg string, t *Task)
	Error(err error, t *Task)
	Task(i int, t Task)
}
