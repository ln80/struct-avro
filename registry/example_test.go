package registry_test

import (
	"context"
	"fmt"
	"os"
	"reflect"

	structavro "github.com/ln80/struct-avro"
	"github.com/ln80/struct-avro/fs"
	"github.com/ln80/struct-avro/registry"
)

type Envelope struct {
	ID   string `avro:"ID"`
	Data any    `avro:"Data" ev:",inject=union"`
}

type AccountOpened struct {
	AccountID string
	Owner     string `pii:"data"`
}

func ExampleOpen() {
	ctx := context.Background()
	api := structavro.NewAPI()
	schema, err := structavro.BuildEnvelopeSchema(api, structavro.EnvelopeConfig{
		Namespace: "bank",
		RootType:  reflect.TypeOf(Envelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries: []structavro.RecordEntry{
			{Name: "bank.AccountOpened", Type: reflect.TypeOf(AccountOpened{})},
		},
	})
	if err != nil {
		panic(err)
	}

	dir, err := os.MkdirTemp("", "schemas")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// In production the schema is persisted ahead of deployment by the tool
	// package; here it is persisted on Open.
	reg, err := registry.Open(ctx, fs.NewDirAdapter(dir),
		registry.WithAPI(api),
		registry.WithCurrent(schema),
		registry.WithPersistCurrent(),
	)
	if err != nil {
		panic(err)
	}

	b, err := reg.Encode(Envelope{ID: "1", Data: AccountOpened{AccountID: "a1", Owner: "Ada"}})
	if err != nil {
		panic(err)
	}
	var out Envelope
	if _, err := reg.Decode(ctx, b, &out); err != nil {
		panic(err)
	}
	fmt.Printf("%+v\n", out.Data.(*AccountOpened))
	// Output: &{AccountID:a1 Owner:Ada}
}
