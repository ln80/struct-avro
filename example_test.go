package structavro_test

import (
	"fmt"
	"reflect"

	structavro "github.com/ln80/struct-avro"
)

type User struct {
	ID    string
	Email string `pii:"data"`
}

func ExampleSchemaOf() {
	schema, err := structavro.SchemaOf(
		reflect.TypeOf(User{}),
		structavro.WithName("User"),
		structavro.WithNamespace("demo"),
	)
	if err != nil {
		panic(err)
	}

	api := structavro.NewAPI()
	b, err := api.Marshal(schema, User{ID: "u1", Email: "a@example.com"})
	if err != nil {
		panic(err)
	}
	var out User
	if err := api.Unmarshal(schema, b, &out); err != nil {
		panic(err)
	}
	fmt.Println(out.ID)
	// Output: u1
}

type Envelope struct {
	ID   string `avro:"ID"`
	Type string `avro:"Type"`
	Data any    `avro:"Data" ev:",inject=union"`
}

type AccountOpened struct {
	AccountID string
	Owner     string
}

type MoneyDeposited struct {
	AccountID string
	Amount    int
}

func ExampleBuildEnvelopeSchema() {
	api := structavro.NewAPI()
	schema, err := structavro.BuildEnvelopeSchema(api, structavro.EnvelopeConfig{
		Namespace: "demo",
		RootType:  reflect.TypeOf(Envelope{}),
		RootName:  "events",
		InjectKey: "union",
		Entries: []structavro.RecordEntry{
			{Name: "demo.AccountOpened", Type: reflect.TypeOf(AccountOpened{}), Sample: AccountOpened{}},
			{Name: "demo.MoneyDeposited", Type: reflect.TypeOf(MoneyDeposited{}), Sample: MoneyDeposited{}},
		},
		Validate: true,
	})
	if err != nil {
		panic(err)
	}

	b, err := api.Marshal(schema, Envelope{
		ID:   "evt-1",
		Type: "demo.AccountOpened",
		Data: AccountOpened{AccountID: "acc-1", Owner: "alice"},
	})
	if err != nil {
		panic(err)
	}
	var out Envelope
	if err := api.Unmarshal(schema, b, &out); err != nil {
		panic(err)
	}
	fmt.Printf("%s %+v\n", out.Type, out.Data.(*AccountOpened))
	// Output: demo.AccountOpened &{AccountID:acc-1 Owner:alice}
}
