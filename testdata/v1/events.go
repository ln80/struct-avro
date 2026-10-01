// Package bank (v1) mirrors event-store's avro/testdata/v1 events, without any
// event-store dependency, for schema-evolution tests.
package bank

type User struct {
	ID    string `pii:"subjectID"`
	Email string `pii:"data"`
}

type AccountOpened struct {
	ID    string
	Owner User `pii:"dive"`
	At    int64
}

type MoneyDeposited struct {
	Amount    int
	AccountID string
	At        int64
}

type MoneyWithdrawn struct {
	Amount    int
	AccountID string
	At        int64
}
