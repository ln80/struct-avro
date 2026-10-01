// Package bank (v2) mirrors event-store's avro/testdata/v2 events, without any
// event-store dependency, for schema-evolution tests.
package bank

type User struct {
	ID       string `pii:"subjectID"`
	Email    string `pii:"data"`
	BirthDay string `pii:"data,replace=00-00"`
}

type AccountOpened struct {
	ID     string
	Client User `pii:"dive" ev:",aliases=Owner"`
	At     int64
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

const DEFAULT_BIRTH_DAY = "01-01"
