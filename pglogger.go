package main

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type PostgresTransactionLogger struct {
	events chan<- Event // write-only channel for sending Events
	errors <-chan error // read-only channel for receiving errors
	db     *sql.DB      // The database access interface
}

func (ptl *PostgresTransactionLogger) WritePut(key, value string) {
	ptl.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (ptl *PostgresTransactionLogger) WriteDelete(key string) {
	ptl.events <- Event{EventType: EventDelete, Key: key}
}

func (ptl *PostgresTransactionLogger) Err() <-chan error {
	return ptl.errors
}
