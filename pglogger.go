package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

type PostgresTransactionLogger struct {
	events chan<- Event // write-only channel for sending Events
	errors <-chan error // read-only channel for receiving errors
	db     *sql.DB      // The database access interface
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf(
		"host=%s dbname=%s user=%s password=%s",
		config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping db connection: %w", err)
	}
	logger := &PostgresTransactionLogger{db: db}

	// todo -> figure out the below methods using database/sql package docs

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}
	return logger, nil
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
