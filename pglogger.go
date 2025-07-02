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
		"host=%s dbname=%s user=%s password=%s sslmode=disable",
		config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping db connection: %w", err)
	}
	logger := &PostgresTransactionLogger{db: db}

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

func (ptl *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	ptl.events = events

	errors := make(chan error, 1)
	ptl.errors = errors

	go func() {
		query := `INSERT INTO transactions (event_type, key, value) VALUES ($1, $2, $3)`

		for ev := range events {
			_, err := ptl.db.Exec(query, ev.EventType, ev.Key, ev.Value)
			if err != nil {
				errors <- err
			}
		}
	}()
}

func (ptl *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outError)

		query := `SELECT event_type, KEY, VALUE FROM transactions ORDER BY SEQUENCE`

		rows, err := ptl.db.Query(query) // get result set
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close()

		var ev Event
		for rows.Next() {
			err = rows.Scan(&ev.EventType, &ev.Key, &ev.Value)
			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}
			outEvent <- ev
		}
		if err = rows.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()
	return outEvent, outError
}

func (ptl *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transaction"
	var result string

	query := fmt.Sprintf("SELECT to_regclass('public.%s');", table)

	rows, err := ptl.db.Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() && result != table {
		_ = rows.Scan(&result)
	}
	return result == table, rows.Err()
}

func (ptl *PostgresTransactionLogger) createTable() error {
	query := `CREATE TABLE transactions(sequence BIGSERIAL PRIMARY KEY, 
                 	event_type 	SMALLINT,
		   	key 		TEXT,
		   	value 		TEXT
		   );`

	if _, err := ptl.db.Exec(query); err != nil {
		return err
	} else {
		return nil
	}
}
