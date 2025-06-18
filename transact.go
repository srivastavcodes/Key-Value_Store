package main

import (
	"bufio"
	"fmt"
	"os"
)

type EventType byte

const (
	_                     = iota // iota == 0; ignore the zero value
	EventDelete EventType = iota // iota == 1;
	EventPut                     // iota == 2; implicitly repeat
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Run()

	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
}

type FileTransactionLogger struct {
	events       chan<- Event // Write-only channel for sending events
	errors       <-chan error // Read-only channel for receiving errors
	lastSequence uint64       // The last used event sequence number
	file         *os.File     // The location of the transaction log
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (ftl *FileTransactionLogger) WritePut(key, value string) {
	ftl.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (ftl *FileTransactionLogger) WriteDelete(key string) {
	ftl.events <- Event{EventType: EventDelete, Key: key}
}

func (ftl *FileTransactionLogger) Err() <-chan error {
	return ftl.errors
}

func (ftl *FileTransactionLogger) Run() {
	events := make(chan Event, 16)
	ftl.events = events

	errors := make(chan error, 1)
	ftl.errors = errors

	go func() {
		for ev := range events { // Receive the next event
			ftl.lastSequence++

			_, err := fmt.Fprintf(ftl.file, // Write the event to log
				"%d\t%d\t%s\t%s\n", ftl.lastSequence, ev.EventType, ev.Key, ev.Value)
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (ftl *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(ftl.file)
	outEvent := make(chan Event) // An unbuffered event channel
	outError := make(chan error, 1)

	go func() {
		var ev Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			_, err := fmt.Sscanf(line,
				"%d\t%d\t%s\t%s", &ev.Sequence, &ev.EventType, &ev.Key, &ev.Value)
			if err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			// Sanity check! Are the sequence numbers in increasing order?
			if ftl.lastSequence >= ev.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}
			ftl.lastSequence = ev.Sequence // update the last sequence used
			outEvent <- ev
		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()
	return outEvent, outError
}
