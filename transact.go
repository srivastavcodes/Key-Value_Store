package main

type EventType byte

const (
	_                     = iota // iota == 0; ignore the zero value
	EventDelete EventType = iota // iota == 1;
	EventPut                     // iota == 2; implicitly repeat
)

type Event struct {
	Sequence  int64
	EventType EventType
	Key       string
	Value     string
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
}

type FileTransactionLogger struct {
}

func (f *FileTransactionLogger) WriteDelete(key string) {}

func (f *FileTransactionLogger) WritePut(key, value string) {}
