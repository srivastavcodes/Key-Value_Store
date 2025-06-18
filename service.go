package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
)

var logger TransactionLogger

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if err != nil {
		switch {
		case errors.Is(err, ErrNoSuchKey):
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	_, _ = fmt.Fprint(w, value)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = fmt.Fprint(w, "entry deleted successfully")
}

func initializeTransactionLog() error {
	var err error

	logger, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}
	events, errs := logger.ReadEvents()

	var ev Event
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errs:
		case ev, ok = <-events:
			switch ev.EventType {
			case EventDelete:
				err = Delete(ev.Key)
			case EventPut:
				err = Put(ev.Key, ev.Value)
			}
		}
	}
	logger.Run()
	return err
}
