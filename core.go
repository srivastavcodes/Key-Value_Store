package main

import (
	"errors"
	"sync"
)

var ErrNoSuchKey = errors.New("no such key")

type LockableMap struct {
	sync.RWMutex
	hm map[string]string
}

var store = LockableMap{
	hm: make(map[string]string),
}

func Put(key, value string) error {
	store.Lock()
	defer store.Unlock()

	store.hm[key] = value
	return nil
}

func Get(key string) (string, error) {
	store.RLock()
	defer store.RUnlock()

	value, ok := store.hm[key]
	if !ok {
		return "", ErrNoSuchKey
	}
	return value, nil
}

func Delete(key string) error {
	store.Lock()
	defer store.Unlock()

	delete(store.hm, key)
	return nil
}
