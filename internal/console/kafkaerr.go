package console

import (
	"sync"
	"time"
)

// KafkaError is one produce the broker or the client rejected.
type KafkaError struct {
	At    time.Time
	Key   string
	Error string
}

// KafkaErrors keeps the newest produce failures for the right-hand pane.
type KafkaErrors struct {
	mu    sync.Mutex
	items []KafkaError
}

// NewKafkaErrors returns an empty buffer.
func NewKafkaErrors() *KafkaErrors {
	return &KafkaErrors{}
}

// Add records one thrown produce error, newest first.
func (k *KafkaErrors) Add(key, message string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.items = append([]KafkaError{{
		At:    time.Now(),
		Key:   key,
		Error: message,
	}}, k.items...)
	if len(k.items) > historyLimit {
		k.items = k.items[:historyLimit]
	}
}

// List returns the newest errors first.
func (k *KafkaErrors) List() []KafkaError {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]KafkaError, len(k.items))
	copy(out, k.items)
	return out
}
