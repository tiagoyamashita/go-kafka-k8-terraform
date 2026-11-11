package store

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned when a transfer id is not in the store.
var ErrNotFound = errors.New("transfer not found")

func errNotPublishing(id string) error {
	return fmt.Errorf("transfer %s is not publishing", id)
}
