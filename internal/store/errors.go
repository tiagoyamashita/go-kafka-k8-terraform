package store

import "fmt"

func errNotPublishing(id string) error {
	return fmt.Errorf("transfer %s is not publishing", id)
}
