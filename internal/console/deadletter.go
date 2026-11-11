package console

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

// DeadLetter is one record the console has read from transfers.dlq.
type DeadLetter struct {
	ID        string
	At        time.Time
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Error     string
	Body      string
}

// DeadLetters keeps the newest dead-letter records for the right-hand pane.
type DeadLetters struct {
	mu    sync.Mutex
	items []DeadLetter
}

// NewDeadLetters returns an empty buffer.
func NewDeadLetters() *DeadLetters {
	return &DeadLetters{}
}

// Handle records one DLQ record. The same partition and offset replaces the earlier copy.
func (d *DeadLetters) Handle(_ context.Context, msg transfer.Inbound) error {
	item := decodeDeadLetter(msg)
	d.mu.Lock()
	defer d.mu.Unlock()
	replaced := false
	for i, existing := range d.items {
		if existing.ID == item.ID {
			d.items[i] = item
			replaced = true
			break
		}
	}
	if !replaced {
		d.items = append(d.items, item)
	}
	sort.SliceStable(d.items, func(i, j int) bool {
		return d.items[i].At.After(d.items[j].At)
	})
	if len(d.items) > historyLimit {
		d.items = d.items[:historyLimit]
	}
	return nil
}

// List returns the newest records first.
func (d *DeadLetters) List() []DeadLetter {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DeadLetter, len(d.items))
	copy(out, d.items)
	return out
}

// Get returns one record by its partition-offset id.
func (d *DeadLetters) Get(id string) (DeadLetter, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, item := range d.items {
		if item.ID == id {
			return item, true
		}
	}
	return DeadLetter{}, false
}

func decodeDeadLetter(msg transfer.Inbound) DeadLetter {
	at := msg.At
	if at.IsZero() {
		at = time.Now()
	}
	item := DeadLetter{
		ID:        fmt.Sprintf("%d-%d", msg.Partition, msg.Offset),
		At:        at,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       string(msg.Key),
		Body:      string(msg.Value),
	}
	var env struct {
		Error   string          `json:"error"`
		Message json.RawMessage `json:"message"`
		Raw     *string         `json:"raw"`
	}
	if err := json.Unmarshal(msg.Value, &env); err != nil || env.Error == "" {
		return item
	}
	if len(env.Message) == 0 && env.Raw == nil {
		return item
	}
	item.Error = env.Error
	if len(env.Message) > 0 {
		item.Body = string(env.Message)
	} else {
		item.Body = *env.Raw
	}
	return item
}

type deadView struct {
	ID    string
	At    string
	Key   string
	Error string
	Body  string
	Place string
}

func newDeadView(letter DeadLetter) deadView {
	body := letter.Body
	if json.Valid([]byte(letter.Body)) {
		body = prettyJSON(json.RawMessage(letter.Body))
	}
	reason := letter.Error
	if reason == "" {
		reason = "dead-lettered"
	}
	return deadView{
		ID:    letter.ID,
		At:    stamp(letter.At),
		Key:   letter.Key,
		Error: reason,
		Body:  body,
		Place: fmt.Sprintf("%s:%d:%d", letter.Topic, letter.Partition, letter.Offset),
	}
}
