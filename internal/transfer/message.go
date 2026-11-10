package transfer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Status is the outbox lifecycle of one transfer.
type Status string

const (
	StatusReceived   Status = "received"
	StatusPublishing Status = "publishing"
	StatusPublished  Status = "published"
	StatusFailed     Status = "failed"
	StatusDead       Status = "dead"
)

// Record is one transfer persisted in Postgres. Payload is the original Kafka
// message JSON, stored as jsonb.
type Record struct {
	ID          string
	MessageKey  string
	Payload     json.RawMessage
	SourceTopic string
	Partition   int
	Offset      int64
	Status      Status
	Attempts    int
	LastError   string
	LockedAt    time.Time
	ReceivedAt  time.Time
	PublishedAt time.Time
}

// Event is one status change recorded for a transfer.
type Event struct {
	Status Status
	Detail string
	At     time.Time
}

// Inbound is one Kafka record handed to the receiver.
type Inbound struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	At        time.Time
}

// Parse checks that the Kafka value is a JSON object with a UUID id and returns
// a copy of that JSON for storage. The copy is the whole message, not a projection.
func Parse(raw []byte) (Record, error) {
	if !json.Valid(raw) {
		return Record{}, fmt.Errorf("message is not json")
	}
	var env struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return Record{}, fmt.Errorf("message is not a json object")
	}
	if _, err := uuid.Parse(env.ID); err != nil {
		return Record{}, fmt.Errorf("invalid transfer id")
	}
	payload := make(json.RawMessage, len(raw))
	copy(payload, raw)
	return Record{
		ID:      env.ID,
		Payload: payload,
		Status:  StatusReceived,
	}, nil
}

// MaxBodyBytes is the largest inbound record the receiver will store.
// The transfers.in topic is configured to the same limit, so a larger produce is rejected by the broker.
const MaxBodyBytes = 64 * 1024

const (
	// FailOnce refuses the first publish. Later attempts go out.
	FailOnce = "once"
	// FailAlways refuses every publish until the attempt cap dead-letters the row.
	FailAlways = "always"
)

// PublishRefusal reports a deliberate publish failure carried in the message.
// attempt is the claim count, starting at 1.
func PublishRefusal(payload []byte, attempt int) (string, bool) {
	switch failMode(payload) {
	case FailOnce:
		if attempt <= 1 {
			return "publish refused: fail once", true
		}
	case FailAlways:
		return "publish refused: fail always", true
	}
	return "", false
}

// FailureNote is the composer line for a message the transmitter will refuse.
func FailureNote(payload []byte) string {
	switch failMode(payload) {
	case FailOnce:
		return "Stored. The transmitter fails the first publish, then sends it."
	case FailAlways:
		return "Stored. The transmitter fails each publish until the row is dead."
	default:
		return ""
	}
}

func failMode(payload []byte) string {
	var body struct {
		Fail string `json:"fail"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return ""
	}
	return body.Fail
}
