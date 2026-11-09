package store

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

func validateInsert(rec transfer.Record) error {
	if rec.ID == "" {
		return errors.New("id is required")
	}
	if rec.MessageKey == "" {
		return errors.New("message key is required")
	}
	if rec.SourceTopic == "" {
		return errors.New("source topic is required")
	}
	if !json.Valid(rec.Payload) {
		return fmt.Errorf("payload must be json")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(rec.Payload, &object); err != nil {
		return fmt.Errorf("payload must be a json object")
	}
	return nil
}

func clonePayload(payload json.RawMessage) json.RawMessage {
	if payload == nil {
		return nil
	}
	return append(json.RawMessage(nil), payload...)
}

func offsetKey(topic string, partition int, offset int64) string {
	return fmt.Sprintf("%s/%d/%d", topic, partition, offset)
}
