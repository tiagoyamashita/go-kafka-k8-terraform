package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

func TestHandlePersistsOriginalJSON(t *testing.T) {
	mem := store.NewMemory()
	prod := &fakeProducer{}
	svc := New(mem, prod, "transfers.dlq", nil)
	body := []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`)
	msg := transfer.Inbound{Topic: "transfers.in", Partition: 1, Offset: 7, Key: []byte("k"), Value: body}

	if err := svc.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	rec, ok := mem.Get("11111111-1111-1111-1111-111111111111")
	if !ok {
		t.Fatal("missing row")
	}
	if !bytes.Equal(rec.Payload, body) {
		t.Fatalf("stored %s", rec.Payload)
	}
	if rec.MessageKey != "k" || rec.SourceTopic != "transfers.in" || rec.Offset != 7 {
		t.Fatalf("metadata %+v", rec)
	}

	if err := svc.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if mem.Len() != 1 {
		t.Fatalf("duplicate stored, len %d", mem.Len())
	}
	if len(prod.got) != 0 {
		t.Fatal("valid message was dead-lettered")
	}
}

func TestHandleDeadLettersInvalidJSON(t *testing.T) {
	mem := store.NewMemory()
	prod := &fakeProducer{}
	svc := New(mem, prod, "transfers.dlq", nil)
	msg := transfer.Inbound{Topic: "transfers.in", Key: []byte("bad"), Value: []byte(`{"id":"nope"}`)}
	if err := svc.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if mem.Len() != 0 {
		t.Fatal("invalid message was stored")
	}
	if len(prod.got) != 1 || prod.got[0].topic != "transfers.dlq" {
		t.Fatalf("dlq %#v", prod.got)
	}
	if !json.Valid(prod.got[0].value) {
		t.Fatalf("dlq body is not json: %s", prod.got[0].value)
	}
	var body map[string]any
	if err := json.Unmarshal(prod.got[0].value, &body); err != nil {
		t.Fatal(err)
	}
	if body["error"] == "" {
		t.Fatalf("dlq body %s", prod.got[0].value)
	}
}

func TestHandleDeadLettersAnOversizedBody(t *testing.T) {
	mem := store.NewMemory()
	prod := &fakeProducer{}
	svc := New(mem, prod, "transfers.dlq", nil)
	body := append([]byte(`{"id":"11111111-1111-1111-1111-111111111111","pad":"`), bytes.Repeat([]byte("x"), transfer.MaxBodyBytes)...)
	body = append(body, []byte(`"}`)...)
	if err := svc.Handle(context.Background(), transfer.Inbound{Topic: "transfers.in", Value: body}); err != nil {
		t.Fatal(err)
	}
	if mem.Len() != 0 {
		t.Fatal("oversized message was stored")
	}
	if len(prod.got) != 1 || prod.got[0].topic != "transfers.dlq" {
		t.Fatalf("dlq %#v", prod.got)
	}
	if !bytes.Contains(prod.got[0].value, []byte("larger than 64KiB")) {
		t.Fatalf("dlq body %s", prod.got[0].value)
	}
}

func TestHandleLeavesOffsetUncommittedWhenDLQFails(t *testing.T) {
	prod := &fakeProducer{fail: map[string]int{"transfers.dlq": 1}}
	svc := New(store.NewMemory(), prod, "transfers.dlq", nil)
	err := svc.Handle(context.Background(), transfer.Inbound{Value: []byte("not-json")})
	if err == nil {
		t.Fatal("expected dlq failure")
	}
}

type errStore struct{}

func (errStore) InsertReceived(context.Context, transfer.Record) (bool, error) {
	return false, errors.New("db down")
}

func TestHandleDoesNotDeadLetterStoreErrors(t *testing.T) {
	prod := &fakeProducer{}
	svc := New(errStore{}, prod, "transfers.dlq", nil)
	body := []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{}}`)
	err := svc.Handle(context.Background(), transfer.Inbound{Topic: "transfers.in", Value: body})
	if err == nil {
		t.Fatal("expected store error")
	}
	if len(prod.got) != 0 {
		t.Fatal("store error was dead-lettered")
	}
}

type produced struct {
	topic string
	key   string
	value []byte
}

type fakeProducer struct {
	fail map[string]int
	got  []produced
}

func (f *fakeProducer) Produce(_ context.Context, topic, key string, value []byte) error {
	f.got = append(f.got, produced{topic: topic, key: key, value: append([]byte(nil), value...)})
	if f.fail[topic] > 0 {
		f.fail[topic]--
		return errors.New("broker down")
	}
	return nil
}
