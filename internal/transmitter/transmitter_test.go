package transmitter

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/receiver"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

func TestRunOncePublishesStoredJSON(t *testing.T) {
	mem := store.NewMemory()
	prod := &fakeProducer{}
	body := []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`)
	recv := receiver.New(mem, prod, "transfers.dlq", nil)
	if err := recv.Handle(context.Background(), transfer.Inbound{
		Topic:  "transfers.in",
		Offset: 3,
		Value:  body,
	}); err != nil {
		t.Fatal(err)
	}

	worker := New(mem, prod, testOptions(5))
	n, err := worker.RunOnce(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("n %d err %v", n, err)
	}
	if len(prod.got) != 1 || prod.got[0].topic != "transfers.out" || prod.got[0].key != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("published %#v", prod.got)
	}
	if !bytes.Equal(prod.got[0].value, body) {
		t.Fatalf("published json %s", prod.got[0].value)
	}
	got, _ := mem.Get("11111111-1111-1111-1111-111111111111")
	if got.Status != transfer.StatusPublished {
		t.Fatalf("status %s", got.Status)
	}
}

func TestRunOnceRetriesThenDeadLetters(t *testing.T) {
	mem := store.NewMemory()
	if _, err := mem.InsertReceived(context.Background(), stored()); err != nil {
		t.Fatal(err)
	}
	prod := &fakeProducer{fail: map[string]int{"transfers.out": 1}}
	worker := New(mem, prod, testOptions(2))

	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := mem.Get(stored().ID)
	if got.Status != transfer.StatusFailed || got.Attempts != 1 {
		t.Fatalf("after retry %+v", got)
	}

	prod.fail["transfers.out"] = 1
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ = mem.Get(stored().ID)
	if got.Status != transfer.StatusDead {
		t.Fatalf("status %s", got.Status)
	}
	if len(prod.got) != 3 || prod.got[2].topic != "transfers.dlq" {
		t.Fatalf("produces %#v", prod.got)
	}
	if !bytes.Equal(prod.got[2].value, stored().Payload) {
		t.Fatalf("dlq json %s", prod.got[2].value)
	}
}

func TestFailOncePublishesOnTheSecondAttempt(t *testing.T) {
	mem := store.NewMemory()
	body := []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","fail":"once"}`)
	if _, err := mem.InsertReceived(context.Background(), transfer.Record{
		ID: "11111111-1111-1111-1111-111111111111", MessageKey: "11111111-1111-1111-1111-111111111111",
		Payload: body, SourceTopic: "transfers.in", Offset: 1,
	}); err != nil {
		t.Fatal(err)
	}
	prod := &fakeProducer{}
	worker := New(mem, prod, testOptions(5))
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := mem.Get("11111111-1111-1111-1111-111111111111")
	if got.Status != transfer.StatusFailed || got.LastError != "publish refused: fail once" {
		t.Fatalf("after first attempt %+v", got)
	}
	if len(prod.got) != 0 {
		t.Fatalf("published early %#v", prod.got)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ = mem.Get("11111111-1111-1111-1111-111111111111")
	if got.Status != transfer.StatusPublished || len(prod.got) != 1 || prod.got[0].topic != "transfers.out" {
		t.Fatalf("after second %+v produces %#v", got, prod.got)
	}
}

func TestFailAlwaysDeadLettersWithoutPublishing(t *testing.T) {
	mem := store.NewMemory()
	body := []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","fail":"always"}`)
	if _, err := mem.InsertReceived(context.Background(), transfer.Record{
		ID: "11111111-1111-1111-1111-111111111111", MessageKey: "11111111-1111-1111-1111-111111111111",
		Payload: body, SourceTopic: "transfers.in", Offset: 2,
	}); err != nil {
		t.Fatal(err)
	}
	prod := &fakeProducer{}
	worker := New(mem, prod, testOptions(2))
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := mem.Get("11111111-1111-1111-1111-111111111111")
	if got.Status != transfer.StatusFailed {
		t.Fatalf("status %s", got.Status)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ = mem.Get("11111111-1111-1111-1111-111111111111")
	if got.Status != transfer.StatusDead || got.LastError != "publish refused: fail always" {
		t.Fatalf("dead %+v", got)
	}
	if len(prod.got) != 1 || prod.got[0].topic != "transfers.dlq" {
		t.Fatalf("produces %#v", prod.got)
	}
}

func TestRunOnceKeepsFailedWhenDLQIsDown(t *testing.T) {
	mem := store.NewMemory()
	if _, err := mem.InsertReceived(context.Background(), stored()); err != nil {
		t.Fatal(err)
	}
	prod := &fakeProducer{fail: map[string]int{"transfers.out": 1, "transfers.dlq": 1}}
	worker := New(mem, prod, testOptions(1))
	if _, err := worker.RunOnce(context.Background()); err == nil {
		t.Fatal("expected dlq error")
	}
	got, _ := mem.Get(stored().ID)
	if got.Status != transfer.StatusFailed || got.Attempts != 1 {
		t.Fatalf("after dlq failure %+v", got)
	}

	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ = mem.Get(stored().ID)
	if got.Status != transfer.StatusDead {
		t.Fatalf("status %s", got.Status)
	}
}

func testOptions(maxAttempts int) Options {
	return Options{
		OutboundTopic: "transfers.out",
		DLQTopic:      "transfers.dlq",
		BatchSize:     10,
		MaxAttempts:   maxAttempts,
		Lease:         time.Minute,
		PollInterval:  time.Millisecond,
	}
}

func stored() transfer.Record {
	return transfer.Record{
		ID:          "11111111-1111-1111-1111-111111111111",
		MessageKey:  "11111111-1111-1111-1111-111111111111",
		Payload:     []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"n":1}}`),
		SourceTopic: "transfers.in",
		Offset:      1,
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
