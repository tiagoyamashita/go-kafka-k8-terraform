package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

func TestInsertIsIdempotentAndStoresJSON(t *testing.T) {
	mem := NewMemory()
	first := sample("11111111-1111-1111-1111-111111111111", 1, `{"n":1}`)
	inserted, err := mem.InsertReceived(context.Background(), first)
	if err != nil || !inserted {
		t.Fatalf("inserted %v err %v", inserted, err)
	}

	again := sample("11111111-1111-1111-1111-111111111111", 2, `{"n":2}`)
	inserted, err = mem.InsertReceived(context.Background(), again)
	if err != nil || inserted {
		t.Fatalf("duplicate inserted %v err %v", inserted, err)
	}
	got, ok := mem.Get(first.ID)
	if !ok || string(got.Payload) != string(first.Payload) {
		t.Fatalf("payload changed to %s", got.Payload)
	}

	other := sample("22222222-2222-2222-2222-222222222222", 1, `{"n":3}`)
	inserted, err = mem.InsertReceived(context.Background(), other)
	if err != nil || inserted {
		t.Fatalf("offset conflict inserted %v err %v", inserted, err)
	}
	if mem.Len() != 1 {
		t.Fatalf("len %d", mem.Len())
	}
}

func TestInsertRejectsNonObjectJSON(t *testing.T) {
	mem := NewMemory()
	rec := sample("11111111-1111-1111-1111-111111111111", 1, `{"n":1}`)
	rec.Payload = json.RawMessage(`[1,2]`)
	if _, err := mem.InsertReceived(context.Background(), rec); err == nil {
		t.Fatal("expected object payload")
	}
}

func TestClaimRetryAndDead(t *testing.T) {
	mem := NewMemory()
	now := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	mem.SetNow(func() time.Time { return now })
	rec := sample("11111111-1111-1111-1111-111111111111", 4, `{"ok":true}`)
	if _, err := mem.InsertReceived(context.Background(), rec); err != nil {
		t.Fatal(err)
	}

	claimed, err := mem.ClaimPublish(context.Background(), 10, time.Minute, 2)
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 || claimed[0].Status != transfer.StatusPublishing {
		t.Fatalf("first claim %#v err %v", claimed, err)
	}
	again, err := mem.ClaimPublish(context.Background(), 10, time.Minute, 2)
	if err != nil || len(again) != 0 {
		t.Fatalf("lease not held %#v", again)
	}

	if err := mem.MarkFailed(context.Background(), rec.ID, "broker down"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	retried, err := mem.ClaimPublish(context.Background(), 10, time.Minute, 2)
	if err != nil || len(retried) != 1 || retried[0].Attempts != 2 {
		t.Fatalf("retry %#v err %v", retried, err)
	}
	if err := mem.MarkFailed(context.Background(), rec.ID, "broker down"); err != nil {
		t.Fatal(err)
	}

	publishable, err := mem.ClaimPublish(context.Background(), 10, time.Minute, 2)
	if err != nil || len(publishable) != 0 {
		t.Fatalf("exhausted row still publishable %#v", publishable)
	}
	dead, err := mem.ClaimDead(context.Background(), 10, time.Minute, 2)
	if err != nil || len(dead) != 1 || dead[0].Attempts != 2 || string(dead[0].Payload) != string(rec.Payload) {
		t.Fatalf("dead claim %#v err %v", dead, err)
	}
	if err := mem.MarkDead(context.Background(), rec.ID, "gave up"); err != nil {
		t.Fatal(err)
	}
	depth, err := mem.OutboxDepth(context.Background())
	if err != nil || depth != 0 {
		t.Fatalf("depth %d err %v", depth, err)
	}
	got, _ := mem.Get(rec.ID)
	if got.Status != transfer.StatusDead {
		t.Fatalf("status %s", got.Status)
	}
}

func TestPublishingLeaseCanBeReclaimed(t *testing.T) {
	mem := NewMemory()
	now := time.Date(2026, 11, 9, 8, 0, 0, 0, time.UTC)
	mem.SetNow(func() time.Time { return now })
	rec := sample("11111111-1111-1111-1111-111111111111", 8, `{"ok":true}`)
	if _, err := mem.InsertReceived(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if _, err := mem.ClaimPublish(context.Background(), 1, time.Minute, 5); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	claimed, err := mem.ClaimPublish(context.Background(), 1, time.Minute, 5)
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("reclaim %#v err %v", claimed, err)
	}
	if err := mem.MarkPublished(context.Background(), rec.ID); err != nil {
		t.Fatal(err)
	}
	depth, err := mem.OutboxDepth(context.Background())
	if err != nil || depth != 0 {
		t.Fatalf("depth %d err %v", depth, err)
	}
}

func sample(id string, offset int64, payload string) transfer.Record {
	return transfer.Record{
		ID:          id,
		MessageKey:  "key",
		Payload:     json.RawMessage(`{"id":"` + id + `","type":"transfer","payload":` + payload + `}`),
		SourceTopic: "transfers.in",
		Partition:   0,
		Offset:      offset,
	}
}
