package console

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

func TestPageListsNothingUntilAMessageIsStored(t *testing.T) {
	h := newTestHandler(t, store.NewMemory(), &fakeProducer{})
	rec := httptest.NewRecorder()
	h.page(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Send") || !strings.Contains(body, "hx-post=\"/messages\"") {
		t.Fatalf("page missing send form: %s", body)
	}
}

func TestSendProducesThePostedJSON(t *testing.T) {
	mem := store.NewMemory()
	prod := &fakeProducer{}
	h := newTestHandler(t, mem, prod)
	body := `{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`
	rec := postMessage(t, h, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "11111111-1111-1111-1111-111111111111") {
		t.Fatalf("response %s", rec.Body.String())
	}
	if rec.Header().Get("HX-Trigger") != "refresh-history" {
		t.Fatalf("trigger %q", rec.Header().Get("HX-Trigger"))
	}
	if len(prod.records) != 1 || prod.records[0].topic != "transfers.in" || prod.records[0].key != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("produced %#v", prod.records)
	}
	if string(prod.records[0].value) != body {
		t.Fatalf("value %s", prod.records[0].value)
	}
	if mem.Len() != 0 {
		t.Fatal("send stored a row; only the receiver should do that")
	}
}

func TestSendRejectsAMessageWithoutAnID(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(t, store.NewMemory(), prod)
	rec := postMessage(t, h, `{"type":"transfer","payload":{"amount":10}}`)
	if !strings.Contains(rec.Body.String(), "UUID id") {
		t.Fatalf("response %s", rec.Body.String())
	}
	if len(prod.records) != 0 {
		t.Fatalf("produced %#v", prod.records)
	}
}

func TestHistoryShowsStoredStatusChanges(t *testing.T) {
	mem := store.NewMemory()
	rec := transfer.Record{
		ID:          "11111111-1111-1111-1111-111111111111",
		MessageKey:  "11111111-1111-1111-1111-111111111111",
		Payload:     []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`),
		SourceTopic: "transfers.in",
	}
	if _, err := mem.InsertReceived(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	claimed, err := mem.ClaimPublish(context.Background(), 1, 0, 5)
	if err != nil || len(claimed) != 1 {
		t.Fatal(err, claimed)
	}
	if err := mem.MarkPublished(context.Background(), rec.ID); err != nil {
		t.Fatal(err)
	}

	h := newTestHandler(t, mem, &fakeProducer{})
	list := httptest.NewRecorder()
	h.history(list, httptest.NewRequest(http.MethodGet, "/history", nil))
	if !strings.Contains(list.Body.String(), "published") || !strings.Contains(list.Body.String(), rec.ID) {
		t.Fatalf("list %s", list.Body.String())
	}

	detail := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/history/"+rec.ID, nil)
	req.SetPathValue("id", rec.ID)
	h.detail(detail, req)
	body := detail.Body.String()
	for _, want := range []string{"received", "publishing", "published", "attempt 1"} {
		if !strings.Contains(body, want) {
			t.Fatalf("detail missing %s: %s", want, body)
		}
	}
}

func TestDetailUnknownID(t *testing.T) {
	h := newTestHandler(t, store.NewMemory(), &fakeProducer{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/history/22222222-2222-2222-2222-222222222222", nil)
	req.SetPathValue("id", "22222222-2222-2222-2222-222222222222")
	h.detail(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d", rec.Code)
	}
}

func newTestHandler(t *testing.T, reader Reader, prod *fakeProducer) *Handler {
	t.Helper()
	h, err := New(prod, reader, "transfers.in")
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func postMessage(t *testing.T, h *Handler, message string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("message", message)
	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.send(rec, req)
	return rec
}

type produced struct {
	topic string
	key   string
	value []byte
}

type fakeProducer struct {
	records []produced
	fail    error
}

func (f *fakeProducer) Produce(_ context.Context, topic, key string, value []byte) error {
	if f.fail != nil {
		return f.fail
	}
	f.records = append(f.records, produced{topic: topic, key: key, value: append([]byte(nil), value...)})
	return nil
}

func TestSendReportsABrokerFailure(t *testing.T) {
	h := newTestHandler(t, store.NewMemory(), &fakeProducer{fail: errors.New("broker down")})
	rec := postMessage(t, h, `{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`)
	if !strings.Contains(rec.Body.String(), "broker down") {
		t.Fatalf("response %s", rec.Body.String())
	}
}
