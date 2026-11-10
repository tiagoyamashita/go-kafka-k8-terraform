package console

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(body, "Valid transfer") || !strings.Contains(body, "Missing id") {
		t.Fatalf("page missing request list: %s", body)
	}
	if !strings.Contains(body, "Dead-lettered") || !strings.Contains(body, `hx-get="/dead-letters"`) {
		t.Fatalf("page missing dead-letter list: %s", body)
	}
	if !strings.Contains(body, "The Go receiver writes this error. Kafka only stores the record.") {
		t.Fatalf("page missing dead-letter source: %s", body)
	}
	if !strings.Contains(body, "The Go receiver stores this row in Postgres. Kafka only delivered the record.") {
		t.Fatalf("page missing consumed source: %s", body)
	}
	if !strings.Contains(body, `id="dead-scroll"`) || !strings.Contains(body, `id="history-scroll"`) {
		t.Fatalf("page missing independent scrolls: %s", body)
	}
}

func TestSelectExampleLoadsTheBody(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(t, store.NewMemory(), prod)
	rec := httptest.NewRecorder()
	h.loadExample(rec, httptest.NewRequest(http.MethodGet, "/examples?example=missing-id", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "amount") || !strings.Contains(body, `value="missing-id" selected`) {
		t.Fatalf("box was not filled: %s", body)
	}
	if len(prod.records) != 0 {
		t.Fatal("choosing an example produced a message")
	}

	missing := httptest.NewRecorder()
	h.loadExample(missing, httptest.NewRequest(http.MethodGet, "/examples?example=nope", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("status %d", missing.Code)
	}
}

func TestSendProducesThePostedJSON(t *testing.T) {
	mem := store.NewMemory()
	prod := &fakeProducer{}
	h := newTestHandler(t, mem, prod)
	body := `{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`
	rec := postMessage(t, h, body, true)
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
	rec := postMessage(t, h, `{"type":"transfer","payload":{"amount":10}}`, true)
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
	h, err := New(prod, reader, "transfers.in", nil)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestSendWithoutTheCheckHandsTheBodyToKafka(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(t, store.NewMemory(), prod)
	rec := postMessage(t, h, `not json`, false)
	if !strings.Contains(rec.Body.String(), "dead-letter") {
		t.Fatalf("response %s", rec.Body.String())
	}
	if len(prod.records) != 1 || prod.records[0].key != "invalid" || string(prod.records[0].value) != "not json" {
		t.Fatalf("produced %#v", prod.records)
	}
}

func postMessage(t *testing.T, h *Handler, message string, check bool) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("message", message)
	if check {
		form.Set("check", "1")
	}
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

func TestDeadLetterPaneShowsWhenTheReceiverWroteIt(t *testing.T) {
	letters := NewDeadLetters()
	h, err := New(&fakeProducer{}, store.NewMemory(), "transfers.in", letters)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Date(2026, 11, 10, 7, 5, 0, 0, time.UTC)
	body := []byte(`{"error":"invalid transfer id","message":{"id":"nope"}}`)
	msg := transfer.Inbound{Topic: "transfers.dlq", Partition: 0, Offset: 3, Key: []byte("invalid"), Value: body, At: when}
	if err := letters.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if err := letters.Handle(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if got := letters.List(); len(got) != 1 {
		t.Fatalf("len %d", len(got))
	}

	list := httptest.NewRecorder()
	h.deadLetters(list, httptest.NewRequest(http.MethodGet, "/dead-letters", nil))
	text := list.Body.String()
	if !strings.Contains(text, "invalid transfer id") || !strings.Contains(text, stamp(when)) || !strings.Contains(text, `hx-get="/dead-letters/0-3"`) {
		t.Fatalf("list %s", text)
	}

	detail := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dead-letters/0-3", nil)
	req.SetPathValue("id", "0-3")
	h.deadLetter(detail, req)
	if !strings.Contains(detail.Body.String(), "nope") || !strings.Contains(detail.Body.String(), "transfers.dlq:0:3") {
		t.Fatalf("detail %s", detail.Body.String())
	}

	raw := []byte(`{"error":"message is not json","raw":"not json"}`)
	if err := letters.Handle(context.Background(), transfer.Inbound{
		Topic: "transfers.dlq", Partition: 1, Offset: 1, Key: []byte("invalid"), Value: raw, At: when.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	rawDetail := httptest.NewRecorder()
	rawReq := httptest.NewRequest(http.MethodGet, "/dead-letters/1-1", nil)
	rawReq.SetPathValue("id", "1-1")
	h.deadLetter(rawDetail, rawReq)
	if !strings.Contains(rawDetail.Body.String(), "message is not json") || !strings.Contains(rawDetail.Body.String(), "not json") {
		t.Fatalf("raw detail %s", rawDetail.Body.String())
	}

	missing := httptest.NewRecorder()
	missingReq := httptest.NewRequest(http.MethodGet, "/dead-letters/9-9", nil)
	missingReq.SetPathValue("id", "9-9")
	h.deadLetter(missing, missingReq)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("status %d", missing.Code)
	}
}

func TestListsShowTenThenTheNextTen(t *testing.T) {
	mem := store.NewMemory()
	tick := 0
	start := time.Date(2026, 11, 10, 8, 0, 0, 0, time.UTC)
	mem.SetNow(func() time.Time {
		tick++
		return start.Add(time.Duration(tick) * time.Second)
	})
	for i := 1; i <= 11; i++ {
		id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
		_, err := mem.InsertReceived(context.Background(), transfer.Record{
			ID:          id,
			MessageKey:  id,
			Payload:     []byte(`{"id":"` + id + `"}`),
			SourceTopic: "transfers.in",
			Partition:   0,
			Offset:      int64(i),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	h := newTestHandler(t, mem, &fakeProducer{})
	first := httptest.NewRecorder()
	h.history(first, httptest.NewRequest(http.MethodGet, "/history", nil))
	body := first.Body.String()
	newest := "00000000-0000-4000-8000-000000000011"
	oldest := "00000000-0000-4000-8000-000000000001"
	if !strings.Contains(body, newest) || strings.Contains(body, oldest) || !strings.Contains(body, `class="more"`) {
		t.Fatalf("first page %s", body)
	}
	second := httptest.NewRecorder()
	h.history(second, httptest.NewRequest(http.MethodGet, "/history?limit=20", nil))
	body = second.Body.String()
	if !strings.Contains(body, oldest) || strings.Contains(body, `class="more"`) {
		t.Fatalf("second page %s", body)
	}

	letters := NewDeadLetters()
	h.letters = letters
	for i := 0; i < 11; i++ {
		err := letters.Handle(context.Background(), transfer.Inbound{
			Topic:     "transfers.dlq",
			Partition: 0,
			Offset:    int64(i),
			Key:       []byte("invalid"),
			Value:     []byte(fmt.Sprintf(`{"error":"invalid transfer id","raw":"row-%02d"}`, i)),
			At:        start.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	dead := httptest.NewRecorder()
	h.deadLetters(dead, httptest.NewRequest(http.MethodGet, "/dead-letters", nil))
	body = dead.Body.String()
	if !strings.Contains(body, `hx-get="/dead-letters/0-10"`) || strings.Contains(body, `hx-get="/dead-letters/0-0"`) || !strings.Contains(body, `class="more"`) {
		t.Fatalf("dead first page %s", body)
	}
	deadNext := httptest.NewRecorder()
	h.deadLetters(deadNext, httptest.NewRequest(http.MethodGet, "/dead-letters?limit=20", nil))
	body = deadNext.Body.String()
	if !strings.Contains(body, `hx-get="/dead-letters/0-0"`) || strings.Contains(body, `class="more"`) {
		t.Fatalf("dead second page %s", body)
	}
}

func TestSendReportsABrokerFailure(t *testing.T) {
	h := newTestHandler(t, store.NewMemory(), &fakeProducer{fail: errors.New("broker down")})
	rec := postMessage(t, h, `{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`, true)
	if !strings.Contains(rec.Body.String(), "broker down") {
		t.Fatalf("response %s", rec.Body.String())
	}
}
