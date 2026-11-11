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
	if !strings.Contains(body, "Valid transfer") || !strings.Contains(body, "Receiver: missing id") || !strings.Contains(body, "Fail until dead") || !strings.Contains(body, "Kafka: message too large") || !strings.Contains(body, "Kafka: unknown topic") || !strings.Contains(body, "Kafka: SASL rejected") || !strings.Contains(body, "Catch it on the front end") {
		t.Fatalf("page missing request list: %s", body)
	}
	if !strings.Contains(body, "Dead-lettered") || !strings.Contains(body, `hx-get="/dead-letters"`) || !strings.Contains(body, "Kafka errors") || !strings.Contains(body, `hx-get="/kafka-errors"`) {
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
	if !strings.Contains(body, "Common Failures") || !strings.Contains(body, "Who decides") || !strings.Contains(body, "transfers.out") {
		t.Fatalf("page missing failure guide: %s", body)
	}
	if !strings.Contains(body, "How to debug") || !strings.Contains(body, "Kibana Discover") || !strings.Contains(body, "transfer_dlq_total") || !strings.Contains(body, "UNKNOWN_TOPIC_OR_PARTITION") || !strings.Contains(body, "ILLEGAL_SASL_STATE") {
		t.Fatalf("page missing debug guide: %s", body)
	}
	if !strings.Contains(body, "What is what") || !strings.Contains(body, "Kafka and Confluent") || !strings.Contains(body, "Confluent Cloud") || !strings.Contains(body, "The three topics") || !strings.Contains(body, "FOR UPDATE SKIP LOCKED") || !strings.Contains(body, "This console") {
		t.Fatalf("page missing glossary: %s", body)
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

func TestFailExamplesAreStoredThenRefused(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(t, store.NewMemory(), prod)
	loaded := httptest.NewRecorder()
	h.loadExample(loaded, httptest.NewRequest(http.MethodGet, "/examples?example=fail-always", nil))
	body := loaded.Body.String()
	if !strings.Contains(body, "always") || !strings.Contains(body, `value="fail-always" selected`) {
		t.Fatalf("box was not filled: %s", body)
	}
	if len(prod.records) != 0 {
		t.Fatal("choosing an example produced a message")
	}

	sent := postMessage(t, h, `{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","fail":"once"}`, true)
	if !strings.Contains(sent.Body.String(), "fails the first publish") {
		t.Fatalf("response %s", sent.Body.String())
	}
	if len(prod.records) != 1 || prod.records[0].key != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("produced %#v", prod.records)
	}

	dead := postMessage(t, h, `{"id":"22222222-2222-2222-2222-222222222222","type":"transfer","fail":"always"}`, true)
	if !strings.Contains(dead.Body.String(), "until the row is dead") {
		t.Fatalf("response %s", dead.Body.String())
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

func TestFrontEndCheckStopsABadBody(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(t, store.NewMemory(), prod)
	over := strings.Repeat("x", transfer.MaxBodyBytes+1)

	pane := postMessage(t, h, over, true)
	if !strings.Contains(pane.Body.String(), "larger than 64KiB") || len(prod.records) != 0 {
		t.Fatalf("pane %s produced %d", pane.Body.String(), len(prod.records))
	}

	receiver := postMessage(t, h, over, false)
	if !strings.Contains(receiver.Body.String(), "dead-letter") || len(prod.records) != 1 {
		t.Fatalf("receiver %s produced %d", receiver.Body.String(), len(prod.records))
	}
}

func TestRequestListPutsReceiverBreaksBeforeKafka(t *testing.T) {
	receiverAt := -1
	kafkaAt := -1
	for i, ex := range requestExamples {
		if strings.HasPrefix(ex.Label, "Receiver:") && receiverAt < 0 {
			receiverAt = i
		}
		if ex.ID == brokerRejectID {
			kafkaAt = i
		}
		if strings.HasPrefix(ex.Label, "Receiver:") && kafkaAt >= 0 && i > kafkaAt {
			t.Fatalf("receiver example %s after kafka", ex.Label)
		}
	}
	if receiverAt < 0 || kafkaAt < receiverAt {
		t.Fatalf("order receiver %d kafka %d", receiverAt, kafkaAt)
	}
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

func postMessage(t *testing.T, h *Handler, message string, front bool) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("message", message)
	if front {
		form.Set("front", "1")
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
	list := httptest.NewRecorder()
	h.kafkaErrors(list, httptest.NewRequest(http.MethodGet, "/kafka-errors", nil))
	if !strings.Contains(list.Body.String(), "broker down") || !strings.Contains(list.Body.String(), "11111111-1111-1111-1111-111111111111") || !strings.Contains(list.Body.String(), "Where to look") {
		t.Fatalf("kafka errors %s", list.Body.String())
	}
}

func TestKafkaErrorSaysWhereToLook(t *testing.T) {
	h := newTestHandler(t, store.NewMemory(), &fakeProducer{fail: errors.New("MESSAGE_TOO_LARGE: The request included a message larger than the max message size the server will accept")})
	postMessage(t, h, `{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`, true)
	list := httptest.NewRecorder()
	h.kafkaErrors(list, httptest.NewRequest(http.MethodGet, "/kafka-errors", nil))
	if !strings.Contains(list.Body.String(), "Broker log") || !strings.Contains(list.Body.String(), "65536") {
		t.Fatalf("where %s", list.Body.String())
	}
	if kafkaWhere("dial tcp 127.0.0.1:1: connectex: No connection could be made because the target machine actively refused it.") != "Only this list. The dial failed before a broker saw it." {
		t.Fatal("wrong address")
	}
	if !strings.Contains(kafkaWhere("ILLEGAL_SASL_STATE: Request is not valid given the current SASL state."), "plaintext") {
		t.Fatal("sasl")
	}
}

func TestBrokerRejectExampleIsRefusedByKafka(t *testing.T) {
	ex, ok := exampleByID(brokerRejectID)
	if !ok {
		t.Fatal("missing example")
	}
	body := ex.text()
	if len(body) <= transfer.MaxBodyBytes {
		t.Fatalf("body length %d", len(body))
	}
	prod := &fakeProducer{fail: errors.New("MESSAGE_TOO_LARGE: The request included a message larger than the max message size the server will accept")}
	h := newTestHandler(t, store.NewMemory(), prod)
	form := url.Values{}
	form.Set("example", brokerRejectID)
	form.Set("message", body)
	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.send(rec, req)
	if !strings.Contains(rec.Body.String(), "MESSAGE_TOO_LARGE") || strings.Contains(rec.Body.String(), "larger than 64KiB.") {
		t.Fatalf("response %s", rec.Body.String()[:200])
	}
	list := httptest.NewRecorder()
	h.kafkaErrors(list, httptest.NewRequest(http.MethodGet, "/kafka-errors", nil))
	if !strings.Contains(list.Body.String(), "MESSAGE_TOO_LARGE") {
		t.Fatalf("kafka errors %s", list.Body.String())
	}

	loaded := httptest.NewRecorder()
	h.loadExample(loaded, httptest.NewRequest(http.MethodGet, "/examples?example="+brokerRejectID+"&front=1", nil))
	if !strings.Contains(loaded.Body.String(), `value="broker-reject" selected`) || !strings.Contains(loaded.Body.String(), `value="1" checked`) {
		t.Fatalf("example %s", loaded.Body.String()[:400])
	}
}

func TestKafkaRequestsTargetTheBrokerFailure(t *testing.T) {
	prod := &fakeProducer{}
	h := newTestHandler(t, store.NewMemory(), prod)
	body := `{"id":"11111111-1111-1111-1111-111111111111","type":"transfer"}`
	postExample(t, h, kafkaUnknownID, body)
	postExample(t, h, kafkaInvalidID, body)
	if len(prod.records) != 2 || prod.records[0].topic != "transfers.missing" || prod.records[1].topic != "bad topic" {
		t.Fatalf("produced %#v", prod.records)
	}

	h.demonstrate = func(_ context.Context, mode string) error {
		return fmt.Errorf("dial %s", mode)
	}
	wrong := postExample(t, h, kafkaWrongID, body)
	if !strings.Contains(wrong.Body.String(), "dial wrong-address") || len(prod.records) != 2 {
		t.Fatalf("wrong address %s produced %d", wrong.Body.String(), len(prod.records))
	}
	list := httptest.NewRecorder()
	h.kafkaErrors(list, httptest.NewRequest(http.MethodGet, "/kafka-errors", nil))
	if !strings.Contains(list.Body.String(), "dial wrong-address") {
		t.Fatalf("kafka errors %s", list.Body.String())
	}
}

func postExample(t *testing.T, h *Handler, example, message string) *httptest.ResponseRecorder {
	t.Helper()
	form := url.Values{}
	form.Set("example", example)
	form.Set("message", message)
	req := httptest.NewRequest(http.MethodPost, "/messages", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.send(rec, req)
	return rec
}
