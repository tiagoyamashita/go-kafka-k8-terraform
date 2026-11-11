package console

import (
	"bytes"
	"context"
	"crypto/rand"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/kafka"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

//go:embed page.html
var pageFS embed.FS

const (
	pageSize     = 10
	historyLimit = 100
)

// Reader loads consumed transfers for the history view.
type Reader interface {
	List(ctx context.Context, limit int) ([]transfer.Record, error)
	History(ctx context.Context, id string) (transfer.Record, []transfer.Event, error)
}

// Handler serves the send form and the consumed-message history.
type Handler struct {
	producer    kafka.Producer
	reader      Reader
	letters     *DeadLetters
	kafkaErrs   *KafkaErrors
	topic       string
	brokers     []string
	demonstrate func(context.Context, string) error
	tmpl        *template.Template
}

// New parses the page templates. letters may be nil, in which case the dead-letter list stays empty.
func New(producer kafka.Producer, reader Reader, topic string, letters *DeadLetters) (*Handler, error) {
	tmpl, err := template.ParseFS(pageFS, "page.html")
	if err != nil {
		return nil, fmt.Errorf("parse console templates: %w", err)
	}
	if letters == nil {
		letters = NewDeadLetters()
	}
	return &Handler{producer: producer, reader: reader, letters: letters, kafkaErrs: NewKafkaErrors(), topic: topic, tmpl: tmpl}, nil
}

// SetBrokers is the cluster address used by the SASL failure example.
func (h *Handler) SetBrokers(brokers []string) {
	h.brokers = append([]string(nil), brokers...)
}

// Register mounts the console routes.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", h.page)
	mux.HandleFunc("GET /examples", h.loadExample)
	mux.HandleFunc("POST /messages", h.send)
	mux.HandleFunc("GET /history", h.history)
	mux.HandleFunc("GET /history/{id}", h.detail)
	mux.HandleFunc("GET /dead-letters", h.deadLetters)
	mux.HandleFunc("GET /dead-letters/{id}", h.deadLetter)
	mux.HandleFunc("GET /kafka-errors", h.kafkaErrors)
}

func (h *Handler) page(w http.ResponseWriter, r *http.Request) {
	h.render(w, "page", map[string]any{
		"Composer": newComposer(sampleMessage(), "", "", validExampleID, false),
	})
}

func (h *Handler) loadExample(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("example")
	ex, ok := exampleByID(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.render(w, "composer", newComposer(ex.text(), "", "", ex.ID, r.URL.Query().Get("front") == "1"))
}

func (h *Handler) send(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.render(w, "composer", newComposer("", "The form could not be read.", "", validExampleID, false))
		return
	}
	selected := r.FormValue("example")
	front := r.FormValue("front") == "1"
	raw := strings.TrimSpace(r.FormValue("message"))
	if len(raw) > 2*1024*1024 {
		h.render(w, "composer", newComposer("", "The message is larger than 2MiB.", "", selected, front))
		return
	}
	rec, err := transfer.Parse([]byte(raw))
	over := len(raw) > transfer.MaxBodyBytes
	if front && over {
		h.render(w, "composer", newComposer(raw, "The message is larger than 64KiB.", "", selected, front))
		return
	}
	if front && err != nil {
		h.render(w, "composer", newComposer(raw, "The message must be a JSON object with a UUID id.", "", selected, front))
		return
	}
	key := "invalid"
	sentID := ""
	note := "Sent to transfers.in. The receiver will dead-letter it. The time and the error show under Dead-lettered."
	if err == nil && !over {
		key = rec.ID
		sentID = rec.ID
		note = transfer.FailureNote([]byte(raw))
	}
	if mode, ok := kafkaDialMode(selected); ok {
		if err := h.dialFailure(r.Context(), mode); err != nil {
			h.kafkaErrs.Add(key, err.Error())
			w.Header().Set("HX-Trigger", "refresh-history")
			h.render(w, "composer", newComposer(raw, "Kafka did not accept the message: "+err.Error(), "", selected, front))
			return
		}
	}
	topic := h.topic
	switch selected {
	case kafkaUnknownID:
		topic = "transfers.missing"
	case kafkaInvalidID:
		topic = "bad topic"
	}
	if err := h.producer.Produce(r.Context(), topic, key, []byte(raw)); err != nil {
		h.kafkaErrs.Add(key, err.Error())
		w.Header().Set("HX-Trigger", "refresh-history")
		h.render(w, "composer", newComposer(raw, "Kafka did not accept the message: "+err.Error(), "", selected, front))
		return
	}
	w.Header().Set("HX-Trigger", "refresh-history")
	data := newComposer(sampleMessage(), "", sentID, validExampleID, front)
	data.Note = note
	h.render(w, "composer", data)
}

func (h *Handler) history(w http.ResponseWriter, r *http.Request) {
	limit := pageLimit(r)
	records, err := h.reader.List(r.Context(), limit+1)
	if err != nil {
		http.Error(w, "could not load history", http.StatusInternalServerError)
		return
	}
	more := len(records) > limit
	if more {
		records = records[:limit]
	}
	rows := make([]rowView, 0, len(records))
	for _, rec := range records {
		rows = append(rows, newRow(rec))
	}
	h.render(w, "rows", map[string]any{"Records": rows, "More": more})
}

func (h *Handler) detail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		http.NotFound(w, r)
		return
	}
	rec, events, err := h.reader.History(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, "could not load history", http.StatusInternalServerError)
		return
	}
	if len(events) == 0 {
		events = fallbackEvents(rec)
	}
	view := detailView{
		rowView: newRow(rec),
		Payload: prettyJSON(rec.Payload),
		Events:  make([]eventView, 0, len(events)),
	}
	for _, event := range events {
		view.Events = append(view.Events, eventView{
			Status: string(event.Status),
			Detail: event.Detail,
			At:     stamp(event.At),
		})
	}
	h.render(w, "detail", view)
}

func (h *Handler) deadLetters(w http.ResponseWriter, r *http.Request) {
	limit := pageLimit(r)
	letters := h.letters.List()
	more := len(letters) > limit
	if more {
		letters = letters[:limit]
	}
	views := make([]deadView, 0, len(letters))
	for _, letter := range letters {
		views = append(views, newDeadView(letter))
	}
	h.render(w, "deadRows", map[string]any{"Letters": views, "More": more})
}

func (h *Handler) kafkaErrors(w http.ResponseWriter, r *http.Request) {
	limit := pageLimit(r)
	items := h.kafkaErrs.List()
	more := len(items) > limit
	if more {
		items = items[:limit]
	}
	views := make([]kafkaErrorView, 0, len(items))
	for _, item := range items {
		views = append(views, kafkaErrorView{
			At:    stamp(item.At),
			Key:   item.Key,
			Error: item.Error,
			Where: kafkaWhere(item.Error),
		})
	}
	h.render(w, "kafkaRows", map[string]any{"Errors": views, "More": more})
}

func pageLimit(r *http.Request) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return pageSize
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return pageSize
	}
	if n > historyLimit {
		return historyLimit
	}
	return n
}

func (h *Handler) deadLetter(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validDeadLetterID(id) {
		http.NotFound(w, r)
		return
	}
	letter, ok := h.letters.Get(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	h.render(w, "deadDetail", newDeadView(letter))
}

func validDeadLetterID(id string) bool {
	part, off, ok := strings.Cut(id, "-")
	if !ok || part == "" || off == "" {
		return false
	}
	if _, err := strconv.Atoi(part); err != nil {
		return false
	}
	_, err := strconv.Atoi(off)
	return err == nil
}

func (h *Handler) render(w http.ResponseWriter, name string, data any) {
	var buf bytes.Buffer
	if err := h.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		http.Error(w, "could not render the page", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = io.Copy(w, &buf)
}

const validExampleID = "valid"

const brokerRejectID = "broker-reject"
const kafkaUnknownID = "kafka-unknown"
const kafkaInvalidID = "kafka-invalid"
const kafkaWrongID = "kafka-wrong-address"
const kafkaUnreachableID = "kafka-unreachable"
const kafkaSASLID = "kafka-sasl"

func kafkaDialMode(id string) (string, bool) {
	switch id {
	case kafkaWrongID:
		return "wrong-address", true
	case kafkaUnreachableID:
		return "broker-unreachable", true
	case kafkaSASLID:
		return "sasl-rejected", true
	default:
		return "", false
	}
}

func (h *Handler) dialFailure(ctx context.Context, mode string) error {
	if h.demonstrate != nil {
		return h.demonstrate(ctx, mode)
	}
	return kafka.Demonstrate(ctx, h.brokers, mode)
}

type requestExample struct {
	ID    string
	Label string
	Body  string
	Fail  string
}

// requestExamples is the send-pane list. Valid transfer is published.
// The fail entries are stored, then the transmitter refuses them.
// Receiver entries are dead-lettered by the receiver.
// The Kafka entries are produces this broker rejects.
var requestExamples = []requestExample{
	{ID: validExampleID, Label: "Valid transfer"},
	{ID: "fail-once", Label: "Fail publish once", Fail: transfer.FailOnce},
	{ID: "fail-always", Label: "Fail until dead", Fail: transfer.FailAlways},
	{ID: "not-json", Label: "Receiver: not JSON", Body: "not json"},
	{ID: "array", Label: "Receiver: JSON array", Body: `[{"id":"11111111-1111-1111-1111-111111111111"}]`},
	{ID: "missing-id", Label: "Receiver: missing id", Body: `{"type":"transfer","payload":{"amount":10}}`},
	{ID: "empty-id", Label: "Receiver: empty id", Body: `{"id":"","type":"transfer"}`},
	{ID: "bad-id", Label: "Receiver: id is not a UUID", Body: `{"id":"transfer-1","type":"transfer"}`},
	{ID: "short-id", Label: "Receiver: UUID too short", Body: `{"id":"11111111-1111-1111-1111-11111111111","type":"transfer"}`},
	{ID: "number-id", Label: "Receiver: id is a number", Body: `{"id":1}`},
	{ID: "string", Label: "Receiver: bare string", Body: `"just a string"`},
	{ID: "truncated", Label: "Receiver: truncated JSON", Body: `{"id":"11111111-1111-1111-1111-111111111111"`},
	{ID: "oversize", Label: "Receiver: larger than 64KiB"},
	{ID: brokerRejectID, Label: "Kafka: message too large"},
	{ID: kafkaUnknownID, Label: "Kafka: unknown topic"},
	{ID: kafkaInvalidID, Label: "Kafka: invalid topic"},
	{ID: kafkaWrongID, Label: "Kafka: wrong address"},
	{ID: kafkaUnreachableID, Label: "Kafka: broker unreachable"},
	{ID: kafkaSASLID, Label: "Kafka: SASL rejected"},
}

func (ex requestExample) text() string {
	switch {
	case ex.ID == validExampleID:
		return sampleMessage()
	case ex.ID == "oversize":
		return strings.Repeat("x", transfer.MaxBodyBytes+1)
	case ex.ID == brokerRejectID:
		return brokerRejectBody()
	case ex.ID == kafkaUnknownID || ex.ID == kafkaInvalidID || ex.ID == kafkaWrongID || ex.ID == kafkaUnreachableID || ex.ID == kafkaSASLID:
		return sampleMessage()
	case ex.Fail != "":
		return failMessage(ex.Fail)
	default:
		return ex.Body
	}
}

func exampleByID(id string) (requestExample, bool) {
	for _, ex := range requestExamples {
		if ex.ID == id {
			return ex, true
		}
	}
	return requestExample{}, false
}

type exampleView struct {
	ID       string
	Label    string
	Selected bool
}

type composerData struct {
	Message  string
	Error    string
	SentID   string
	Note     string
	Front    bool
	Examples []exampleView
}

func newComposer(message, errText, sentID, selected string, front bool) composerData {
	if selected == "" {
		selected = validExampleID
	}
	views := make([]exampleView, len(requestExamples))
	for i, ex := range requestExamples {
		views[i] = exampleView{ID: ex.ID, Label: ex.Label, Selected: ex.ID == selected}
	}
	return composerData{
		Message:  message,
		Error:    errText,
		SentID:   sentID,
		Front:    front,
		Examples: views,
	}
}

type kafkaErrorView struct {
	At    string
	Key   string
	Error string
	Where string
}

func kafkaWhere(err string) string {
	switch {
	case strings.Contains(err, "MESSAGE_TOO_LARGE"):
		return "Broker log: kubectl -n transfer logs kafka-0. transfers.in allows 65536 bytes after compression."
	case strings.Contains(err, "UNKNOWN_TOPIC_OR_PARTITION"):
		return "Broker log. Auto-create is off, so transfers.missing is not created."
	case strings.Contains(err, "INVALID_TOPIC_EXCEPTION"):
		return "Broker log. The topic name is not valid."
	case strings.Contains(err, "ILLEGAL_SASL_STATE"):
		return "Broker log. The listener is plaintext, so SASL is rejected."
	case strings.Contains(err, "127.0.0.1:1"), strings.Contains(err, "connection refused"), strings.Contains(err, "actively refused"):
		return "Only this list. The dial failed before a broker saw it."
	case strings.Contains(err, "broker unreachable"), strings.Contains(err, "192.0.2.1"), strings.Contains(err, "deadline exceeded"), strings.Contains(err, "i/o timeout"):
		return "Only this list. The address did not route, so the Kafka pod log stays quiet."
	default:
		return "This list has the produce error. If the broker answered, also read kubectl -n transfer logs kafka-0."
	}
}

type rowView struct {
	ID        string
	Status    string
	Attempts  int
	Received  string
	Published string
	Error     string
}

type eventView struct {
	Status string
	Detail string
	At     string
}

type detailView struct {
	rowView
	Payload string
	Events  []eventView
}

func newRow(rec transfer.Record) rowView {
	return rowView{
		ID:        rec.ID,
		Status:    string(rec.Status),
		Attempts:  rec.Attempts,
		Received:  stamp(rec.ReceivedAt),
		Published: stamp(rec.PublishedAt),
		Error:     rec.LastError,
	}
}

func fallbackEvents(rec transfer.Record) []transfer.Event {
	events := []transfer.Event{{
		Status: transfer.StatusReceived,
		Detail: fmt.Sprintf("%s:%d:%d", rec.SourceTopic, rec.Partition, rec.Offset),
		At:     rec.ReceivedAt,
	}}
	if rec.Status != transfer.StatusReceived && rec.Status != "" {
		at := rec.PublishedAt
		if at.IsZero() {
			at = rec.ReceivedAt
		}
		events = append(events, transfer.Event{Status: rec.Status, Detail: rec.LastError, At: at})
	}
	return events
}

// brokerRejectBody is random text larger than the transfers.in limit.
// Repeated bytes compress under that limit, so Kafka accepts them.
func brokerRejectBody() string {
	raw := make([]byte, 51*1024)
	if _, err := rand.Read(raw); err != nil {
		return err.Error()
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func failMessage(mode string) string {
	return sampleMessageWith(map[string]any{
		"fail": mode,
		"payload": map[string]any{
			"amount": 10,
		},
	})
}

func sampleMessage() string {
	return sampleMessageWith(map[string]any{
		"payload": map[string]any{
			"amount": 10,
		},
	})
}

func sampleMessageWith(extra map[string]any) string {
	body := map[string]any{
		"id":   uuid.NewString(),
		"type": "transfer",
	}
	for key, value := range extra {
		body[key] = value
	}
	raw, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return ""
	}
	return string(raw)
}

func prettyJSON(raw json.RawMessage) string {
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Local().Format("2006-01-02 15:04:05")
}
