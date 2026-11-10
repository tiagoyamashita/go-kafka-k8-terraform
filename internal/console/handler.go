package console

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/kafka"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/store"
	"github.com/wovenbytoyota/go-kafka-k8-terraform/internal/transfer"
)

//go:embed page.html
var pageFS embed.FS

const historyLimit = 100

// Reader loads consumed transfers for the history view.
type Reader interface {
	List(ctx context.Context, limit int) ([]transfer.Record, error)
	History(ctx context.Context, id string) (transfer.Record, []transfer.Event, error)
}

// Handler serves the send form and the consumed-message history.
type Handler struct {
	producer kafka.Producer
	reader   Reader
	topic    string
	tmpl     *template.Template
}

// New parses the page templates.
func New(producer kafka.Producer, reader Reader, topic string) (*Handler, error) {
	tmpl, err := template.ParseFS(pageFS, "page.html")
	if err != nil {
		return nil, fmt.Errorf("parse console templates: %w", err)
	}
	return &Handler{producer: producer, reader: reader, topic: topic, tmpl: tmpl}, nil
}

// Register mounts the console routes.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /{$}", h.page)
	mux.HandleFunc("GET /examples", h.loadExample)
	mux.HandleFunc("POST /messages", h.send)
	mux.HandleFunc("GET /history", h.history)
	mux.HandleFunc("GET /history/{id}", h.detail)
}

func (h *Handler) page(w http.ResponseWriter, r *http.Request) {
	h.render(w, "page", map[string]any{
		"Composer": newComposer(sampleMessage(), "", "", validExampleID),
	})
}

func (h *Handler) loadExample(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("example")
	ex, ok := exampleByID(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	body := ex.Body
	if ex.ID == validExampleID {
		body = sampleMessage()
	}
	h.render(w, "composer", newComposer(body, "", "", ex.ID))
}

func (h *Handler) send(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.render(w, "composer", newComposer("", "The form could not be read.", "", validExampleID))
		return
	}
	selected := r.FormValue("example")
	raw := strings.TrimSpace(r.FormValue("message"))
	if len(raw) > 64*1024 {
		h.render(w, "composer", newComposer(raw, "The message is larger than 64KiB.", "", selected))
		return
	}
	rec, err := transfer.Parse([]byte(raw))
	if err != nil {
		h.render(w, "composer", newComposer(raw, "The message must be a JSON object with a UUID id.", "", selected))
		return
	}
	if err := h.producer.Produce(r.Context(), h.topic, rec.ID, []byte(raw)); err != nil {
		h.render(w, "composer", newComposer(raw, "Kafka did not accept the message: "+err.Error(), "", selected))
		return
	}
	w.Header().Set("HX-Trigger", "refresh-history")
	h.render(w, "composer", newComposer(sampleMessage(), "", rec.ID, validExampleID))
}

func (h *Handler) history(w http.ResponseWriter, r *http.Request) {
	records, err := h.reader.List(r.Context(), historyLimit)
	if err != nil {
		http.Error(w, "could not load history", http.StatusInternalServerError)
		return
	}
	rows := make([]rowView, 0, len(records))
	for _, rec := range records {
		rows = append(rows, newRow(rec))
	}
	h.render(w, "rows", map[string]any{"Records": rows})
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

type requestExample struct {
	ID    string
	Label string
	Body  string
}

// requestExamples is the send-pane list. The first entry is a fresh valid
// message. The rest are bodies the pane rejects.
var requestExamples = []requestExample{
	{ID: validExampleID, Label: "Valid transfer"},
	{ID: "not-json", Label: "Not JSON", Body: "not json"},
	{ID: "array", Label: "JSON array", Body: `[{"id":"11111111-1111-1111-1111-111111111111"}]`},
	{ID: "missing-id", Label: "Missing id", Body: `{"type":"transfer","payload":{"amount":10}}`},
	{ID: "empty-id", Label: "Empty id", Body: `{"id":"","type":"transfer"}`},
	{ID: "bad-id", Label: "id is not a UUID", Body: `{"id":"transfer-1","type":"transfer"}`},
	{ID: "short-id", Label: "UUID too short", Body: `{"id":"11111111-1111-1111-1111-11111111111","type":"transfer"}`},
	{ID: "number-id", Label: "id is a number", Body: `{"id":1}`},
	{ID: "string", Label: "Bare string", Body: `"just a string"`},
	{ID: "truncated", Label: "Truncated JSON", Body: `{"id":"11111111-1111-1111-1111-111111111111"`},
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
	Examples []exampleView
}

func newComposer(message, errText, sentID, selected string) composerData {
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
		Examples: views,
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

func sampleMessage() string {
	raw, err := json.MarshalIndent(map[string]any{
		"id":   uuid.NewString(),
		"type": "transfer",
		"payload": map[string]any{
			"amount": 10,
		},
	}, "", "  ")
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
