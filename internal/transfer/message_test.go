package transfer

import (
	"bytes"
	"testing"
)

func TestParseKeepsOriginalJSON(t *testing.T) {
	raw := []byte(`{"id":"11111111-1111-1111-1111-111111111111","type":"transfer","payload":{"amount":10}}`)
	rec, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if rec.ID != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("id %s", rec.ID)
	}
	if !bytes.Equal(rec.Payload, raw) {
		t.Fatalf("payload %s", rec.Payload)
	}
	raw[0] = ' '
	if rec.Payload[0] != '{' {
		t.Fatal("payload aliases the caller buffer")
	}
}

func TestParseRejectsInvalidMessages(t *testing.T) {
	cases := [][]byte{
		[]byte(`not-json`),
		[]byte(`{"id":"nope","payload":{}}`),
		[]byte(`{"payload":{}}`),
		[]byte(`[1,2,3]`),
	}
	for _, raw := range cases {
		if _, err := Parse(raw); err == nil {
			t.Fatalf("expected error for %s", raw)
		}
	}
}
