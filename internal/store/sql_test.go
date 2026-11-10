package store

import "strings"
import "testing"

func TestSchemaStoresJSONPayload(t *testing.T) {
	schema := strings.ToLower(migration001)
	if !strings.Contains(schema, "payload jsonb") {
		t.Fatal("migration must store payload as jsonb")
	}
	if !strings.Contains(schema, "jsonb_typeof(payload) = 'object'") {
		t.Fatal("payload must be a json object")
	}
	if !strings.Contains(insertSQL, "$3::jsonb") || !strings.Contains(insertSQL, "ON CONFLICT DO NOTHING") {
		t.Fatal("insert must persist jsonb idempotently")
	}
	if !strings.Contains(strings.ToLower(migration002), "transfer_events") {
		t.Fatal("migration must record status history")
	}
}
