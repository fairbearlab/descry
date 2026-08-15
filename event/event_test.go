package event

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

func TestToCloudEvent_Golden(t *testing.T) {
	obs := check.Observation{
		Status:     check.StatusUp,
		StatusCode: 200,
		LatencyMs:  42,
		ObservedAt: time.Date(2026, 5, 27, 2, 47, 38, 0, time.UTC),
		Labels:     map[string]string{"url": "https://example.com"},
	}
	e, err := ToCloudEvent(obs, Config{Source: "descry/test"})
	if err != nil {
		t.Fatalf("ToCloudEvent: %v", err)
	}

	// time must equal ObservedAt
	if got := e.Time().UTC(); !got.Equal(obs.ObservedAt) {
		t.Errorf("time = %v, want %v", got, obs.ObservedAt)
	}
	// a 26-char ULID id must be present
	if len(e.ID()) != 26 {
		t.Errorf("id = %q, want 26-char ULID", e.ID())
	}

	// snapshot the marshaled JSON with id masked
	b, err := e.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["specversion"] != "1.0" {
		t.Errorf("specversion = %v, want 1.0", m["specversion"])
	}
	if m["type"] != DefaultType {
		t.Errorf("type = %v, want %v", m["type"], DefaultType)
	}
	if m["datacontenttype"] != "application/json" {
		t.Errorf("datacontenttype = %v", m["datacontenttype"])
	}
}

// TestToCloudEvent_TLSExpiryAndTypeOverride covers the optional payload
// fields the golden test leaves unset: tls_expiry (RFC3339, UTC) and a
// caller-supplied event type.
func TestToCloudEvent_TLSExpiryAndTypeOverride(t *testing.T) {
	// Non-UTC zone to prove the formatter normalises to UTC.
	loc := time.FixedZone("plus2", 2*60*60)
	exp := time.Date(2027, 1, 2, 5, 4, 3, 0, loc)
	obs := check.Observation{
		Status:     check.StatusDown,
		StatusCode: 503,
		LatencyMs:  7,
		ErrorClass: check.ErrHTTPError,
		FinalURL:   "https://example.com/final",
		TLSExpiry:  &exp,
		ObservedAt: time.Date(2026, 5, 27, 2, 47, 38, 0, time.UTC),
		Labels:     map[string]string{"url": "https://example.com"},
		Extra:      map[string]any{"body": "nope"},
	}
	e, err := ToCloudEvent(obs, Config{Source: "descry/test", Type: "custom.type"})
	if err != nil {
		t.Fatalf("ToCloudEvent: %v", err)
	}
	if e.Type() != "custom.type" {
		t.Errorf("type = %q, want custom.type", e.Type())
	}
	if e.Subject() != "https://example.com" {
		t.Errorf("subject = %q", e.Subject())
	}

	var p map[string]any
	if err := json.Unmarshal(e.Data(), &p); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if got, want := p["tls_expiry"], "2027-01-02T03:04:03Z"; got != want {
		t.Errorf("tls_expiry = %v, want %v", got, want)
	}
	if p["status"] != "down" || p["error_class"] != "http_error" {
		t.Errorf("status/error_class = %v/%v", p["status"], p["error_class"])
	}
	if p["final_url"] != "https://example.com/final" {
		t.Errorf("final_url = %v", p["final_url"])
	}
	if extra, ok := p["extra"].(map[string]any); !ok || extra["body"] != "nope" {
		t.Errorf("extra = %v", p["extra"])
	}
}

// TestToCloudEvent_OmitsTLSExpiryWhenNil ensures the pointer field is dropped
// (omitempty) rather than serialised as null.
func TestToCloudEvent_OmitsTLSExpiryWhenNil(t *testing.T) {
	obs := check.Observation{
		Status:     check.StatusUp,
		ObservedAt: time.Now().UTC(),
		Labels:     map[string]string{"url": "https://example.com"},
	}
	e, err := ToCloudEvent(obs, Config{Source: "descry/test"})
	if err != nil {
		t.Fatalf("ToCloudEvent: %v", err)
	}
	var p map[string]any
	if err := json.Unmarshal(e.Data(), &p); err != nil {
		t.Fatalf("unmarshal data: %v", err)
	}
	if _, present := p["tls_expiry"]; present {
		t.Errorf("tls_expiry should be omitted when nil, got %v", p["tls_expiry"])
	}
}
