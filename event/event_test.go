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
