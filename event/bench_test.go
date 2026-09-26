package event

import (
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

// benchObs is a representative observation: the fields an HTTP check fills on
// a healthy HTTPS target, plus one Extra entry.
func benchObs() check.Observation {
	exp := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	return check.Observation{
		Status:     check.StatusUp,
		StatusCode: 200,
		LatencyMs:  42,
		FinalURL:   "https://example.com/healthz",
		TLSExpiry:  &exp,
		ObservedAt: time.Date(2026, 5, 27, 2, 47, 38, 0, time.UTC),
		Labels:     map[string]string{"url": "https://example.com/healthz"},
		Extra:      map[string]any{"region": "us-east-1"},
	}
}

// BenchmarkToCloudEvent measures the per-observation envelope cost; run with
// -benchmem to see allocs/op.
func BenchmarkToCloudEvent(b *testing.B) {
	obs := benchObs()
	cfg := Config{Source: "descry/bench"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ToCloudEvent(obs, cfg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkToCloudEvent_Minimal is the down/no-TLS/no-Extra shape.
func BenchmarkToCloudEvent_Minimal(b *testing.B) {
	obs := check.Observation{
		Status:     check.StatusDown,
		ErrorClass: check.ErrHTTPError,
		ObservedAt: time.Date(2026, 5, 27, 2, 47, 38, 0, time.UTC),
		Labels:     map[string]string{"url": "https://example.com"},
	}
	cfg := Config{Source: "descry/bench"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ToCloudEvent(obs, cfg); err != nil {
			b.Fatal(err)
		}
	}
}
