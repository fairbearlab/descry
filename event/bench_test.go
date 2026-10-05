package event

import (
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

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

// minimalObs is the down/no-TLS/no-Extra shape.
func minimalObs() check.Observation {
	return check.Observation{
		Status:     check.StatusDown,
		ErrorClass: check.ErrHTTPError,
		ObservedAt: time.Date(2026, 5, 27, 2, 47, 38, 0, time.UTC),
		Labels:     map[string]string{"url": "https://example.com"},
	}
}

var benchShapes = []struct {
	name string
	obs  func() check.Observation
}{
	{"full", benchObs},
	{"minimal", minimalObs},
}

// BenchmarkEncoder_Encode measures the per-observation envelope cost on the
// runner's path: one Encoder built up front, Encode per event. Run with
// -benchmem to see allocs/op.
func BenchmarkEncoder_Encode(b *testing.B) {
	enc, err := NewEncoder(Config{Source: "descry/bench"})
	if err != nil {
		b.Fatal(err)
	}
	for _, s := range benchShapes {
		b.Run(s.name, func(b *testing.B) {
			obs := s.obs()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := enc.Encode(obs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkToCloudEvent measures the convenience wrapper, which builds an
// Encoder (probe event, source parse, Validate) on every call.
func BenchmarkToCloudEvent(b *testing.B) {
	cfg := Config{Source: "descry/bench"}
	for _, s := range benchShapes {
		b.Run(s.name, func(b *testing.B) {
			obs := s.obs()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ToCloudEvent(obs, cfg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestEncoder_Allocs guards Encode's allocation count. The minimal shape
// costs the *EventContextV1, the ULID string, the subject/time/
// datacontenttype pointers the SDK setters store, and json.Marshal's work on
// the payload; the full shape adds the TLS expiry string and encoding/json's
// own cost for a one-entry Extra map. Before Encoder (per-event SetSource +
// Validate): 13 and 18 allocs/op.
// Bound measured 2026-10-05 with go1.26.6 and cloudevents/sdk-go v2.16.2 on
// darwin/arm64 and linux/amd64 (golang:1.26.6 container, the CI platform):
// 8/13 on both. A Go toolchain or SDK bump may require re-measuring (bounds
// are "<=" the measured value). The wrapper is benchmarked, not guarded.
func TestEncoder_Allocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are unreliable under -race")
	}
	enc, err := NewEncoder(Config{Source: "descry/test"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		obs  check.Observation
		max  float64
	}{
		{"minimal", minimalObs(), 8},
		{"full", benchObs(), 13},
	} {
		var e cloudevents.Event
		got := testing.AllocsPerRun(500, func() {
			var err error
			if e, err = enc.Encode(tc.obs); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("%s: Encode allocs/op = %v (bound %v)", tc.name, got, tc.max)
		if got > tc.max {
			t.Errorf("%s: Encode allocs/op = %v, want <= %v", tc.name, got, tc.max)
		}
		_ = e
	}
}
