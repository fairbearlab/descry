package event

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

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

// TestNewEncoder_Rejects covers the config errors NewEncoder reports once,
// with the same text a per-event Validate gave (errLines orders multi-field
// ValidationErrors).
func TestNewEncoder_Rejects(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"empty source", Config{Source: ""}},
		{"fragment-only source", Config{Source: "#"}},
		{"unparseable source", Config{Source: "%zz"}},
		{"missing scheme source", Config{Source: "::x"}},
		{"whitespace type", Config{Source: "s", Type: "   "}},
		{"bad source + whitespace type", Config{Source: "#", Type: "   "}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := NewEncoder(tc.cfg)
			if err == nil {
				t.Fatalf("NewEncoder(%+v) accepted an invalid config", tc.cfg)
			}
			if enc != nil {
				t.Fatalf("NewEncoder returned a non-nil Encoder with error %v", err)
			}
			if !strings.HasPrefix(err.Error(), "invalid cloudevent: ") {
				t.Fatalf("error = %q, want \"invalid cloudevent: \" prefix", err)
			}
			_, legacyErr := legacyToCloudEvent(check.Observation{}, tc.cfg)
			if legacyErr == nil || errLines(err) != errLines(legacyErr) {
				t.Fatalf("error = %v, legacy error = %v", err, legacyErr)
			}
		})
	}
}

// TestNewEncoder_Accepts covers inputs legacy accepted that a stricter
// validator might not: a whitespace source (URL-escaped, as legacy did) and
// an empty type (defaulted).
func TestNewEncoder_Accepts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cfg        Config
		wantSource string
		wantType   string
	}{
		{"whitespace source", Config{Source: "  "}, "%20%20", DefaultType},
		{"empty type defaults", Config{Source: "descry/test"}, "descry/test", DefaultType},
		{"padded type is trimmed", Config{Source: "descry/test", Type: " a.b "}, "descry/test", "a.b"},
		{"absolute URI source", Config{Source: "https://user@example.com/a?b=c#f"}, "https://user@example.com/a?b=c#f", DefaultType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc, err := NewEncoder(tc.cfg)
			if err != nil {
				t.Fatalf("NewEncoder(%+v): %v", tc.cfg, err)
			}
			e, err := enc.Encode(check.Observation{})
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if e.Source() != tc.wantSource || e.Type() != tc.wantType {
				t.Fatalf("source/type = %q/%q, want %q/%q", e.Source(), e.Type(), tc.wantSource, tc.wantType)
			}
			if err := e.Validate(); err != nil {
				t.Fatalf("Encode produced an invalid event: %v", err)
			}
		})
	}
}

// TestEncoder_ZeroValueErrors: an Encoder not built by NewEncoder must refuse
// to encode rather than emit an event with an empty source and type.
func TestEncoder_ZeroValueErrors(t *testing.T) {
	for name, enc := range map[string]*Encoder{"nil": nil, "zero": {}} {
		t.Run(name, func(t *testing.T) {
			e, err := enc.Encode(benchObs())
			if err == nil || err.Error() != "event: Encoder not initialised; use NewEncoder" {
				t.Fatalf("error = %v, want the not-initialised error", err)
			}
			if !reflect.DeepEqual(e, cloudevents.Event{}) {
				t.Fatalf("event = %#v, want zero Event", e)
			}
		})
	}
}

// TestEncoder_DataErrorDoesNotPoison: a payload that cannot be marshaled
// fails that one event; the same Encoder encodes the next observation.
func TestEncoder_DataErrorDoesNotPoison(t *testing.T) {
	enc, err := NewEncoder(Config{Source: "descry/test"})
	if err != nil {
		t.Fatal(err)
	}
	for name, extra := range map[string]map[string]any{
		"chan": {"c": make(chan int)},
		"NaN":  {"f": math.NaN()},
	} {
		t.Run(name, func(t *testing.T) {
			e, err := enc.Encode(check.Observation{Extra: extra})
			if err == nil || !strings.HasPrefix(err.Error(), "set data: ") {
				t.Fatalf("error = %v, want a \"set data: \" error", err)
			}
			if !reflect.DeepEqual(e, cloudevents.Event{}) {
				t.Fatalf("event = %#v, want zero Event", e)
			}
			if _, err := enc.Encode(benchObs()); err != nil {
				t.Fatalf("Encode after a data error: %v", err)
			}
		})
	}
}

// TestEncoder_SubjectAndTime pins the SDK-setter semantics Encode keeps while
// skipping Validate: an absent or blank url label leaves subject unset (not
// an empty string Validate would reject), a padded one is trimmed, and a zero
// ObservedAt leaves time unset.
func TestEncoder_SubjectAndTime(t *testing.T) {
	enc, err := NewEncoder(Config{Source: "descry/test"})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 5, 27, 2, 47, 38, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		labels      map[string]string
		observedAt  time.Time
		wantSubject *string
		wantTime    bool
	}{
		{"nil labels", nil, at, nil, true},
		{"missing url label", map[string]string{"other": "x"}, at, nil, true},
		{"whitespace url label", map[string]string{"url": " \t "}, at, nil, true},
		{"padded url label", map[string]string{"url": "  https://x/  "}, at, ptr("https://x/"), true},
		{"zero ObservedAt", map[string]string{"url": "https://x/"}, time.Time{}, ptr("https://x/"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := enc.Encode(check.Observation{Labels: tc.labels, ObservedAt: tc.observedAt})
			if err != nil {
				t.Fatal(err)
			}
			ec := e.Context.(*cloudevents.EventContextV1)
			if !reflect.DeepEqual(ec.Subject, tc.wantSubject) {
				t.Errorf("subject = %v, want %v", ec.Subject, tc.wantSubject)
			}
			if (ec.Time != nil) != tc.wantTime {
				t.Errorf("time = %v, want set=%v", ec.Time, tc.wantTime)
			}
			b, err := e.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatal(err)
			}
			if _, has := m["time"]; has != tc.wantTime {
				t.Errorf("marshaled time present = %v, want %v: %s", has, tc.wantTime, b)
			}
			if _, has := m["subject"]; has != (tc.wantSubject != nil) {
				t.Errorf("marshaled subject present = %v, want %v: %s", has, tc.wantSubject != nil, b)
			}
			if err := e.Validate(); err != nil {
				t.Errorf("Encode produced an invalid event: %v", err)
			}
		})
	}
}

func ptr(s string) *string { return &s }

// TestEncoder_ConcurrentEncode exercises one Encoder from many goroutines, as
// the runner's workers do; run under -race it catches any shared mutable
// state between the Encoder and the events it returns.
func TestEncoder_ConcurrentEncode(t *testing.T) {
	enc, err := NewEncoder(Config{Source: "https://user@example.com/descry"})
	if err != nil {
		t.Fatal(err)
	}
	const workers, perWorker = 8, 200
	ids := make(chan string, workers*perWorker)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			obs := benchObs()
			for range perWorker {
				e, err := enc.Encode(obs)
				if err != nil {
					t.Error(err)
					return
				}
				if e.Source() != "https://user@example.com/descry" || e.Type() != DefaultType || e.Subject() != "https://example.com/healthz" {
					t.Errorf("worker %d: source/type/subject = %q/%q/%q", w, e.Source(), e.Type(), e.Subject())
					return
				}
				// Writes to the returned event must stay private to it.
				e.SetSubject("mutated")
				e.Context.(*cloudevents.EventContextV1).Source.Path = "/mutated"
				ids <- e.ID()
			}
		})
	}
	wg.Wait()
	close(ids)
	seen := make(map[string]bool, workers*perWorker)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}
