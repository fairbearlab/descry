package event

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	ceevent "github.com/cloudevents/sdk-go/v2/event"

	"github.com/fairbearlab/descry/check"
)

// assertMatchesLegacy checks ToCloudEvent, and Encode on a NewEncoder built
// from the same cfg, against the reference implementation. On success: IDs
// aside, since each call mints a fresh ULID, a deeply equal event that
// marshals to identical bytes. On error: the error text legacyWantErr
// expects and a zero Event, per the wrapper's documented contract.
func assertMatchesLegacy(t *testing.T, obs check.Observation, cfg Config) {
	t.Helper()
	want, wantErr := legacyToCloudEvent(obs, cfg)
	wantErr = legacyWantErr(obs, cfg, wantErr)

	got, gotErr := ToCloudEvent(obs, cfg)
	compareToLegacy(t, "ToCloudEvent", got, gotErr, want, wantErr)

	enc, err := NewEncoder(cfg)
	if err != nil {
		// The config error must be the one the wrapper reported.
		compareToLegacy(t, "NewEncoder", cloudevents.Event{}, err, want, wantErr)
		if enc != nil {
			t.Fatalf("NewEncoder returned a non-nil Encoder with error %v", err)
		}
		return
	}
	got, gotErr = enc.Encode(obs)
	compareToLegacy(t, "Encode", got, gotErr, want, wantErr)
}

// legacyWantErr adapts the legacy error to the wrapper's ordering: legacy
// marshals the payload before validating the config, so a bad Extra plus a
// bad source reported "set data"; the wrapper reports the config error
// first. Only Extra can fail to marshal, so legacy run without it yields the
// config error, if any.
func legacyWantErr(obs check.Observation, cfg Config, legacyErr error) error {
	if legacyErr == nil || !strings.HasPrefix(legacyErr.Error(), "set data: ") {
		return legacyErr
	}
	obs.Extra = nil
	if _, cfgErr := legacyToCloudEvent(obs, cfg); cfgErr != nil {
		return cfgErr
	}
	return legacyErr
}

func compareToLegacy(t *testing.T, path string, got cloudevents.Event, gotErr error, want cloudevents.Event, wantErr error) {
	t.Helper()
	if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && errLines(gotErr) != errLines(wantErr)) {
		t.Fatalf("%s: error = %v, legacy error = %v", path, gotErr, wantErr)
	}
	if gotErr != nil {
		if !reflect.DeepEqual(got, cloudevents.Event{}) {
			t.Fatalf("%s: returned a non-zero Event with error %v: %#v", path, gotErr, got)
		}
		return
	}
	if len(got.ID()) != 26 {
		t.Fatalf("%s: id = %q, want 26-char ULID", path, got.ID())
	}
	got.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	want.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: event differs from legacy:\n got  %#v\n want %#v", path, got, want)
	}
	gb, gerr := got.MarshalJSON()
	wb, werr := want.MarshalJSON()
	if (gerr == nil) != (werr == nil) || !bytes.Equal(gb, wb) {
		t.Fatalf("%s: marshaled event differs from legacy:\n got  %s (%v)\n want %s (%v)", path, gb, gerr, wb, werr)
	}
}

// errLines renders err for comparison. A multi-field
// cloudevents.ValidationError is a map, so its Error() line order varies call
// to call; render it as sorted "field: message" lines under a stable prefix.
func errLines(err error) string {
	var ve ceevent.ValidationError
	if !errors.As(err, &ve) {
		return err.Error()
	}
	// ve.Error() is the tail of err.Error(); only its order varies, not its length.
	msg := err.Error()
	prefix := msg[:len(msg)-len(ve.Error())]
	lines := make([]string, 0, len(ve))
	for field, e := range ve {
		lines = append(lines, field+": "+e.Error())
	}
	sort.Strings(lines)
	return prefix + "\n" + strings.Join(lines, "\n")
}

func TestToCloudEvent_MatchesLegacy(t *testing.T) {
	plus2 := time.FixedZone("plus2", 2*60*60)
	exp := time.Date(2027, 1, 2, 5, 4, 3, 0, plus2)
	farExp := time.Date(12345, 6, 7, 8, 9, 10, 0, time.UTC)
	negExp := time.Date(-5, 1, 1, 0, 0, 0, 0, time.UTC)
	observed := time.Date(2026, 5, 27, 2, 47, 38, 123456789, plus2)

	cases := []struct {
		name string
		obs  check.Observation
		cfg  Config
	}{
		{"bench shape", benchObs(), Config{Source: "descry/bench"}},
		{"minimal down", check.Observation{Status: check.StatusDown, ErrorClass: check.ErrHTTPError, ObservedAt: observed}, Config{Source: "s"}},
		{"tls non-UTC + type override", check.Observation{Status: check.StatusUp, TLSExpiry: &exp, ObservedAt: observed, Labels: map[string]string{"url": "https://example.com"}}, Config{Source: "s", Type: "custom.type"}},
		{"tls year > 9999", check.Observation{TLSExpiry: &farExp, ObservedAt: observed}, Config{Source: "s"}},
		{"tls negative year", check.Observation{TLSExpiry: &negExp, ObservedAt: observed}, Config{Source: "s"}},
		{"zero ObservedAt omits time", check.Observation{Status: check.StatusUp}, Config{Source: "s"}},
		{"whitespace subject is dropped", check.Observation{Labels: map[string]string{"url": "  "}}, Config{Source: "s"}},
		{"subject is trimmed", check.Observation{Labels: map[string]string{"url": " https://x "}}, Config{Source: "s"}},
		{"type is trimmed", check.Observation{}, Config{Source: "s", Type: "  padded.type  "}},
		{"whitespace source", check.Observation{}, Config{Source: "  "}},
		{"html-ish strings", check.Observation{FinalURL: "https://x/?a=<b>&c", Labels: map[string]string{"url": "<&>"}, Extra: map[string]any{"k": "</script> "}}, Config{Source: "s"}},
		{"nested extra", check.Observation{Extra: map[string]any{"b": 1, "a": []int{1, 2}, "m": map[string]any{"z": nil, "y": true}}}, Config{Source: "s"}},
		{"source with userinfo+query", check.Observation{}, Config{Source: "https://user@example.com/a?b=c#frag"}},
		// Error paths: the error text must match the legacy one exactly, with
		// a zero Event.
		{"empty source", check.Observation{}, Config{Source: ""}},
		{"fragment-only source", check.Observation{}, Config{Source: "#"}},
		{"unparseable source", check.Observation{}, Config{Source: "%zz"}},
		{"missing scheme source", check.Observation{}, Config{Source: "::x"}},
		{"whitespace type", check.Observation{}, Config{Source: "s", Type: "   "}},
		{"bad source + whitespace type", check.Observation{}, Config{Source: "#", Type: "   "}},
		{"unmarshalable extra", check.Observation{Extra: map[string]any{"c": make(chan int)}}, Config{Source: "s"}},
		{"NaN extra", check.Observation{Extra: map[string]any{"f": math.NaN()}}, Config{Source: "s"}},
		// Legacy reported "set data" here; the wrapper reports the config
		// error first (legacyWantErr).
		{"unmarshalable extra + bad source", check.Observation{Extra: map[string]any{"c": make(chan int)}}, Config{Source: ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertMatchesLegacy(t, tc.obs, tc.cfg) })
	}
}

// TestToCloudEvent_ConfigErrorBeforeDataError pins the wrapper's documented
// error order directly, independent of the legacy oracle: a bad source wins
// over an unmarshalable payload, and the Event is zero.
func TestToCloudEvent_ConfigErrorBeforeDataError(t *testing.T) {
	obs := check.Observation{Extra: map[string]any{"c": make(chan int)}}
	e, err := ToCloudEvent(obs, Config{Source: "%zz"})
	if err == nil || !strings.HasPrefix(err.Error(), "invalid cloudevent: ") {
		t.Fatalf("error = %v, want an \"invalid cloudevent: \" error", err)
	}
	if !reflect.DeepEqual(e, cloudevents.Event{}) {
		t.Fatalf("event = %#v, want zero Event", e)
	}
}

// TestEncoder_EventsAreIndependent guards the per-event context: events from
// one Encoder must not share mutable state with each other or the Encoder.
func TestEncoder_EventsAreIndependent(t *testing.T) {
	enc, err := NewEncoder(Config{Source: "https://user@example.com/descry"})
	if err != nil {
		t.Fatal(err)
	}
	obs := benchObs()
	a, err := enc.Encode(obs)
	if err != nil {
		t.Fatal(err)
	}
	b, err := enc.Encode(obs)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == b.ID() {
		t.Fatalf("two events share id %q", a.ID())
	}
	if a.Context == b.Context {
		t.Fatal("two events share one *EventContextV1")
	}
	a.SetSubject("changed")
	a.SetSource("changed")
	a.SetType("changed")
	a.SetTime(time.Unix(1, 0))
	a.DataEncoded[0] = 'X'
	// Mutate the copied URL in place too, not only through SetSource.
	b2, err := enc.Encode(obs)
	if err != nil {
		t.Fatal(err)
	}
	b2.Context.(*cloudevents.EventContextV1).Source.Path = "/mutated"

	if b.Subject() != "https://example.com/healthz" || b.Source() != "https://user@example.com/descry" || b.Type() != DefaultType {
		t.Fatalf("mutating one event changed another: subject=%q source=%q type=%q", b.Subject(), b.Source(), b.Type())
	}
	if !b.Time().Equal(obs.ObservedAt) || b.DataEncoded[0] != '{' {
		t.Fatalf("mutating one event changed another: time=%v data=%s", b.Time(), b.DataEncoded)
	}
	c, err := enc.Encode(obs)
	if err != nil {
		t.Fatal(err)
	}
	if c.Source() != "https://user@example.com/descry" {
		t.Fatalf("mutating an event changed the Encoder: next source = %q", c.Source())
	}
	if a.Subject() != "changed" || a.Source() != "changed" {
		t.Fatalf("a later call changed an earlier event: subject=%q source=%q", a.Subject(), a.Source())
	}
}
