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

// assertMatchesLegacy checks ToCloudEvent against the reference
// implementation: same error text, and — IDs aside, since each call mints a
// fresh ULID — a deeply equal event that marshals to identical bytes.
func assertMatchesLegacy(t *testing.T, obs check.Observation, cfg Config) {
	t.Helper()
	got, gotErr := ToCloudEvent(obs, cfg)
	want, wantErr := legacyToCloudEvent(obs, cfg)

	if (gotErr == nil) != (wantErr == nil) || (gotErr != nil && errLines(gotErr) != errLines(wantErr)) {
		t.Fatalf("error = %v, legacy error = %v", gotErr, wantErr)
	}
	if len(got.ID()) != 26 {
		t.Fatalf("id = %q, want 26-char ULID", got.ID())
	}
	got.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	want.SetID("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event differs from legacy:\n got  %#v\n want %#v", got, want)
	}
	gb, gerr := got.MarshalJSON()
	wb, werr := want.MarshalJSON()
	if (gerr == nil) != (werr == nil) || !bytes.Equal(gb, wb) {
		t.Fatalf("marshaled event differs from legacy:\n got  %s (%v)\n want %s (%v)", gb, gerr, wb, werr)
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
		{"html-ish strings", check.Observation{FinalURL: "https://x/?a=<b>&c", Labels: map[string]string{"url": "<&>"}, Extra: map[string]any{"k": "</script> "}}, Config{Source: "s"}},
		{"nested extra", check.Observation{Extra: map[string]any{"b": 1, "a": []int{1, 2}, "m": map[string]any{"z": nil, "y": true}}}, Config{Source: "s"}},
		{"source with userinfo+query", check.Observation{}, Config{Source: "https://user@example.com/a?b=c#frag"}},
		// Error paths: the returned error text must match the legacy one exactly.
		{"empty source", check.Observation{}, Config{Source: ""}},
		{"fragment-only source", check.Observation{}, Config{Source: "#"}},
		{"unparseable source", check.Observation{}, Config{Source: "%zz"}},
		{"whitespace type", check.Observation{}, Config{Source: "s", Type: "   "}},
		{"bad source + whitespace type", check.Observation{}, Config{Source: "#", Type: "   "}},
		{"unmarshalable extra", check.Observation{Extra: map[string]any{"c": make(chan int)}}, Config{Source: "s"}},
		{"NaN extra", check.Observation{Extra: map[string]any{"f": math.NaN()}}, Config{Source: "s"}},
		{"unmarshalable extra + bad source", check.Observation{Extra: map[string]any{"c": make(chan int)}}, Config{Source: ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertMatchesLegacy(t, tc.obs, tc.cfg) })
	}
}

// TestToCloudEvent_SourceCacheSwitching alternates sources so the cached
// parse is both hit and replaced, including a switch from a valid source to
// an invalid one and back.
func TestToCloudEvent_SourceCacheSwitching(t *testing.T) {
	obs := benchObs()
	for _, src := range []string{"a", "a", "b", "", "b", "%zz", "a", "#", "a"} {
		assertMatchesLegacy(t, obs, Config{Source: src})
	}
}

// TestToCloudEvent_EventsAreIndependent guards the single-allocation layout:
// events built back to back must not share mutable state.
func TestToCloudEvent_EventsAreIndependent(t *testing.T) {
	obs := benchObs()
	cfg := Config{Source: "descry/test"}
	a, err := ToCloudEvent(obs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ToCloudEvent(obs, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == b.ID() {
		t.Fatalf("two events share id %q", a.ID())
	}
	a.SetSubject("changed")
	a.SetSource("changed")
	if b.Subject() != "https://example.com/healthz" || b.Source() != "descry/test" {
		t.Fatalf("mutating one event changed another: subject=%q source=%q", b.Subject(), b.Source())
	}
	if _, err := ToCloudEvent(obs, cfg); err != nil {
		t.Fatal(err)
	}
	if a.Subject() != "changed" || a.Source() != "changed" {
		t.Fatalf("a later call changed an earlier event: subject=%q source=%q", a.Subject(), a.Source())
	}
}

// TestToCloudEvent_Allocs guards the envelope's allocation count. The
// minimal shape costs one block for the envelope, the ID string, and
// json.Marshal's result; the full shape adds encoding/json's own cost for a
// one-entry Extra map (sorted-keys slice plus reflect copies of key and
// value). Before this guard: 13 and 18 allocs/op.
// Bound measured 2026-09-25 on go1.26.6 darwin/arm64; a Go toolchain or
// cloudevents/sdk-go bump may require re-measuring (bounds are "<=" the
// measured value).
func TestToCloudEvent_Allocs(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are unreliable under -race")
	}
	cfg := Config{Source: "descry/test"}
	minimal := check.Observation{
		Status:     check.StatusDown,
		ErrorClass: check.ErrHTTPError,
		ObservedAt: time.Date(2026, 5, 27, 2, 47, 38, 0, time.UTC),
		Labels:     map[string]string{"url": "https://example.com"},
	}
	for _, tc := range []struct {
		name string
		obs  check.Observation
		max  float64
	}{
		{"minimal", minimal, 3},
		{"full", benchObs(), 6},
	} {
		var e cloudevents.Event
		got := testing.AllocsPerRun(500, func() {
			var err error
			if e, err = ToCloudEvent(tc.obs, cfg); err != nil {
				t.Fatal(err)
			}
		})
		if got > tc.max {
			t.Errorf("%s: ToCloudEvent allocs/op = %v, want <= %v", tc.name, got, tc.max)
		}
		_ = e
	}
}
