package event

import (
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

// ToCloudEvent feeds three attacker-influenced strings straight into the
// CloudEvents SDK: Source and Type come from config, but Subject is
// obs.Labels["url"], which is a target URL. The contract worth pinning is that
// a nil error means a genuinely valid event — if SetSubject or SetData ever
// accepts something Validate would reject, a sink downstream emits a malformed
// envelope and nothing upstream noticed. Encode skips the per-event Validate,
// so this is also the load-bearing check on that shortcut: ToCloudEvent and
// NewEncoder+Encode are pinned to the SDK-setter reference implementation
// (legacy_test.go) — same event bytes apart from the id, and a rejection
// exactly where legacy rejects, with the same error text (config errors
// reported first; see legacyWantErr).
//
// Runs as an ordinary unit test in CI (seed corpus only). On-demand fuzzing
// uses an execution count rather than a duration (golang/go#75804):
// go test ./event -run '^$' -fuzz FuzzToCloudEvent -fuzztime 100000x

func FuzzToCloudEvent(f *testing.F) {
	f.Add("descry/example", "dev.descry.observation.v1", "https://example.com", 200, 42)
	f.Add("descry/example", "", "https://user:pw@example.com/a?b=c", 0, 0)
	f.Add("", "", "", 500, -1)
	f.Add("/relative-source", "x", "\x00\x7f", 999, 1<<30)
	f.Add("urn:uuid:1234", "a.b.c", "not a url at all", -1, 0)
	f.Add("#", "  ", "  ", 0, 0)
	f.Add("%zz", " t ", " <&> ", 0, 0)
	f.Add("::x", "", "\t", 0, 0)
	f.Add("  ", "", " https://x ", 0, 0)

	f.Fuzz(func(t *testing.T, source, typ, urlLabel string, statusCode, latencyMs int) {
		obs := check.Observation{
			Status:     check.StatusUp,
			LatencyMs:  latencyMs,
			StatusCode: statusCode,
			FinalURL:   urlLabel,
			ObservedAt: time.Unix(0, 0).UTC(),
			Labels:     map[string]string{"url": urlLabel},
		}

		assertMatchesLegacy(t, obs, Config{Source: source, Type: typ})

		e, err := ToCloudEvent(obs, Config{Source: source, Type: typ})
		if err != nil {
			return // rejecting bad input is the correct outcome
		}

		// A nil error must mean the event really is valid.
		if verr := e.Validate(); verr != nil {
			t.Fatalf("ToCloudEvent returned nil error but the event is invalid: %v", verr)
		}
		// An empty Type must have been defaulted, never emitted empty.
		if e.Type() == "" {
			t.Fatalf("ToCloudEvent(source=%q, type=%q) produced an event with an empty type", source, typ)
		}
		if typ == "" && e.Type() != DefaultType {
			t.Fatalf("empty Config.Type produced type %q, want %q", e.Type(), DefaultType)
		}
	})
}
