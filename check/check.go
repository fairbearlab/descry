// Package check defines the engine's core abstractions: the Check interface
// probes run against, the Target it runs against, and the Observation it
// produces. Concrete probes (e.g. checks/http) implement Check; the runner
// package schedules them and hands results to event/sink.
package check

import (
	"context"
	"errors"
	"net/url"
	"time"
)

// ErrNotRun reports that a check could not run at all, so it has no evidence
// about the target either way: the failure was local to the prober (running
// out of file descriptors or ephemeral ports, say), not an observation of the
// target. A Check returns it, wrapped with the cause, as Run's error instead
// of a down Observation. Test with errors.Is.
//
// The runner reports it on Results like any other Run error, and publishes no
// event. The metrics decorator records no verdict for it: up, runs_total,
// errors_total, latency and last_run are left alone, and the not-run series
// count it instead (see package metrics).
var ErrNotRun = errors.New("check not run")

// RedactURL masks any userinfo (credentials) in a target URL so it is safe to
// log: a password becomes "xxxxx" (the username stays, as in url.URL.Redacted),
// and a bare username with no password — the shape of a token-in-URL such as
// https://<token>@api.example.com — is masked too, since there it is the secret.
// Query strings are left alone. On parse failure it returns the fixed
// placeholder "<unparseable>" rather than echoing input that may still carry
// credentials; the placeholder itself passes through unchanged, so redacting
// twice is the same as redacting once.
func RedactURL(raw string) string {
	const unparseable = "<unparseable>"
	if raw == unparseable {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return unparseable
	}
	if u.User != nil {
		if _, hasPW := u.User.Password(); !hasPW && u.User.Username() != "" {
			u.User = url.User("xxxxx")
		}
	}
	return u.Redacted()
}

// Status is a closed, engine-owned enum. v1: StatusUp | StatusDown only.
type Status string

// The two possible Status values a Check can report.
const (
	StatusUp   Status = "up"   // the target responded and passed the check's success criteria
	StatusDown Status = "down" // the target failed to respond, or failed the success criteria
)

// ErrorClass is a closed, engine-owned enum. The engine does NOT try to match
// a consumer's own `error_type` vocabulary — lossy translation is the adapter's job.
type ErrorClass string

// The ErrorClass values a Check may report on a failed Observation. Exactly
// one applies per failure; adapters should map their own error taxonomy onto
// this set rather than the engine widening it to match every adapter.
const (
	ErrNone              ErrorClass = ""                   // no error; Status is StatusUp
	ErrTimeout           ErrorClass = "timeout"            // request exceeded its deadline
	ErrConnectionRefused ErrorClass = "connection_refused" // TCP connect was refused or reset
	ErrDNSFailure        ErrorClass = "dns_failure"        // hostname failed to resolve
	ErrTLSError          ErrorClass = "tls_error"          // certificate or handshake failure
	ErrSSRFBlocked       ErrorClass = "ssrf_blocked"       // target blocked by the SSRF guard
	ErrHTTPError         ErrorClass = "http_error"         // request completed with a non-success status
	ErrUnknown           ErrorClass = "unknown"            // failure did not match a known class
)

// Target is what a Check runs against. Labels are opaque: carried through,
// never interpreted by the engine.
//
// Interval is the cadence at which the runner schedules this target; the
// cadence rides with the target, as in a Prometheus per-target scrape
// interval. Zero (or negative) means "use the runner's default interval".
type Target struct {
	URL      string
	Labels   map[string]string
	Interval time.Duration
}

// Observation is the generic result of a single Check run.
type Observation struct {
	Status     Status
	LatencyMs  int
	ErrorClass ErrorClass
	StatusCode int        // 0 when no HTTP response
	FinalURL   string     // after redirects
	TLSExpiry  *time.Time // nil when not HTTPS / unavailable
	ObservedAt time.Time  // authoritative; CloudEvent time = this
	Labels     map[string]string
	Extra      map[string]any // non-generic needs (body, consumer-specific payload, etc.)
}

// Check produces an Observation for a Target.
//
// Run reports what it observed of the target as an Observation with a nil
// error, a down target included: a timeout, a refused connection or a 500 is
// a successful check of a down target. A non-nil error means the run produced
// no Observation. Wrap ErrNotRun in it when the check itself could not run
// for a reason local to the prober, so the error is not read as a verdict; a
// run abandoned because ctx was cancelled may return ctx's error, and the
// runner drops whatever a run returns once its context is done.
type Check interface {
	Name() string
	Run(ctx context.Context, target Target) (Observation, error)
}
