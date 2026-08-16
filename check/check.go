// Package check defines the engine's core abstractions: the Check interface
// probes run against, the Target it runs against, and the Observation it
// produces. Concrete probes (e.g. checks/http) implement Check; the runner
// package schedules them and hands results to event/sink.
package check

import (
	"context"
	"net/url"
	"time"
)

// RedactURL masks any userinfo (credentials) in a target URL so it is safe to
// log. On parse failure it returns the input unchanged.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
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
type Check interface {
	Name() string
	Run(ctx context.Context, target Target) (Observation, error)
}
