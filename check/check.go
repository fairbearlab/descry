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

const (
	StatusUp   Status = "up"
	StatusDown Status = "down"
)

// ErrorClass is a closed, engine-owned enum. The engine does NOT try to match
// Node's error_type strings — lossy translation is the adapter's job.
type ErrorClass string

const (
	ErrNone              ErrorClass = "" // success
	ErrTimeout           ErrorClass = "timeout"
	ErrConnectionRefused ErrorClass = "connection_refused"
	ErrDNSFailure        ErrorClass = "dns_failure"
	ErrTLSError          ErrorClass = "tls_error"
	ErrSSRFBlocked       ErrorClass = "ssrf_blocked"
	ErrHTTPError         ErrorClass = "http_error"
	ErrUnknown           ErrorClass = "unknown"
)

// Target is what a Check runs against. Labels are opaque: carried through,
// never interpreted by the engine.
type Target struct {
	URL    string
	Labels map[string]string
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
