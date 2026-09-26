// Package event maps a check.Observation onto a validated CloudEvents 1.0
// envelope, ready to hand to a sink.EventSink.
package event

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	cetypes "github.com/cloudevents/sdk-go/v2/types"
	"github.com/oklog/ulid/v2"

	"github.com/fairbearlab/descry/check"
)

// DefaultType is the CloudEvents "type" attribute used when Config.Type is
// left empty.
const DefaultType = "dev.descry.observation.v1"

// Config carries the static envelope fields.
type Config struct {
	Source string // e.g. "descry/example"
	Type   string // defaults to DefaultType when empty
}

// payload is the typed generic HTTP data payload. Extra is merged in as a map
// so genuinely non-generic fields ride alongside the typed ones.
type payload struct {
	Status     check.Status     `json:"status"`
	StatusCode int              `json:"status_code"`
	LatencyMs  int              `json:"latency_ms"`
	ErrorClass check.ErrorClass `json:"error_class,omitempty"`
	FinalURL   string           `json:"final_url,omitempty"`
	TLSExpiry  *jsonText        `json:"tls_expiry,omitempty"` // RFC3339, UTC
	Extra      map[string]any   `json:"extra,omitempty"`
}

// jsonText is a pre-encoded JSON value. As a pointer field it marshals
// without allocating: encoding/json boxes the pointer, not a copy.
type jsonText struct{ b []byte }

func (t *jsonText) MarshalJSON() ([]byte, error) { return t.b, nil }

// envelope co-locates everything one event points at — the V1 context and
// the backing storage for its pointer attributes, the ULID bytes, and the
// payload being marshaled — so building an event costs one allocation where
// the SDK setters cost one per attribute.
type envelope struct {
	ec      cloudevents.EventContextV1
	subject string
	ts      cetypes.Timestamp
	ct      string
	id      ulid.ULID
	p       payload
	tls     jsonText
	tlsBuf  [len(`"2006-01-02T15:04:05Z"`) + 2]byte // 4-digit years fit; others grow via append
}

// setPayload fills e.p from obs. The TLS expiry is formatted into e.tlsBuf.
func (e *envelope) setPayload(obs check.Observation) {
	e.p = payload{
		Status:     obs.Status,
		StatusCode: obs.StatusCode,
		LatencyMs:  obs.LatencyMs,
		ErrorClass: obs.ErrorClass,
		FinalURL:   obs.FinalURL,
		Extra:      obs.Extra,
	}
	if obs.TLSExpiry != nil {
		b := append(e.tlsBuf[:0], '"')
		b = obs.TLSExpiry.UTC().AppendFormat(b, time.RFC3339)
		b = append(b, '"')
		e.tls.b = b
		e.p.TLSExpiry = &e.tls
	}
}

// sourceEntry is Config.Source parsed once, with the verdict of the SDK's
// Validate rule for it ("source: REQUIRED" when it does not round-trip to a
// non-blank string). Immutable once published.
type sourceEntry struct {
	raw string
	ref cetypes.URIRef
	ok  bool
}

// lastSource caches the most recent Config.Source. A process normally uses a
// single source, so one entry hits every time; a miss only re-parses.
var lastSource atomic.Pointer[sourceEntry]

func sourceFor(raw string) *sourceEntry {
	if s := lastSource.Load(); s != nil && s.raw == raw {
		return s
	}
	s := &sourceEntry{raw: raw}
	if u, err := url.Parse(raw); err == nil {
		s.ref = cetypes.URIRef{URL: *u}
		s.ok = strings.TrimSpace(s.ref.String()) != ""
	}
	lastSource.Store(s)
	return s
}

// monotonic is ulid.Make's entropy source, asserted once so newID can read
// into heap memory it already owns (ulid.Make's stack ULID escapes through
// the io.Reader interface call).
var monotonic, _ = ulid.DefaultEntropy().(ulid.MonotonicReader)

// newID is ulid.Make().String() with the 16 ULID bytes stored in e.
func (e *envelope) newID() string {
	if monotonic == nil {
		return ulid.Make().String()
	}
	ms := ulid.Now()
	// Both calls can only fail in ways ulid.Make also panics on (a timestamp
	// past year 10889, or 2^80 IDs within one millisecond).
	if err := e.id.SetTime(ms); err != nil {
		panic(err)
	}
	if err := monotonic.MonotonicRead(ms, e.id[6:]); err != nil {
		panic(err)
	}
	return e.id.String()
}

// ToCloudEvent maps an Observation to a validated CloudEvents 1.0 event.
//
// The common case writes the V1 context directly instead of going through
// the SDK setters and Validate: every attribute it sets is valid by
// construction except source and type, which depend only on cfg and are
// checked here. Anything that could fail — a source or type the SDK would
// reject, or Extra that does not marshal — takes the SDK path, so errors and
// the returned event are exactly the SDK's.
func ToCloudEvent(obs check.Observation, cfg Config) (cloudevents.Event, error) {
	if cfg.Type == "" {
		cfg.Type = DefaultType
	}
	typ := strings.TrimSpace(cfg.Type) // as SetType stores it
	if src := sourceFor(cfg.Source); src.ok && typ != "" {
		if e, ok := build(obs, src, typ); ok {
			return e, nil
		}
	}
	return viaSDK(obs, cfg)
}

// build is the direct construction. It must produce an event deeply equal
// to viaSDK's (legacy_test.go holds the oracle); ok is false only when the
// payload does not marshal.
func build(obs check.Observation, src *sourceEntry, typ string) (cloudevents.Event, bool) {
	env := new(envelope)
	env.ec.ID = env.newID()
	env.ec.Source = src.ref
	env.ec.Type = typ
	env.ct = cloudevents.ApplicationJSON
	env.ec.DataContentType = &env.ct
	if s := strings.TrimSpace(obs.Labels["url"]); s != "" { // subject = the "url" label, set by the caller
		env.subject = s
		env.ec.Subject = &env.subject
	}
	if !obs.ObservedAt.IsZero() {
		env.ts = cetypes.Timestamp{Time: obs.ObservedAt}
		env.ec.Time = &env.ts
	}

	env.setPayload(obs)
	data, err := json.Marshal(&env.p)
	// Drop the payload's references (Extra, FinalURL) so the event does not
	// keep the observation's data reachable.
	env.p, env.tls = payload{}, jsonText{}
	if err != nil {
		return cloudevents.Event{}, false
	}
	return cloudevents.Event{Context: &env.ec, DataEncoded: data}, true
}

// viaSDK builds the event through the SDK setters and Validate. It is the
// error path, and the definition the fast path is tested against.
func viaSDK(obs check.Observation, cfg Config) (cloudevents.Event, error) {
	e := cloudevents.NewEvent() // specversion "1.0"
	e.SetID(ulid.Make().String())
	e.SetSource(cfg.Source)
	e.SetType(cfg.Type)
	e.SetSubject(obs.Labels["url"]) // subject = the "url" label, set by the caller
	e.SetTime(obs.ObservedAt)

	var env envelope
	env.setPayload(obs)
	if err := e.SetData(cloudevents.ApplicationJSON, &env.p); err != nil {
		return e, fmt.Errorf("set data: %w", err)
	}
	if err := e.Validate(); err != nil {
		return e, fmt.Errorf("invalid cloudevent: %w", err)
	}
	return e, nil
}
