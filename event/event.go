// Package event maps a check.Observation onto a validated CloudEvents 1.0
// envelope, ready to hand to a sink.EventSink.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
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
	TLSExpiry  *string          `json:"tls_expiry,omitempty"` // RFC3339
	Extra      map[string]any   `json:"extra,omitempty"`
}

// errNotInitialised is returned by Encode on a nil or zero-value Encoder.
var errNotInitialised = errors.New("event: Encoder not initialised; use NewEncoder")

// Encoder maps Observations onto CloudEvents 1.0 events for one fixed Config.
// NewEncoder validates the config's source and type once, so Encode can build
// each event without re-parsing or re-validating them. Compile it once and
// reuse it, as with regexp.MustCompile: an Encoder is immutable after
// NewEncoder returns and is safe for concurrent use by multiple goroutines.
//
// The zero value is not usable; build one with NewEncoder. Encode on a nil or
// zero Encoder returns an error.
type Encoder struct {
	source cloudevents.URIRef // parsed by the SDK's SetSource on the probe event
	typ    string             // DefaultType applied, trimmed by the SDK's SetType
	ok     bool               // set only by NewEncoder
}

// NewEncoder validates cfg and returns an Encoder for it. An empty cfg.Type
// becomes DefaultType. The source and type are checked with the CloudEvents
// SDK's own setters and Validate on a probe event, so NewEncoder accepts and
// rejects exactly what a per-event Validate would, with the same error text
// (prefixed "invalid cloudevent: ").
func NewEncoder(cfg Config) (*Encoder, error) {
	if cfg.Type == "" {
		cfg.Type = DefaultType
	}
	probe := cloudevents.NewEvent() // specversion "1.0"
	probe.SetID("probe")
	probe.SetSource(cfg.Source)
	probe.SetType(cfg.Type)
	probe.SetDataContentType(cloudevents.ApplicationJSON)
	if err := probe.Validate(); err != nil {
		return nil, fmt.Errorf("invalid cloudevent: %w", err)
	}
	// AsV1 returns a copy of the probe's context: the stored source and type
	// are the SDK's parsed and trimmed forms, never re-derived here.
	ec := probe.Context.AsV1()
	return &Encoder{source: ec.Source, typ: ec.Type, ok: true}, nil
}

// Encode maps an Observation to a CloudEvents 1.0 event. It returns a zero
// Event and an error, prefixed "set data: ", only when the payload cannot be
// marshaled to JSON (e.g. a chan or NaN in obs.Extra); the Encoder stays
// usable for the next call.
//
// Encode skips the per-event Validate. That is sound because every attribute
// Validate checks is either fixed or valid by construction:
//   - id is a fresh ULID, never empty;
//   - source and type are copied from the probe NewEncoder validated, not
//     re-parsed. The source's url.URL is copied by value; its only pointer,
//     User, points at an immutable url.Userinfo;
//   - subject, time and datacontenttype go through the SDK's EventContextV1
//     setters, which store nil for an empty (trimmed) subject or a zero time,
//     and Validate has no time check;
//   - each event gets its own *EventContextV1, so events share no mutable
//     state and Encode never writes to the Encoder;
//   - DataBase64 stays false and FieldErrors nil, as after a successful
//     SetData on the SDK's own path.
//
// Output is byte-identical to the SDK-setter construction (legacy_test.go)
// apart from the random id.
func (enc *Encoder) Encode(obs check.Observation) (cloudevents.Event, error) {
	if enc == nil || !enc.ok {
		return cloudevents.Event{}, errNotInitialised
	}
	var tls *string
	if obs.TLSExpiry != nil {
		s := obs.TLSExpiry.UTC().Format(time.RFC3339)
		tls = &s
	}
	data, err := json.Marshal(&payload{
		Status:     obs.Status,
		StatusCode: obs.StatusCode,
		LatencyMs:  obs.LatencyMs,
		ErrorClass: obs.ErrorClass,
		FinalURL:   obs.FinalURL,
		TLSExpiry:  tls,
		Extra:      obs.Extra,
	})
	if err != nil {
		return cloudevents.Event{}, fmt.Errorf("set data: %w", err)
	}

	ec := &cloudevents.EventContextV1{
		ID:     ulid.Make().String(),
		Source: enc.source,
		Type:   enc.typ,
	}
	// The EventContextV1 setters never fail for these attributes.
	_ = ec.SetSubject(obs.Labels["url"]) // subject = the "url" label, set by the caller
	_ = ec.SetTime(obs.ObservedAt)
	_ = ec.SetDataContentType(cloudevents.ApplicationJSON)
	return cloudevents.Event{Context: ec, DataEncoded: data}, nil
}

// ToCloudEvent maps an Observation to a validated CloudEvents 1.0 event. It
// builds an Encoder per call; for repeated use with one Config, call
// NewEncoder once and reuse it (like regexp.MustCompile).
//
// Config errors ("invalid cloudevent: ...") are reported before payload
// errors ("set data: ..."), and any error comes with a zero Event.
func ToCloudEvent(obs check.Observation, cfg Config) (cloudevents.Event, error) {
	enc, err := NewEncoder(cfg)
	if err != nil {
		return cloudevents.Event{}, err
	}
	return enc.Encode(obs)
}
