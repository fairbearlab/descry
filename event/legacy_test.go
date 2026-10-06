package event

import (
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/oklog/ulid/v2"

	"github.com/fairbearlab/descry/check"
)

// legacyPayload and legacyToCloudEvent are the pre-optimisation
// implementation, kept verbatim as the reference oracle: ToCloudEvent must
// produce the same event (modulo the random ID) and the same error for every
// input. Do not "fix" or modernise this code — its value is that it is the
// straightforward SDK-setter version whose output defines correct.
type legacyPayload struct {
	Status     check.Status     `json:"status"`
	StatusCode int              `json:"status_code"`
	LatencyMs  int              `json:"latency_ms"`
	ErrorClass check.ErrorClass `json:"error_class,omitempty"`
	FinalURL   string           `json:"final_url,omitempty"`
	TLSExpiry  *string          `json:"tls_expiry,omitempty"` // RFC3339
	Extra      map[string]any   `json:"extra,omitempty"`
}

func legacyToCloudEvent(obs check.Observation, cfg Config) (cloudevents.Event, error) {
	e := cloudevents.NewEvent() // specversion "1.0"
	e.SetID(ulid.Make().String())
	e.SetSource(cfg.Source)
	if cfg.Type == "" {
		cfg.Type = DefaultType
	}
	e.SetType(cfg.Type)
	e.SetSubject(obs.Labels["url"]) // subject = the "url" label, set by the caller
	e.SetTime(obs.ObservedAt)

	var tls *string
	if obs.TLSExpiry != nil {
		s := obs.TLSExpiry.UTC().Format(time.RFC3339)
		tls = &s
	}
	p := legacyPayload{
		Status:     obs.Status,
		StatusCode: obs.StatusCode,
		LatencyMs:  obs.LatencyMs,
		ErrorClass: obs.ErrorClass,
		FinalURL:   obs.FinalURL,
		TLSExpiry:  tls,
		Extra:      obs.Extra,
	}
	if err := e.SetData(cloudevents.ApplicationJSON, p); err != nil {
		return e, fmt.Errorf("set data: %w", err)
	}
	if err := e.Validate(); err != nil {
		return e, fmt.Errorf("invalid cloudevent: %w", err)
	}
	return e, nil
}
