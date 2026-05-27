package event

import (
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/oklog/ulid/v2"

	"github.com/fairbearlab/descry/check"
)

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

// ToCloudEvent maps an Observation to a validated CloudEvents 1.0 event.
func ToCloudEvent(obs check.Observation, cfg Config) (cloudevents.Event, error) {
	e := cloudevents.NewEvent() // specversion "1.0"
	e.SetID(ulid.Make().String())
	e.SetSource(cfg.Source)
	if cfg.Type == "" {
		cfg.Type = DefaultType
	}
	e.SetType(cfg.Type)
	e.SetSubject(obs.Labels["url"]) // subject = target URL (set by caller via Labels or FinalURL)
	e.SetTime(obs.ObservedAt)

	var tls *string
	if obs.TLSExpiry != nil {
		s := obs.TLSExpiry.UTC().Format("2006-01-02T15:04:05Z07:00")
		tls = &s
	}
	p := payload{
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
