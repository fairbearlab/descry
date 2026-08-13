package sink

import (
	"context"
	"fmt"
	"io"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// EventSink consumes events. Implementations MUST be concurrency-safe.
type EventSink interface {
	Publish(ctx context.Context, e cloudevents.Event) error
}

// StdoutSink writes one marshaled CloudEvent JSON per line.
type StdoutSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewStdoutSink creates a StdoutSink that writes to w.
func NewStdoutSink(w io.Writer) *StdoutSink { return &StdoutSink{w: w} }

// Publish marshals e as JSON and writes it as a single line to the
// underlying writer.
func (s *StdoutSink) Publish(_ context.Context, e cloudevents.Event) error {
	b, err := e.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = fmt.Fprintf(s.w, "%s\n", b)
	return err
}
