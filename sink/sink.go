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

func NewStdoutSink(w io.Writer) *StdoutSink { return &StdoutSink{w: w} }

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
