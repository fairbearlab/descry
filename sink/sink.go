package sink

import (
	"bufio"
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

// writeLine writes b followed by a newline to w. It writes b directly
// (no append-triggered reallocation) and the newline as a single byte.
// Callers are responsible for any locking and flushing.
func writeLine(w *bufio.Writer, b []byte) error {
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

// StdoutSink writes one marshaled CloudEvent JSON per line.
type StdoutSink struct {
	mu sync.Mutex
	w  *bufio.Writer
}

// NewStdoutSink creates a StdoutSink that writes to w.
func NewStdoutSink(w io.Writer) *StdoutSink { return &StdoutSink{w: bufio.NewWriter(w)} }

// Publish marshals e as JSON and writes it as a single line to the
// underlying writer. The bufio.Writer is flushed after every write (no
// fsync), matching the "flushed before Publish returns" durability
// contract.
func (s *StdoutSink) Publish(_ context.Context, e cloudevents.Event) error {
	b, err := e.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeLine(s.w, b); err != nil {
		return err
	}
	return s.w.Flush()
}
