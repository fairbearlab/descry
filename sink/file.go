package sink

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// FileSink is an append-only JSONL event sink (one CloudEvent per line).
// It is concurrency-safe via an internal mutex.
type FileSink struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// NewFileSink opens (or creates) path in append mode and returns a FileSink.
func NewFileSink(path string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open sink file: %w", err)
	}
	return &FileSink{f: f, w: bufio.NewWriter(f)}, nil
}

// Publish marshals e as JSON and appends it as a single line to the file.
// The bufio.Writer is flushed after every write (no fsync).
func (s *FileSink) Publish(_ context.Context, e cloudevents.Event) error {
	b, err := e.MarshalJSON()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.w.Flush() // no fsync by default
}

// Close flushes any buffered data and closes the underlying file. The file is
// always closed even if the flush fails, so a flush error cannot leak the fd;
// both errors are reported (joined) when they occur together.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	flushErr := s.w.Flush()
	closeErr := s.f.Close()
	return errors.Join(flushErr, closeErr)
}
