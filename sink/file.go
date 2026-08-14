// Package sink defines EventSink and its implementations: StdoutSink (writes
// JSONL to an io.Writer) and FileSink (writes JSONL to a file, append-only,
// concurrency-safe).
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
// The file's mode is tightened to 0o600 after opening — the O_CREATE mode
// argument only governs newly created files, so a pre-existing file with
// looser permissions is chmod'd here too (deterministic 0600, regardless of
// whether this call created the file or reopened an existing one).
func NewFileSink(path string) (*FileSink, error) {
	// #nosec G304 -- path is the user-supplied config file_path / --file flag; writing to it is the feature
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open sink file: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("chmod sink file: %w", err), f.Close())
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
