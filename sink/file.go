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
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	torn bool // last write failed and may have left a partial line; see publishLine
}

// NewFileSink opens (or creates) path in append mode and returns a FileSink.
// The file's mode is tightened to 0o600 after opening — the O_CREATE mode
// argument only governs newly created files, so a pre-existing file with
// looser permissions is chmod'd here too (deterministic 0600, regardless of
// whether this call created the file or reopened an existing one).
func NewFileSink(path string) (*FileSink, error) {
	// Write-only, as a sink should be: an existing file that grants write but
	// not read (mode 0200, a write-only ACL) must still open, and the chmod
	// below is what makes it readable for the tail inspection that follows.
	// #nosec G304 -- path is the user-supplied config file_path / --file flag; writing to it is the feature
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open sink file: %w", err)
	}
	if err := f.Chmod(0o600); err != nil {
		return nil, errors.Join(fmt.Errorf("chmod sink file: %w", err), f.Close())
	}
	// A previous process (or a crash mid-write) may have left the file without
	// a trailing newline. Start torn so the first publish terminates that
	// fragment instead of appending its record onto it.
	torn, err := endsWithoutNewline(f)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("inspect sink file: %w", err), f.Close())
	}
	return &FileSink{f: f, w: bufio.NewWriter(f), torn: torn}, nil
}

// endsWithoutNewline reports whether the regular file behind f (opened
// write-only) is non-empty and its last byte is not '\n'. The byte is read
// through a separate short-lived read-only handle on the same path, opened
// after the chmod in NewFileSink so a write-only file is readable by then.
// Empty and non-regular files (a FIFO, /dev/stdout) are never torn.
func endsWithoutNewline(f *os.File) (bool, error) {
	st, err := f.Stat()
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() || st.Size() == 0 {
		return false, nil
	}
	// #nosec G304 -- same user-supplied sink path that f was opened from
	rf, err := os.Open(f.Name())
	if err != nil {
		return false, err
	}
	defer func() { _ = rf.Close() }() // read-only handle; nothing to lose on close
	var last [1]byte
	if _, err := rf.ReadAt(last[:], st.Size()-1); err != nil {
		return false, err
	}
	return last[0] != '\n', nil
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
	return publishLine(s.w, s.f, &s.torn, b) // flushed, no fsync by default
}

// Close flushes any buffered data and closes the underlying file. The file is
// always closed even if the flush fails, so a flush error cannot leak the fd;
// both errors are reported (joined) when they occur together.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.torn {
		// Terminate a torn fragment so the next process (see NewFileSink) or
		// reader starts on a line boundary. Best effort: a failure here is
		// reported with the flush error and the reopen check covers it.
		_ = s.w.WriteByte('\n')
	}
	flushErr := s.w.Flush()
	closeErr := s.f.Close()
	return errors.Join(flushErr, closeErr)
}
