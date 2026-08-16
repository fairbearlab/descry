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

// writeLine writes b followed by a newline to w. It writes b directly (no
// append-triggered reallocation) and the newline as a single byte. Callers are
// responsible for any locking and flushing.
//
// A line larger than the buffer is the exception: bufio would hand b straight
// to the underlying writer and buffer the newline for the following flush,
// i.e. two write calls, and with several processes appending to one file
// another writer's line could land between them. Such a line is written as one
// append(b, '\n') so it stays a single write; MarshalJSON's slice usually has
// the spare byte, and when it does not, the reallocation is the price of a
// line that big.
func writeLine(w *bufio.Writer, b []byte) error {
	if len(b)+1 > w.Available() {
		if w.Buffered() > 0 {
			if err := w.Flush(); err != nil {
				return err
			}
		}
		_, err := w.Write(append(b, '\n'))
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

// publishLine writes b as one line through w and flushes it. On any error the
// buffer is reset onto resetTo: bufio.Writer latches its first error and
// returns it from every later call, which would turn one transient failure of
// the underlying writer into a permanently wedged sink. Reset drops the
// buffered line (this publish has already failed) and starts the next one
// clean.
//
// A failed write may have been partial (a regular file returns the bytes it
// managed plus ENOSPC), leaving a torn fragment with no newline in the output.
// *torn records whether any byte reached the underlying writer, and the next
// successful publish then first emits a bare '\n' so the fragment is
// terminated as its own (unparseable, skippable) line instead of swallowing
// the record that follows it. The flag is sticky: it clears only when a
// publish fully succeeds, so a run of failures (partial, then zero-byte) keeps
// the obligation until the separator is actually written. Callers hold their
// own lock around the call.
func publishLine(w *bufio.Writer, resetTo io.Writer, torn *bool, b []byte) error {
	handed := 0 // bytes handed to w this call; minus w.Buffered() = bytes delivered
	var err error
	if *torn {
		if err = w.WriteByte('\n'); err == nil {
			handed++
		}
	}
	if err == nil {
		var n int
		n, err = w.Write(b)
		handed += n
	}
	if err == nil {
		if err = w.WriteByte('\n'); err == nil {
			handed++
		}
	}
	if err == nil {
		err = w.Flush()
	}
	if err != nil {
		*torn = *torn || handed-w.Buffered() > 0
		w.Reset(resetTo)
		return err
	}
	*torn = false
	return nil
}

// StdoutSink writes one marshaled CloudEvent JSON per line.
type StdoutSink struct {
	mu   sync.Mutex
	raw  io.Writer     // the caller's writer; kept so a failed write can be reset
	w    *bufio.Writer // wraps raw; flushed inside the lock every Publish
	torn bool          // last write failed and may have left a partial line
}

// NewStdoutSink creates a StdoutSink that writes to w.
func NewStdoutSink(w io.Writer) *StdoutSink {
	return &StdoutSink{raw: w, w: bufio.NewWriter(w)}
}

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
	return publishLine(s.w, s.raw, &s.torn, b)
}
