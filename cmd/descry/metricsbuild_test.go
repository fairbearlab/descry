package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Value: protects=run's documented exit 1 when the metrics collector cannot be built, that this early return still releases the bound admin port, and that it comes before the file sink opens (no events file)
// Value: fails_when=the metrics.New error is ignored, maps to 0 or 2, loses its "error: metrics:" line, the deferred listener Close is dropped (the port stays bound), or the collector is built after newFileSink (an events file is left behind)
// Value: why_new=no test reached the collector-build failure; seam=none; no_claim=other Register failure causes (an invalid-UTF-8 version stands in for them)
//
// The failure is driven through the existing ldflags-populated version var;
// listen and notifyContext are the package's existing test hooks.
func TestRun_MetricsBuildFailureExitsOne(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "descry.yaml")
	eventsPath := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(cfgPath, []byte(adminYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	// A build stamped with a version that is not valid UTF-8 cannot register
	// descry_build_info, so metrics.New fails.
	origVersion, origListen, origNotify := version, listen, notifyContext
	t.Cleanup(func() { version, listen, notifyContext = origVersion, origListen, origNotify })
	version = "\xff"
	var addr string
	listen = func(network, a string) (net.Listener, error) {
		ln, err := origListen(network, a)
		if err == nil {
			addr = ln.Addr().String()
		}
		return ln, err
	}
	// Already cancelled: if the failure were ignored, run would return
	// promptly instead of probing until the test times out.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return ctx, func() {}
	}

	var stderr lockedBuffer
	code := run([]string{"--config", cfgPath, "--listen", "127.0.0.1:0", "--sink", "file", "--file", eventsPath}, io.Discard, &stderr)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "error: metrics:") {
		t.Errorf("stderr does not report the metrics failure: %q", stderr.String())
	}
	if _, err := os.Stat(eventsPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a collector-build failure left an events file (stat err = %v)", err)
	}
	if addr == "" {
		t.Fatal("the admin listener was never bound")
	}
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = c.Close()
		t.Errorf("admin port %s still accepts connections after run returned", addr)
	}
}
