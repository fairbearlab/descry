package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// targetsYAML is a config with the given target URLs, quoted.
func targetsYAML(urls ...string) string {
	var b strings.Builder
	b.WriteString("source: https://example.com/descry\ninterval: 1s\ntimeout: 500ms\ntargets:\n")
	for _, u := range urls {
		b.WriteString("  - url: \"" + u + "\"\n")
	}
	return b.String()
}

// Value: protects=the shared-label refusal: with --listen and per-target series, two targets on one metrics label exit 2 before the port is bound, naming positions and the clean label, never a URL
// Value: fails_when=the gate is skipped, runs after the bind, misses exact duplicates or userinfo/query variants, misformats 3+ positions, or echoes a credential or query in the message
// Value: why_new=the refusal is new; seam=listen counts calls (any call fails the row), notifyContext is already cancelled
func TestRun_LabelCollisionExitsTwo(t *testing.T) {
	var calls atomic.Int32
	origListen, origNotify := listen, notifyContext
	t.Cleanup(func() { listen, notifyContext = origListen, origNotify })
	// Already "signalled": a config the gate wrongly lets through exits 0 at
	// once instead of running until the test times out.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return ctx, func() {}
	}
	listen = func(network, addr string) (net.Listener, error) {
		calls.Add(1)
		return net.Listen(network, addr)
	}

	for _, tc := range []struct {
		name string
		urls []string
		want []string // substrings of stderr
	}{
		{"query variants", []string{"https://h/p?a=1", "https://h/p?b=2"},
			[]string{`targets 1 and 2 share the metrics label "https://h/p"`, "--metrics-per-target=false"}},
		{"userinfo variant", []string{"https://" + cred + "@h/p", "https://h/p"},
			[]string{`targets 1 and 2 share the metrics label "https://h/p"`}},
		{"exact duplicate", []string{"https://h/p", "https://h/p"},
			[]string{`targets 1 and 2 share the metrics label "https://h/p"`}},
		{"three of one label", []string{"https://h/p?a=1", "https://other/", "https://h/p#f", "https://h/p"},
			[]string{`targets 1, 3 and 4 share the metrics label "https://h/p"`}},
		{"first shared label in config order", []string{"https://a/", "https://b/", "https://b/?x", "https://a/?y"},
			[]string{`targets 1 and 4 share the metrics label "https://a/"`}},
		{"two unparseable", []string{"https://h/%zz", "https://ok/", "https://h/@me"},
			[]string{`targets 1 and 3 have no safe metrics label (each renders as "<unparseable>")`, "%40"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfgPath := filepath.Join(t.TempDir(), "descry.yaml")
			if err := os.WriteFile(cfgPath, []byte(targetsYAML(tc.urls...)), 0o600); err != nil {
				t.Fatal(err)
			}
			calls.Store(0)
			var stdout, stderr bytes.Buffer
			if code := run([]string{"--config", cfgPath, "--listen", "127.0.0.1:0"}, &stdout, &stderr); code != 2 {
				t.Fatalf("exit code = %d, want 2; stderr:\n%s", code, stderr.String())
			}
			out := stderr.String()
			for _, w := range tc.want {
				if !strings.Contains(out, w) {
					t.Errorf("stderr lacks %q:\n%s", w, out)
				}
			}
			if n := strings.Count(out, "error:"); n != 1 {
				t.Errorf("want one error line, got %d:\n%s", n, out)
			}
			for _, leak := range []string{secretPW, "?", "a=1", "b=2", "%zz", "@me"} {
				if strings.Contains(out, leak) {
					t.Errorf("stderr echoes %q from a target URL:\n%s", leak, out)
				}
			}
			if n := calls.Load(); n != 0 {
				t.Errorf("listen called %d times: the collision must be refused before the bind", n)
			}
			if stdout.Len() != 0 {
				t.Errorf("events written before the refusal: %q", stdout.String())
			}
		})
	}
}

// Value: protects=the gate's scope: shared labels are refused only when per-target series are served, and a lone <unparseable> target is not a collision
// Value: fails_when=the refusal applies without --listen (breaking v0.4.0 configs with query variants), ignores --metrics-per-target=false, or treats one placeholder as a clash
// Value: why_new=the refusal is new; seam=notifyContext (and listen, wrapped only to learn the port)
func TestRun_LabelCollisionGate(t *testing.T) {
	queryVariants := targetsYAML("http://127.0.0.1:1/p?a=1", "http://127.0.0.1:1/p?b=2")

	t.Run("no listen", func(t *testing.T) {
		cfgPath := filepath.Join(t.TempDir(), "descry.yaml")
		if err := os.WriteFile(cfgPath, []byte(queryVariants), 0o600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		origNotify := notifyContext
		t.Cleanup(func() { notifyContext = origNotify })
		notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
			return ctx, func() {}
		}
		var stderr lockedBuffer
		if code := run([]string{"--config", cfgPath}, io.Discard, &stderr); code != 0 {
			t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, stderr.String())
		}
	})

	for _, tc := range []struct {
		name  string
		yaml  string
		extra []string
	}{
		{"per-target off", queryVariants, []string{"--metrics-per-target=false"}},
		{"single unparseable", targetsYAML("http://127.0.0.1:1/@me"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ar := startAdminRun(t, tc.yaml, tc.extra...)
			waitFor(t, func() bool {
				code, _ := scrape(t, ar.addr, "/readyz")
				return code == http.StatusOK
			})
			ar.cancel()
			ar.waitExit(t, 0)
			if strings.Contains(ar.stderr.String(), "error:") {
				t.Errorf("stderr has an error: %q", ar.stderr.String())
			}
		})
	}
}

// TestJoinPositions: the position list reads naturally at every length the
// collision message can produce.
func TestJoinPositions(t *testing.T) {
	for _, tc := range []struct {
		in   []int
		want string
	}{
		{[]int{1, 2}, "1 and 2"},
		{[]int{1, 4, 6}, "1, 4 and 6"},
		{[]int{2, 3, 5, 9}, "2, 3, 5 and 9"},
	} {
		if got := joinPositions(tc.in); got != tc.want {
			t.Errorf("joinPositions(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
