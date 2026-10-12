package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/fairbearlab/descry/check"
	"github.com/fairbearlab/descry/config"
	"github.com/fairbearlab/descry/event"
	"github.com/fairbearlab/descry/metrics"
	"github.com/fairbearlab/descry/runner"
)

// secretPW is a password built at runtime so no literal credential sits in
// the source; every test that plants userinfo checks that it never reaches an
// event.
var secretPW = "sec" + "ret"

// cred is "user:<secretPW>", the userinfo the tests plant in target URLs.
var cred = "us" + "er:" + secretPW

// Value: protects=the event-URL rule: subject and final_url are rendered exactly like the metrics url label, so a credential, token or fragment never reaches an event and the two join on one key
// Value: fails_when=eventURL diverges from metrics.TargetLabel, is not idempotent, or turns an absent final URL into a placeholder
// Value: why_new=eventURL is new; seam=none
func TestEventURL(t *testing.T) {
	for _, tc := range []struct {
		name, in, want string
	}{
		{"userinfo", "https://" + cred + "@h/p", "https://h/p"},
		{"password only", "https://:" + secretPW + "@h/p", "https://h/p"},
		{"token as username", "https://" + secretPW + "@h/p", "https://h/p"},
		{"query", "https://h/p?token=x", "https://h/p"},
		{"bare question mark", "https://h/p?", "https://h/p"},
		{"fragment", "https://h/p#f", "https://h/p"},
		{"empty path", "https://h", "https://h/"},
		{"port kept", "https://h:8443/p", "https://h:8443/p"},
		{"bad escape", "https://h/%zz", "<unparseable>"},
		{"literal @ in path", "https://h/@me", "<unparseable>"},
		{"misparsed password", "https://user:12/abc@host/", "<unparseable>"},
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := eventURL(tc.in)
			if got != tc.want {
				t.Fatalf("eventURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if tc.in != "" && got != metrics.TargetLabel(tc.in) {
				t.Errorf("eventURL(%q) = %q, but the metrics label is %q: events and series no longer join", tc.in, got, metrics.TargetLabel(tc.in))
			}
			if again := eventURL(got); again != got {
				t.Errorf("not idempotent: eventURL(%q) = %q", got, again)
			}
			if strings.Contains(got, secretPW) {
				t.Errorf("eventURL(%q) leaks the credential: %q", tc.in, got)
			}
		})
	}
}

// stubCheck returns a fixed Observation and error.
type stubCheck struct {
	obs check.Observation
	err error
}

func (stubCheck) Name() string { return "stub" }
func (c stubCheck) Run(context.Context, check.Target) (check.Observation, error) {
	return c.obs, c.err
}

// eventData decodes an event's JSON payload into a map.
func eventData(t *testing.T, ev cloudevents.Event) map[string]any {
	t.Helper()
	var data map[string]any
	if err := json.Unmarshal(ev.Data(), &data); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	return data
}

// Value: protects=final_url in the serialized event: the wrapper the CLI puts around its check rewrites Observation.FinalURL before the runner's encoder sees it
// Value: fails_when=sanitizedCheck stops rewriting FinalURL, rewrites an empty one into a placeholder, changes Name, or swallows the inner error
// Value: why_new=final_url was published raw; seam=a stub inner check and the real event encoder
func TestSanitizedCheck_FinalURLEncodedClean(t *testing.T) {
	raw := "https://" + cred + "@example.com/dest?token=x#f"
	inner := stubCheck{obs: check.Observation{
		Status:     check.StatusUp,
		StatusCode: 200,
		FinalURL:   raw,
		ObservedAt: time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC),
		Labels:     map[string]string{"url": eventURL(raw)},
	}}
	chk := sanitizedCheck{inner: inner}
	if chk.Name() != "stub" {
		t.Errorf("Name = %q, want the inner check's name", chk.Name())
	}
	obs, err := chk.Run(context.Background(), check.Target{URL: raw})
	if err != nil {
		t.Fatal(err)
	}
	enc, err := event.NewEncoder(event.Config{Source: "https://example.com/descry"})
	if err != nil {
		t.Fatal(err)
	}
	ev, err := enc.Encode(obs)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ev.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{secretPW, "token", "#f"} {
		if bytes.Contains(b, []byte(leak)) {
			t.Errorf("serialized event contains %q: %s", leak, b)
		}
	}
	if got := eventData(t, ev)["final_url"]; got != "https://example.com/dest" {
		t.Errorf("final_url = %v, want https://example.com/dest", got)
	}
	if ev.Subject() != "https://example.com/dest" {
		t.Errorf("subject = %q, want https://example.com/dest", ev.Subject())
	}

	// An Observation with no final URL (no response) keeps omitting the field.
	inner.obs.FinalURL = ""
	obs, err = sanitizedCheck{inner: inner}.Run(context.Background(), check.Target{URL: raw})
	if err != nil {
		t.Fatal(err)
	}
	ev, err = enc.Encode(obs)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := eventData(t, ev)["final_url"]; ok {
		t.Errorf("empty FinalURL produced a final_url key: %s", ev.Data())
	}

	// The inner error passes through untouched.
	boom := errors.New("check could not run")
	if _, err := (sanitizedCheck{inner: stubCheck{err: boom}}).Run(context.Background(), check.Target{URL: raw}); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the inner error", err)
	}
}

// getCheck does a real GET of the target URL with net/http and reports the
// final request URL the way the bundled HTTP check does. The bundled check
// cannot reach an httptest server (its SSRF guard refuses loopback and
// non-standard ports), so this stands in for it.
type getCheck struct{ client *http.Client }

func (getCheck) Name() string { return "get" }

func (c getCheck) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	obs := check.Observation{Labels: t.Labels, ObservedAt: time.Now().UTC()}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return obs, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return obs, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	obs.Status = check.StatusUp
	obs.StatusCode = resp.StatusCode
	obs.FinalURL = resp.Request.URL.String()
	return obs, nil
}

// captureSink keeps every published event.
type captureSink struct {
	mu     sync.Mutex
	events []cloudevents.Event
}

func (s *captureSink) Publish(_ context.Context, e cloudevents.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *captureSink) first() (cloudevents.Event, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		return cloudevents.Event{}, false
	}
	return s.events[0], true
}

// Value: protects=final_url after a real redirect: net/http keeps the target's userinfo across a relative redirect and carries the redirect's query, and the event the runner publishes must hold neither
// Value: fails_when=the wrapper misses the real net/url carry-over (only a hand-built URL is cleaned), or the Observation it returns is not the one the runner encodes; it builds its own wrapper and runner, so it does not prove run() wraps the production check (no test does: see TODOS)
// Value: why_new=the leak was verified but never tested; seam=a GET-only inner check against a resolving httptest server, wrapped in sanitizedCheck and driven by a real runner into a capturing sink
func TestSanitizedCheck_RealRedirectFinalURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/r" {
			http.Redirect(w, r, "/dest?sig=abc", http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	target := "http://" + cred + "@" + host + "/r?token=t"

	inner := getCheck{client: srv.Client()}
	tgt := check.Target{URL: target, Labels: map[string]string{"url": eventURL(target)}}

	// Precondition: the unwrapped final URL carries the credential, so the
	// rest of the test is cleaning a real leak, not a hypothetical one.
	raw, err := inner.Run(context.Background(), tgt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(raw.FinalURL, secretPW) || !strings.Contains(raw.FinalURL, "sig=abc") {
		t.Fatalf("precondition: net/http final URL %q should carry the userinfo and the redirect query", raw.FinalURL)
	}
	if u, err := url.Parse(raw.FinalURL); err != nil || u.Path != "/dest" {
		t.Fatalf("precondition: redirect not followed: %q (%v)", raw.FinalURL, err)
	}

	s := &captureSink{}
	r := runner.New(sanitizedCheck{inner: inner}, s, event.Config{Source: "https://example.com/descry"},
		[]check.Target{tgt}, time.Second, 1)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(ctx) }()
	go func() {
		// Drain so the runner never has to drop a Result.
		for res := range r.Results() {
			_ = res
		}
	}()
	var ev cloudevents.Event
	waitFor(t, func() bool {
		var ok bool
		ev, ok = s.first()
		return ok
	})
	cancel()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}

	b, err := ev.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want := "http://" + host + "/dest"
	if got := eventData(t, ev)["final_url"]; got != want {
		t.Errorf("final_url = %v, want %s", got, want)
	}
	for _, leak := range []string{secretPW, "sig", "token"} {
		if bytes.Contains(b, []byte(leak)) {
			t.Errorf("serialized event contains %q: %s", leak, b)
		}
	}
}

// Value: protects=the event subject in a real run: a target with userinfo and a query publishes scheme://host/path as its subject
// Value: fails_when=buildTargets fills the url label with the raw URL again, or run stops going through buildTargets
// Value: why_new=the subject carried the raw target URL; seam=notifyContext swapped for a context the test cancels
func TestRun_EventSubjectCarriesNoCredentials(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "descry.yaml")
	target := "http://" + cred + "@127.0.0.1:1/p?token=x"
	yaml := "source: https://example.com/descry\ninterval: 1s\ntimeout: 500ms\ntargets:\n  - url: \"" + target + "\"\n"
	if err := os.WriteFile(cfgPath, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	outFile := filepath.Join(dir, "events.jsonl")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	orig := notifyContext
	t.Cleanup(func() { notifyContext = orig })
	notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return ctx, func() {}
	}

	var stderr lockedBuffer
	done := make(chan int, 1)
	go func() {
		done <- run([]string{"--config", cfgPath, "--sink", "file", "--file", outFile}, io.Discard, &stderr)
	}()
	waitFor(t, func() bool {
		b, err := os.ReadFile(outFile)
		return err == nil && bytes.Contains(b, []byte("dev.descry.observation.v1"))
	})
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit code = %d, want 0; stderr:\n%s", code, stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after its context was cancelled")
	}

	b, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"subject":"http://127.0.0.1:1/p"`)) {
		t.Errorf("events lack the clean subject:\n%s", b)
	}
	for _, leak := range []string{secretPW, "token"} {
		if bytes.Contains(b, []byte(leak)) {
			t.Errorf("events contain %q:\n%s", leak, b)
		}
	}
}

// Value: protects=the url label buildTargets fills in (the event subject) and the startup warning when distinct URLs collapse onto one subject
// Value: fails_when=the label keeps the query or userinfo, an operator-set url label is rewritten, the shared-subject warning is missing, repeated, or fires for exact duplicates
// Value: why_new=the subject used to be the raw URL, so distinct URLs never shared one; seam=none
func TestBuildTargets_SanitizesURLLabelAndWarnsOnSharedSubject(t *testing.T) {
	cfg := config.Config{
		Interval: 30 * time.Second,
		Timeout:  10 * time.Second,
		Targets: []config.Target{
			{URL: "https://h/p?region=a"},
			{URL: "https://h/p?region=b"},
			{URL: "https://" + cred + "@h/p"}, // a third URL on the same subject: still one warning
			{URL: "https://other.example/x", Labels: map[string]string{"url": "billing-api"}},
			{URL: "https://dup.example/"},
			{URL: "https://dup.example/"},
		},
	}
	var buf bytes.Buffer
	targets := buildTargets(cfg, slog.New(slog.NewTextHandler(&buf, nil)))
	for i := range 3 {
		if got := targets[i].Labels["url"]; got != "https://h/p" {
			t.Errorf("target %d url label = %q, want https://h/p", i, got)
		}
		if targets[i].URL != cfg.Targets[i].URL {
			t.Errorf("target %d URL rewritten: %q (only the label is sanitized; the probe needs the real URL)", i, targets[i].URL)
		}
	}
	if got := targets[3].Labels["url"]; got != "billing-api" {
		t.Errorf("operator-set url label = %q, want it verbatim", got)
	}

	out := buf.String()
	if n := strings.Count(out, "share an event subject"); n != 1 {
		t.Fatalf("want exactly 1 shared-subject warning, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "subject=https://h/p") {
		t.Errorf("shared-subject warning does not name the subject:\n%s", out)
	}
	if n := strings.Count(out, "duplicate target URL"); n != 1 || !strings.Contains(out, "dup.example") {
		t.Errorf("exact duplicate must keep its own single warning, got %d:\n%s", n, out)
	}
	for _, leak := range []string{secretPW, "region"} {
		if strings.Contains(out, leak) {
			t.Errorf("log leaks %q:\n%s", leak, out)
		}
	}
}

// waitFor polls cond every 20ms until it holds, failing the test after 10s.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 10s")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
