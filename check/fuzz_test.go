package check

import (
	"net/url"
	"testing"
)

// RedactURL is the last thing standing between a target URL's credentials and
// the log. It is a thin wrapper over url.Parse + (*url.URL).Redacted(), and its
// documented failure mode — "on parse failure it returns the input unchanged" —
// is the interesting one: a string that url.Parse rejects is echoed verbatim,
// credentials included. This target pins the contract on both paths.
//
// Runs as an ordinary unit test in CI (seed corpus only). On-demand fuzzing:
// go test ./check -run '^$' -fuzz FuzzRedactURL -fuzztime 60s

func FuzzRedactURL(f *testing.F) {
	seeds := []string{
		"https://example.com",
		"https://user:hunter2@example.com/path?q=1",
		"https://user@example.com",
		"https://:hunter2@example.com",
		"http://user:p%40ss@example.com:8080/a/b#frag",
		// Password strings that also appear elsewhere in the URL — a naive
		// substring-based redaction would false-positive on these.
		"https://a:example.com@example.com/",
		"https://a:xxxxx@example.com/",
		// Inputs that url.Parse rejects, exercising the pass-through path.
		"https://user:pw@exa mple.com", "://", "http://[::1",
		"%%", "\x7f", "",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		got := RedactURL(raw)

		u, err := url.Parse(raw)
		if err != nil {
			// Documented contract: unparseable input is returned unchanged.
			// This is also where credentials survive into the log — the
			// behaviour is intentional, but it must stay exactly this
			// predictable, so assert it rather than leave it to chance.
			if got != raw {
				t.Fatalf("RedactURL(%q) = %q; unparseable input must be returned unchanged", raw, got)
			}
			return
		}

		if u.User == nil {
			return
		}
		pw, hasPW := u.User.Password()
		// url.Redacted() substitutes the literal "xxxxx"; a password that is
		// already "xxxxx" is indistinguishable from a successful redaction.
		if !hasPW || pw == "" || pw == "xxxxx" {
			return
		}

		// Check the redacted output's own userinfo rather than searching for the
		// password as a substring: a password may legitimately reappear in the
		// host or path, and that is not a leak.
		u2, err2 := url.Parse(got)
		if err2 != nil {
			t.Fatalf("RedactURL(%q) = %q, which no longer parses: %v", raw, got, err2)
		}
		if u2.User == nil {
			return
		}
		if pw2, ok := u2.User.Password(); ok && pw2 == pw {
			t.Fatalf("RedactURL(%q) leaked the password in %q", raw, got)
		}
	})
}

// FuzzRedactURLIdempotent pins a property the callers rely on implicitly:
// redacting an already-redacted URL is a no-op. Without it, a URL that passes
// through two logging paths could be mangled differently each time, and the
// second pass is exactly where an unnoticed re-parse difference would surface.
func FuzzRedactURLIdempotent(f *testing.F) {
	for _, s := range []string{
		"https://user:hunter2@example.com/path",
		"https://example.com",
		"https://a:xxxxx@example.com/",
		"http://[::1]:8080/x",
		"://",
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		once := RedactURL(raw)
		twice := RedactURL(once)
		if once != twice {
			t.Fatalf("RedactURL is not idempotent for %q: %q then %q", raw, once, twice)
		}
	})
}
