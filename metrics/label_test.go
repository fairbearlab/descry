package metrics

import (
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestTargetLabel locks in that a url label never carries userinfo, a query
// or a fragment, and that anything it cannot render safely becomes the
// placeholder rather than an echo of the input.
func TestTargetLabel(t *testing.T) {
	pw := "secret" // joined into URLs at runtime: no literal basic-auth URL in source
	at := "@"      // likewise for the rows whose password url.Parse misreads
	cases := []struct {
		name   string
		in     string
		want   string
		absent []string // substrings of the input that must not reach the label
	}{
		{"credentials", "https://user:" + pw + "@host/p", "https://host/p", []string{"user", "secret"}},
		{"username only", "https://user@host/p", "https://host/p", []string{"user"}},
		{"signed URL query token", "https://bucket.example.com/obj?X-Amz-Signature=abc123&token=xyz", "https://bucket.example.com/obj", []string{"Signature", "abc123", "token", "xyz"}},
		{"fragment", "https://host/p#frag", "https://host/p", []string{"frag"}},
		{"force query", "https://h/p?", "https://h/p", []string{"?"}},
		{"empty path is slash", "https://example.com", "https://example.com/", nil},
		{"slash path", "https://example.com/", "https://example.com/", nil},
		{"port kept", "http://host:8080/health", "http://host:8080/health", nil},
		{"ipv6 host kept", "http://[::1]:9090/metrics", "http://[::1]:9090/metrics", nil},
		{"escaped path kept", "https://host/a%20b/c%2Fd", "https://host/a%20b/c%2Fd", nil},
		{"password with unescaped slash spills into host and path", "https://user:12/abc" + at + "host/", unparseable, []string{"user", "12", "abc", "host"}},
		{"password tail with slash spills past parsed userinfo", "https://a" + at + "b:12/c" + at + "host/", unparseable, []string{"12", "host"}},
		{"literal @ in a legitimate path", "https://host/users/@me", unparseable, []string{"host", "users", "me"}},
		{"numeric password with unescaped ? hides @ in the query", "https://user:12?b" + at + "host/", unparseable, []string{"user", "12", "host"}},
		{"numeric password with unescaped # hides @ in the fragment", "https://user:12#b" + at + "host/", unparseable, []string{"user", "12", "host"}},
		{"numeric password with slash and ? hides @ in the query", "https://user:12/a?b" + at + "host/", unparseable, []string{"user", "12", "host"}},
		{"password tail with ? past parsed userinfo", "https://a" + at + "b:12?c" + at + "host/", unparseable, []string{"12", "host"}},
		{"literal @ in a query parameter", "https://host/p?email=a" + at + "b.example", unparseable, []string{"host", "email", "example"}},
		{"escaped %40 in a query parameter", "https://host/p?email=a%40b.example", "https://host/p", []string{"email", "example"}},
		{"escaped %40 in path kept", "https://host/users/%40me", "https://host/users/%40me", nil},
		{"escaped %40 in a path net/url re-escapes", "https://host/%40 x", unparseable, []string{"host", "x"}},
		{"password with unescaped ? fails to parse", "https://user:p?w" + at + "host/", unparseable, []string{"user", "p?w", "host"}},
		{"password with unescaped # fails to parse", "https://user:p#w" + at + "host/", unparseable, []string{"user", "p#w", "host"}},
		{"username with unescaped ? hides @ in the query", "https://user?x" + at + "host/", unparseable, []string{"user", "x", "host"}},
		{"username with unescaped # hides @ in the fragment", "https://user#x" + at + "host/", unparseable, []string{"user", "x", "host"}},
		{"scheme lowercased by url.Parse", "HTTPS://Host/p", "https://Host/p", nil},
		{"malformed escape with credentials", "https://user:" + pw + "@host/%zz", unparseable, []string{"user", "secret"}},
		{"malformed scheme", "://bad", unparseable, []string{"bad"}},
		{"opaque form with credentials", "https:user:" + pw + "@host", unparseable, []string{"user", "secret"}},
		{"no host", "https:///p", unparseable, nil},
		{"file URL has no host", "file:///etc/passwd", unparseable, []string{"passwd"}},
		{"no scheme", "//host/p", unparseable, []string{"host"}},
		{"bare host and path", "host/p", unparseable, []string{"host"}},
		{"rendering that does not parse back (non-ASCII IPv6 zone)", "http://[::%25\xef]/", unparseable, nil},
		{"empty", "", unparseable, nil},
		{"placeholder is a fixed point", unparseable, unparseable, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := TargetLabel(c.in)
			if got != c.want {
				t.Errorf("TargetLabel(%q) = %q, want %q", c.in, got, c.want)
			}
			for _, s := range c.absent {
				if strings.Contains(got, s) {
					t.Errorf("TargetLabel(%q) = %q contains %q", c.in, got, s)
				}
			}
		})
	}
}

// TestTargetLabel_OpaqueParse proves the opaque row above is the case it
// claims: url.Parse accepts it and puts the credential in Opaque, which a
// scheme://host rebuild would otherwise have to handle.
func TestTargetLabel_OpaqueParse(t *testing.T) {
	u, err := url.Parse("https:user:" + "secret" + "@host")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	if u.Opaque == "" || u.Host != "" {
		t.Fatalf("Opaque = %q, Host = %q; want an opaque URL with no host", u.Opaque, u.Host)
	}
}

// FuzzTargetLabel checks the label's safety properties on arbitrary input:
// valid UTF-8 (WithLabelValues panics otherwise), no query, fragment or "@",
// the placeholder for any input with an "@" besides the userinfo delimiter,
// no userinfo, idempotence, a label independent of the input's userinfo, and
// credentials of a parsed URL never surviving unless the kept scheme, host or
// path carries the same text.
func FuzzTargetLabel(f *testing.F) {
	at := "@" // joined at runtime, as in TestTargetLabel: no literal userinfo URL in source
	for _, s := range []string{
		"https://example.com",
		"https://user:pw" + at + "host:8443/a/b?token=t#frag",
		"https://tok" + at + "api.example.com/",
		"http://[fe80::1%25en0]:80/x",
		"https://h/p?",
		"https://host/a%2Fb%20c",
		"https://user:12/abc" + at + "host/",
		"https://a" + at + "b:12/c" + at + "host/",
		"https://host/users/%40me",
		"https://user?x" + at + "host/",
		"https://user:12?b" + at + "host/",
		"https://host/p?email=a%40b.example",
		"A://" + at + "0#%40 ",
		"http://a%25b/",
		"https:user:pw" + at + "host",
		"://bad",
		"//host/p",
		unparseable,
		"",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		got := TargetLabel(raw)
		if !utf8.ValidString(got) {
			t.Fatalf("TargetLabel(%q) = %q is not valid UTF-8", raw, got)
		}
		if strings.ContainsAny(got, "?#@") {
			t.Fatalf("TargetLabel(%q) = %q contains ?, # or @", raw, got)
		}
		if again := TargetLabel(got); again != got {
			t.Fatalf("not idempotent: TargetLabel(%q) = %q, then %q", raw, got, again)
		}
		if u, err := url.Parse(raw); err == nil && strings.Count(raw, "@") > delimiters(u) && got != unparseable {
			t.Fatalf("TargetLabel(%q) = %q, want the placeholder for an @ beyond the userinfo delimiter", raw, got)
		}
		if got == unparseable {
			return
		}
		lu, err := url.Parse(got)
		if err != nil {
			t.Fatalf("label %q does not parse: %v", got, err)
		}
		if lu.User != nil || lu.RawQuery != "" || lu.ForceQuery || lu.Fragment != "" {
			t.Fatalf("label %q kept userinfo, query or fragment", got)
		}
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("TargetLabel(%q) = %q, but the input does not parse: %v", raw, got, err)
		}
		if u.User == nil {
			return
		}
		// The label must not depend on the userinfo at all...
		// The dropped query and fragment are cleared too: String re-escapes
		// them and can turn an escaped "%40" there into a literal "@".
		stripped := *u
		stripped.User = nil
		stripped.RawQuery, stripped.ForceQuery, stripped.Fragment, stripped.RawFragment = "", false, "", ""
		if want := TargetLabel(stripped.String()); got != want {
			t.Fatalf("TargetLabel(%q) = %q, but %q without its userinfo gives %q", raw, got, raw, want)
		}
		// ...so a credential can appear only where the kept parts carry the
		// same text (e.g. "A" inside the escaped host "%AC", or ":" in "://").
		escHost := (&url.URL{Host: u.Host}).String()
		kept := u.Scheme + "://" + " " + u.Host + " " + escHost + " " + u.Path + " " + u.EscapedPath()
		pw, _ := u.User.Password()
		for _, cred := range []string{u.User.Username(), pw} {
			if cred != "" && strings.Contains(got, cred) && !strings.Contains(kept, cred) {
				t.Fatalf("TargetLabel(%q) = %q leaks userinfo %q", raw, got, cred)
			}
		}
	})
}
