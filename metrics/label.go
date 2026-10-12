package metrics

import (
	"net/url"
	"strings"
)

// unparseable is returned by TargetLabel for any URL it will not render. It is
// the same text check.RedactURL uses, so logs and labels agree on it.
const unparseable = "<unparseable>"

// TargetLabel returns the url label value for a target URL:
// scheme://host[:port]/path, rebuilt from url.Parse's scheme, host and escaped
// path. Userinfo, the query (including a bare trailing "?") and the fragment
// are always dropped, and an empty path renders as "/", so https://example.com
// and https://example.com/ share a label.
//
// It is stricter than check.RedactURL on purpose. RedactURL serves log lines
// and keeps the query and the username for debugging; a label is persisted by
// every scraper and served on an unauthenticated port, so nothing that can
// carry a secret (passwords, tokens in userinfo, signed-URL query parameters)
// may reach it. The path itself is kept as-is, so a URL that carries a secret
// in its path (a tokenised webhook or capability URL) still exposes it; such
// targets need Options.DisablePerTarget.
//
// A URL that fails to parse, is opaque (e.g. "https:user:pass@host"), or has
// no scheme or no host yields the fixed placeholder "<unparseable>", never the
// input or any function of it: even a hash of a URL that carries a weak
// password can be brute-forced offline. So does the rare URL whose rendering
// would not parse back to itself (net/url corner cases such as a non-ASCII
// IPv6 zone), which keeps TargetLabel idempotent. config.Load does not parse
// target URLs, so such targets still run (and fail), and they all share the
// one <unparseable> series set.
//
// A URL with any literal "@" other than the one url.Parse consumed as the
// userinfo delimiter also yields the placeholder. Without that rule a password
// with an unescaped "/", "?" or "#" is misparsed and part of it survives: the
// authority ends at the first of those, so https://user:12/abc@host/ has host
// "user:12" and path "/abc@host/", and the same URL with "?b" in place of
// "/abc" has host "user:12" with the "@" hidden in the dropped query. With it,
// credentials written in userinfo syntax never reach the label: either the only
// "@" is the delimiter and everything before it is dropped, or the URL is
// rejected. The cost is that a URL with a literal "@" in its path, query or
// fragment (/users/@me, or an email in a query parameter) or in an unescaped
// password also gets the placeholder; escape it as "%40" to keep a distinct
// label. A rendering that still contains an "@" (a "%40" in a path that net/url
// has to re-escape) gets the placeholder too.
//
// Targets that differ only in the dropped parts share one series set: their
// counters sum and their gauges are last-writer-wins. The same holds for every
// target that renders as the <unparseable> placeholder.
func TargetLabel(raw string) string {
	label, ok := render(raw)
	if !ok {
		return unparseable
	}
	if again, ok := render(label); !ok || again != label {
		return unparseable
	}
	return label
}

// render rebuilds raw from its scheme, host and path, reporting false when it
// has no safe rendering.
func render(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	if strings.Count(raw, "@") > delimiters(u) {
		return "", false
	}
	// Only the kept parts go into the rebuild; String re-escapes host and path.
	kept := url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path, RawPath: u.RawPath}
	if kept.Path == "" {
		kept.Path, kept.RawPath = "/", ""
	}
	label := kept.String()
	// An escaped "%40" can still come back as "@": when the raw path holds a
	// byte that must be escaped (a space, say), String re-escapes the decoded
	// path, and "@" is legal there.
	if strings.Contains(label, "@") {
		return "", false
	}
	return label, true
}

// delimiters is how many "@" url.Parse consumed from the input: one when it
// found userinfo, else none.
func delimiters(u *url.URL) int {
	if u.User != nil {
		return 1
	}
	return 0
}
