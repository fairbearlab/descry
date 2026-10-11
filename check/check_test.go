package check

import "testing"

// TestRedactURL locks in the credential-masking guarantee relied on by every
// log line that mentions a target URL.
func TestRedactURL(t *testing.T) {
	pw := "secret" // joined into the URL at runtime: no literal basic-auth URL in source
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"user and password", "https://user:secret@example.com/path?q=1", "https://user:xxxxx@example.com/path?q=1"},
		{"user only (token-in-URL shape) is masked", "https://ghp_token@example.com/", "https://xxxxx@example.com/"},
		{"already-redacted user only is stable", "https://xxxxx@example.com/", "https://xxxxx@example.com/"},
		{"no userinfo", "https://example.com/health", "https://example.com/health"},
		{"empty", "", ""},
		{"unparseable yields the placeholder", "http://[::1", "<unparseable>"},
		{"unparseable with userinfo does not echo it", "https://user:" + pw + "@host/%zz", "<unparseable>"},
		{"placeholder is a fixed point", "<unparseable>", "<unparseable>"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RedactURL(c.in); got != c.want {
				t.Errorf("RedactURL(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
