package check

import "testing"

// TestRedactURL locks in the credential-masking guarantee relied on by every
// log line that mentions a target URL.
func TestRedactURL(t *testing.T) {
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
		{"unparseable returned as-is", "http://[::1", "http://[::1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := RedactURL(c.in); got != c.want {
				t.Errorf("RedactURL(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
