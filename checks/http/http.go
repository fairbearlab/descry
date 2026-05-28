package http

import (
	"context"
	"errors"
	"io"
	nethttp "net/http"
	"strings"
	"time"

	"github.com/fairbearlab/descry/check"
)

const (
	defaultTimeout = 10 * time.Second
	maxRedirects   = 5
	maxBodyBytes   = 4096
)

// Check is an HTTP uptime check that implements check.Check.
type Check struct {
	client   *nethttp.Client
	timeout  time.Duration
	skipSSRF bool // disable both SSRF guard layers (tests only)
}

// New creates a new HTTP Check with the given timeout. If timeout <= 0,
// defaultTimeout (10s) is used.
func New(timeout time.Duration) *Check {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c := &Check{timeout: timeout}
	c.client = &nethttp.Client{
		Transport: newSafeTransport(),
		CheckRedirect: func(req *nethttp.Request, via []*nethttp.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("too many redirects")
			}
			return nil
		},
	}
	return c
}

// newForTest creates a Check that uses a plain (non-SSRF-guarded) transport,
// for use in package-internal tests against httptest servers.
func newForTest(timeout time.Duration) *Check {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	c := &Check{timeout: timeout, skipSSRF: true}
	c.client = &nethttp.Client{
		Transport: &nethttp.Transport{},
		CheckRedirect: func(req *nethttp.Request, via []*nethttp.Request) error {
			if len(via) >= maxRedirects {
				return errors.New("too many redirects")
			}
			return nil
		},
	}
	return c
}

func (c *Check) Name() string { return "http" }

func (c *Check) Run(ctx context.Context, t check.Target) (check.Observation, error) {
	obs := check.Observation{
		ObservedAt: time.Now().UTC(),
		Labels:     t.Labels,
		Extra:      map[string]any{},
	}

	// Layer 1 guard (skipped in test mode)
	if !c.skipSSRF {
		if err := assertSafeURL(t.URL); err != nil {
			obs.Status = check.StatusDown
			obs.ErrorClass = check.ErrSSRFBlocked
			obs.ObservedAt = time.Now().UTC()
			return obs, nil // best-effort produce: report, don't error the run
		}
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodGet, t.URL, nil)
	if err != nil {
		obs.Status = check.StatusDown
		obs.ErrorClass = check.ErrUnknown
		return obs, nil
	}

	start := time.Now()
	resp, err := c.client.Do(req)
	obs.LatencyMs = int(time.Since(start).Milliseconds())
	obs.ObservedAt = time.Now().UTC()

	if err != nil {
		obs.Status = check.StatusDown
		obs.ErrorClass = classifyError(err)
		return obs, nil
	}
	defer resp.Body.Close()

	obs.StatusCode = resp.StatusCode
	obs.FinalURL = resp.Request.URL.String()

	// TLS expiry from connection state
	if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
		exp := resp.TLS.PeerCertificates[0].NotAfter
		obs.TLSExpiry = &exp
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		obs.Status = check.StatusUp
	} else {
		obs.Status = check.StatusDown
		obs.ErrorClass = check.ErrHTTPError
	}

	// capture body only on down, capped at maxBodyBytes. ToValidUTF8 drops any
	// partial multibyte rune left dangling at the byte boundary.
	if obs.Status == check.StatusDown {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
		obs.Extra["body"] = strings.ToValidUTF8(string(body), "")
	}
	return obs, nil
}

// classifyError maps a transport error to the engine's closed ErrorClass enum.
func classifyError(err error) check.ErrorClass {
	if errors.Is(err, ErrSSRFBlocked) {
		return check.ErrSSRFBlocked
	}
	msg := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, context.DeadlineExceeded), strings.Contains(msg, "timeout"):
		return check.ErrTimeout
	case strings.Contains(msg, "too many redirects"):
		return check.ErrHTTPError
	case strings.Contains(msg, "connection refused"), strings.Contains(msg, "connection reset"):
		return check.ErrConnectionRefused
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "server misbehaving"):
		return check.ErrDNSFailure
	case strings.Contains(msg, "certificate"), strings.Contains(msg, "tls"), strings.Contains(msg, "x509"):
		return check.ErrTLSError
	default:
		return check.ErrUnknown
	}
}
