package http

import (
	"context"
	nethttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/fairbearlab/descry/check"
)

func TestRun_200Up(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()
	obs, _ := newForTest(2 * time.Second).Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusUp {
		t.Fatalf("status = %v, want up", obs.Status)
	}
}

func TestRun_500HTTPError(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()
	obs, _ := newForTest(2 * time.Second).Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusDown || obs.ErrorClass != check.ErrHTTPError {
		t.Fatalf("got %v/%v, want down/http_error", obs.Status, obs.ErrorClass)
	}
}

func TestRun_BodyCappedTo4096(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(strings.Repeat("x", 8000)))
	}))
	defer srv.Close()
	obs, _ := newForTest(2 * time.Second).Run(context.Background(), check.Target{URL: srv.URL})
	if got := obs.Extra["body"].(string); len(got) != 4096 {
		t.Fatalf("body len = %d, want 4096", len(got))
	}
}

func TestRun_Timeout(t *testing.T) {
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		// Block until client disconnects
		<-r.Context().Done()
	}))
	defer srv.Close()
	// Use a very short timeout so the test runs quickly
	obs, _ := newForTest(50 * time.Millisecond).Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusDown {
		t.Fatalf("status = %v, want down", obs.Status)
	}
	if obs.ErrorClass != check.ErrTimeout {
		t.Fatalf("error_class = %v, want timeout", obs.ErrorClass)
	}
}

func TestRun_FinalURLAfterRedirect(t *testing.T) {
	var srvURL string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		if r.URL.Path == "/start" {
			nethttp.Redirect(w, r, srvURL+"/end", nethttp.StatusFound)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	srvURL = srv.URL

	obs, _ := newForTest(2 * time.Second).Run(context.Background(), check.Target{URL: srv.URL + "/start"})
	if obs.Status != check.StatusUp {
		t.Fatalf("status = %v, want up", obs.Status)
	}
	if !strings.HasSuffix(obs.FinalURL, "/end") {
		t.Fatalf("final_url = %q, want suffix /end", obs.FinalURL)
	}
}

func TestRun_RedirectLoop(t *testing.T) {
	var srvURL string
	srv := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		nethttp.Redirect(w, r, srvURL+"/loop", nethttp.StatusFound)
	}))
	defer srv.Close()
	srvURL = srv.URL

	obs, _ := newForTest(2 * time.Second).Run(context.Background(), check.Target{URL: srv.URL + "/loop"})
	if obs.Status != check.StatusDown {
		t.Fatalf("status = %v, want down", obs.Status)
	}
	if obs.ErrorClass != check.ErrHTTPError {
		t.Fatalf("error_class = %v, want http_error", obs.ErrorClass)
	}
}

func TestRun_TLSExpiryCaptured(t *testing.T) {
	srv := httptest.NewTLSServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		w.WriteHeader(200)
	}))
	defer srv.Close()

	// Use the TLS server's client which trusts the test cert, with skipSSRF
	c := newForTest(2 * time.Second)
	c.client.Transport = srv.Client().Transport

	obs, _ := c.Run(context.Background(), check.Target{URL: srv.URL})
	if obs.Status != check.StatusUp {
		t.Fatalf("status = %v, want up", obs.Status)
	}
	if obs.TLSExpiry == nil {
		t.Fatalf("tls_expiry is nil, want non-nil")
	}
}
