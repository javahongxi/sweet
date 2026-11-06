package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUnaryResponseMetadataAllowlistAndTrailers(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, func(opts *Options) {
		opts.ForwardResponseHeaders = []string{"X-Backend", "X-Backend-Bin"}
		opts.ForwardResponseTrailers = []string{"X-End"}
	})
	ts := httptest.NewServer(g)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+greetPath, strings.NewReader(`{"name":"meta"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("TE", "trailers")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("X-Backend"); got != "unary" {
		t.Errorf("X-Backend = %q", got)
	}
	if got := resp.Header.Get("X-Backend-Bin"); got != "AAEC" {
		t.Errorf("binary metadata = %q, want base64 AAEC", got)
	}
	if got := resp.Header.Get("X-Secret"); got != "" {
		t.Errorf("unlisted metadata leaked: %q", got)
	}
	if got := resp.Trailer.Get("Grpc-Trailer-X-End"); got != "unary-done" {
		t.Errorf("response trailer = %q, headers = %#v", got, resp.Trailer)
	}
}

func TestResponseMetadataDefaultsClosed(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)
	rec := post(t, g, `{"name":"meta"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("X-Backend"); got != "" {
		t.Errorf("metadata forwarded without an allowlist: %q", got)
	}
}

func TestStreamResponseMetadataAndResultEnvelope(t *testing.T) {
	b := newBackend(t, greetService)
	g := newStreamingGateway(t, b, func(opts *Options) {
		opts.ForwardResponseHeaders = []string{"X-Backend"}
		opts.ForwardResponseTrailers = []string{"X-End"}
	})
	ts := httptest.NewServer(g)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+greetStreamPath, strings.NewReader(`{"name":"meta","times":1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("TE", "trailers")
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Header.Get("X-Backend"); got != "stream" {
		t.Errorf("X-Backend = %q", got)
	}
	if got := resp.Header.Get("X-Secret"); got != "" {
		t.Errorf("unlisted stream metadata leaked: %q", got)
	}
	if got := resp.Trailer.Get("Grpc-Trailer-X-End"); got != "stream-done" {
		t.Errorf("stream trailer = %q", got)
	}
	if !strings.Contains(string(body), `"result"`) || !strings.Contains(string(body), `"message"`) {
		t.Errorf("stream body = %s, want a result envelope", body)
	}
}
