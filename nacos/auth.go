package nacos

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// AccessGetter supplies the credentials that go on *every* request.
//
// Per request, not once per connection: the server special-cases
// ServerCheckRequest and returns before any handler or filter runs
// (GrpcRequestAcceptor.java:106-115), so there is no connection-level identity
// to attach to. Validation happens in the filter chain in front of every
// handler (RequestHandler.handleRequest:45-56, RemoteRequestAuthFilter:110
// reading request.getHeaders()), and every official client injects per request
// (ClientWorker.java:1377, NamingGrpcClientProxy.java:512, LockGrpcClient.java:119).
//
// Implementing "per request" does not mean "log in per request": Access is
// expected to be cheap and cached, and Invalidate exists so a 403 can force a
// refresh. That is exactly what the Java client does with SecurityProxy.
type AccessGetter interface {
	// Access returns the headers to merge into Metadata.headers for one request.
	// An empty map means "this deployment needs no credentials" — the common case
	// with auth disabled, and a legitimate answer rather than an error.
	Access(ctx context.Context) (map[string]string, error)

	// Invalidate reports that the cached credential was rejected, so the next
	// Access must fetch a new one.
	Invalidate()
}

// StaticAccess is an AccessGetter for callers that already hold credentials —
// a long-lived token from a secret store, or username/password headers, both of
// which the default auth plugin accepts (NacosAuthPluginService.java:56-63 lists
// Authorization / accessToken / username / password as identity names).
type StaticAccess struct {
	Headers map[string]string
}

func (s StaticAccess) Access(context.Context) (map[string]string, error) { return s.Headers, nil }

// Invalidate is a no-op: a static credential has nothing to refresh.
func (s StaticAccess) Invalidate() {}

// PasswordAccess logs in over HTTP and caches the resulting token.
//
// The endpoint is the one the server still routes in 3.2.3 — verified against a
// live 3.2.3 instance, where both /nacos/v3/auth/user/login and
// /nacos/v1/auth/login answered (500 with auth disabled, not 404). v3 is the
// default here; Path overrides it.
//
// Response key spelling is accepted in both camelCase and snake_case. The
// constants in the Java client say accessToken (NacosAuthLoginConstant.java:26)
// and tokenTtl (api Constants.java:94), while the v3 controller builds its map
// through a different path; rather than guess at which release changed what,
// both spellings are read. The *request* header key is unambiguous and camelCase:
// the identity names the server looks for are Authorization / accessToken /
// username / password.
type PasswordAccess struct {
	// BaseURL is the server root including the context path, e.g.
	// "http://127.0.0.1:8848/nacos". No trailing slash.
	BaseURL string
	// Path defaults to "/v3/auth/user/login".
	Path     string
	Username string
	Password string
	// Client may be nil, in which case a client with a 5s timeout is used.
	Client *http.Client

	mu      sync.Mutex
	token   string
	until   time.Time
	refresh time.Duration
}

const defaultLoginPath = "/v3/auth/user/login"

// Access returns {"accessToken": <token>}, logging in when the cache is empty or
// inside the refresh window.
func (a *PasswordAccess) Access(ctx context.Context) (map[string]string, error) {
	if strings.TrimSpace(a.BaseURL) == "" {
		return nil, fmt.Errorf("nacos: PasswordAccess needs a BaseURL")
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.token != "" && time.Now().Before(a.until) {
		return map[string]string{"accessToken": a.token}, nil
	}

	path := a.Path
	if path == "" {
		path = defaultLoginPath
	}
	form := url.Values{"username": {a.Username}, "password": {a.Password}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(a.BaseURL, "/")+path,
		strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("nacos: build login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := a.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: login at %s: %v", ErrUnavailable, req.URL, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("nacos: read login response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: login at %s: status %d: %s", ErrUnavailable, req.URL, resp.StatusCode,
			strings.TrimSpace(string(body)))
	}
	var parsed struct {
		AccessToken  string `json:"accessToken"`
		AccessTokenU string `json:"access_token"`
		TokenTTL     int64  `json:"tokenTtl"`
		TokenTTLU    int64  `json:"token_ttl"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("nacos: decode login response %q: %w", truncate(string(body), 120), err)
	}
	token := parsed.AccessToken
	if token == "" {
		token = parsed.AccessTokenU
	}
	if token == "" {
		// A 200 with no token is what an unconfigured auth plugin answers; saying
		// "login failed" would send the operator to check their password.
		return nil, fmt.Errorf("nacos: login returned no access token (auth disabled or misconfigured?): %s",
			truncate(string(body), 120))
	}
	ttl := parsed.TokenTTL
	if ttl == 0 {
		ttl = parsed.TokenTTLU
	}
	a.token = token
	if ttl <= 0 {
		// No TTL advertised: cache briefly rather than forever.
		a.refresh = 30 * time.Second
	} else {
		// The Java client refreshes inside a window of tokenTtl/15 … tokenTtl/10;
		// a fixed tenth keeps the same shape without a random dependency.
		a.refresh = time.Duration(ttl) * time.Second / 10
	}
	a.until = time.Now().Add(time.Duration(ttl)*time.Second - a.refresh)
	if a.token == "" {
		return nil, fmt.Errorf("nacos: login produced an empty token")
	}
	return map[string]string{"accessToken": a.token}, nil
}

// Invalidate drops the cached token so the next Access logs in again — the
// response to a 403 mid-connection.
func (a *PasswordAccess) Invalidate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.token = ""
	a.until = time.Time{}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
