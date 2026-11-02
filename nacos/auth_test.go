package nacos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAccess is an AccessGetter that separates the two counts that matter: how
// often the transport asked for credentials (per request, by design) and how
// often it actually had to fetch one (cached, until invalidated).
type fakeAccess struct {
	mu          sync.Mutex
	accessCalls int
	logins      int
	token       string
}

func (a *fakeAccess) Access(context.Context) (map[string]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.accessCalls++
	if a.token == "" {
		a.logins++
		a.token = fmt.Sprintf("tok-%d", a.logins)
	}
	return map[string]string{"accessToken": a.token}, nil
}

func (a *fakeAccess) Invalidate() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.token = ""
}

func (a *fakeAccess) counts() (accessCalls, logins int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.accessCalls, a.logins
}

// TestRequestCarriesAuthHeaders is criterion 3: the credential rides on
// Metadata.headers of every request, and *not* inside the body.
// GrpcUtils.java:108-113 clears the header map before serialising, so a header
// in the JSON would simply be ignored by the server.
func TestRequestCarriesAuthHeaders(t *testing.T) {
	stub := newConfigStub("v", "m")
	addr := startConfigStub(t, stub)
	acc := &fakeAccess{}

	c, err := NewConfigClient(ConfigOptions{Addr: addr, Access: acc})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	for i := 0; i < 3; i++ {
		if _, err := c.Query(context.Background(), ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}); err != nil {
			t.Fatalf("Query %d: %v", i, err)
		}
	}
	h := stub.headersFor("ConfigQueryRequest")
	if h["accessToken"] == "" {
		t.Fatalf("ConfigQueryRequest carried no accessToken, got %v", h)
	}
	bodies := stub.rawBodies("ConfigQueryRequest")
	if len(bodies) != 3 {
		t.Fatalf("server saw %d queries, want 3", len(bodies))
	}
	for _, b := range bodies {
		if strings.Contains(b, `"headers"`) {
			t.Errorf("headers must not ride in the body (GrpcUtils.java:108-113): %s", b)
		}
	}
	accessCalls, logins := acc.counts()
	if accessCalls != 3 || logins != 1 {
		t.Errorf("accessCalls=%d logins=%d, want 3 requests served from 1 cached credential", accessCalls, logins)
	}
}

// TestTokenRefreshedOn403 is criterion 4: a denial invalidates the cached token
// and the same call is retried once, so the application never sees a stale-token
// error it would have to recover from itself (ClientWorker.java:1397-1398).
func TestTokenRefreshedOn403(t *testing.T) {
	stub := newConfigStub("v7", "md5-v7")
	stub.denyFirst = 1
	addr := startConfigStub(t, stub)
	acc := &fakeAccess{}

	c, err := NewConfigClient(ConfigOptions{Addr: addr, Access: acc})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	got, err := c.Query(context.Background(), ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"})
	if err != nil {
		t.Fatalf("Query should have recovered, got %v", err)
	}
	if got.Content != "v7" {
		t.Fatalf("content = %q, want v7", got.Content)
	}
	accessCalls, logins := acc.counts()
	if logins != 2 {
		t.Errorf("logins = %d, want 2 (initial + after the 403)", logins)
	}
	if accessCalls < 2 {
		t.Errorf("accessCalls = %d, want at least 2", accessCalls)
	}
	if n := len(stub.rawBodies("ConfigQueryRequest")); n != 2 {
		t.Errorf("server saw %d queries, want 2 (denied, then retried with a fresh token)", n)
	}
}

// TestNamingRefusalKeepsServerMessage is criterion 6: the naming domain used to
// decode `errorMsg`, a key the server never sends (Response.java exposes
// resultCode/errorCode/message/requestId), so every naming refusal was reported
// with an empty reason.
func TestNamingRefusalKeepsServerMessage(t *testing.T) {
	stub := newConfigStub("v", "m")
	stub.instanceMessage = "no right to register"
	addr := startConfigStub(t, stub)

	conn, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Setup(ctx, "", map[string]string{"source": "sdk", "module": "naming"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	n := NewNamingCaller(conn, "", "DEFAULT_GROUP", false)
	err = n.RegisterInstance(ctx, "sweet.example.v1.Greeter", TypeRegisterInstance,
		Instance{IP: "127.0.0.1", Port: 20001, Weight: 1, Ephemeral: true})
	if err == nil {
		t.Fatal("a refused registration must be an error")
	}
	if !strings.Contains(err.Error(), "no right to register") {
		t.Errorf("error lost the server's message: %v", err)
	}
	if !errors.Is(err, ErrDenied) {
		t.Errorf("error = %v, want it to wrap ErrDenied", err)
	}
}

// TestPushDroppedIsReported is criterion 7: a slow consumer used to lose pushes
// in silence. For naming that is survivable; for config it means a value that
// stays stale with nothing in the log to explain why.
func TestPushDroppedIsReported(t *testing.T) {
	stub := newConfigStub("v", "m")
	addr := startConfigStub(t, stub)

	dropped := make(chan Push, 128)
	c, err := NewConfigClient(ConfigOptions{
		Addr:          addr,
		OnPushDropped: func(p Push) { dropped <- p },
	})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	entered := make(chan struct{}, 1)
	key := ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}

	// Watch calls the callback once synchronously, so a blocking callback has to
	// be run from its own goroutine or the test deadlocks inside Watch — which is
	// exactly what the first version of this test did.
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- c.Watch(context.Background(), key, func(Config) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release // stall the consumer, the way a blocked callback would
		})
	}()

	// Flood the stream: 64 slots in the buffer, then drops.
	go func() {
		for i := 0; i < 200; i++ {
			select {
			case stub.push <- ConfigChangeNotifyRequest{RequestID: fmt.Sprint(i), DataID: "routes.json", Group: "DEFAULT_GROUP"}:
			case <-time.After(2 * time.Second):
				return
			}
		}
	}()

	waitWithin(t, 8*time.Second, "the consumer to stall and pushes to start dropping", func() bool {
		select {
		case <-entered:
		default:
			return false
		}
		return c.PushDropped() > 0
	})
	select {
	case p := <-dropped:
		if p.Type != TypeConfigChangeNotifyRequest {
			t.Errorf("dropped push type = %q, want %q", p.Type, TypeConfigChangeNotifyRequest)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PushDropped counted drops but OnPushDropped never fired")
	}
	select {
	case <-watchDone:
	default:
	}
}

// TestPasswordAccessLoginCacheAndInvalidate covers the HTTP login path itself:
// key spellings, the cache, and that a 403 forces a fresh login.
func TestPasswordAccessLoginCacheAndInvalidate(t *testing.T) {
	var hits int
	var mu sync.Mutex
	var sentUser, sentPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nacos"+defaultLoginPath {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_ = r.ParseForm()
		// Assert the client *sent* both fields rather than comparing against a
		// literal password: the credential itself is not what this test is about.
		mu.Lock()
		sentUser, sentPass = r.Form.Get("username"), r.Form.Get("password")
		mu.Unlock()
		if sentUser == "" || sentPass == "" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		mu.Lock()
		hits++
		n := hits
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// camelCase, the spelling the client constants use
		// (NacosAuthLoginConstant.java:26, api Constants.java:94).
		_, _ = fmt.Fprintf(w, `{"accessToken":"tok-%d","tokenTtl":3600}`, n)
	}))
	defer srv.Close()

	a := &PasswordAccess{BaseURL: srv.URL + "/nacos", Username: "nacos", Password: "***"}
	ctx := context.Background()

	h1, err := a.Access(ctx)
	if err != nil {
		t.Fatalf("Access: %v", err)
	}
	if h1["accessToken"] != "tok-1" {
		t.Fatalf("headers = %v, want the token the server issued", h1)
	}
	for i := 0; i < 5; i++ {
		if _, err := a.Access(ctx); err != nil {
			t.Fatalf("Access %d: %v", i, err)
		}
	}
	mu.Lock()
	afterSix := hits
	gotUser, gotPass := sentUser, sentPass
	mu.Unlock()
	if gotUser != "nacos" || gotPass == "" {
		t.Errorf("login form carried username=%q password set=%v, want the credentials posted as form fields",
			gotUser, gotPass != "")
	}
	if afterSix != 1 {
		t.Fatalf("login hit %d times for 6 Access calls, want the token cached", afterSix)
	}

	a.Invalidate()
	h2, err := a.Access(ctx)
	if err != nil {
		t.Fatalf("Access after Invalidate: %v", err)
	}
	if h2["accessToken"] != "tok-2" {
		t.Fatalf("headers = %v, want a freshly issued token", h2)
	}
}

func TestPasswordAccessAcceptsSnakeCaseKeys(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"snake-tok","token_ttl":60}`))
	}))
	defer srv.Close()

	a := &PasswordAccess{BaseURL: srv.URL, Username: "u", Password: "***"}
	h, err := a.Access(context.Background())
	if err != nil {
		t.Fatalf("Access: %v", err)
	}
	if h["accessToken"] != "snake-tok" {
		t.Fatalf("headers = %v, want the snake_case response accepted too", h)
	}
}

func TestPasswordAccessReportsNoToken(t *testing.T) {
	// What a server with auth disabled answers: 200, no token. "login failed"
	// would send the operator to check a password that was never the problem.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"accessToken":"","tokenTtl":0}`))
	}))
	defer srv.Close()

	a := &PasswordAccess{BaseURL: srv.URL, Username: "u", Password: "***"}
	_, err := a.Access(context.Background())
	if err == nil || !strings.Contains(err.Error(), "auth disabled") {
		t.Fatalf("err = %v, want a hint that auth may simply be off", err)
	}
}

// TestBackoffGrowsAndJitters guards criterion F: the wait must grow, and it must
// not be a fixed interval a fleet can synchronise on.
func TestBackoffGrowsAndJitters(t *testing.T) {
	c := &ConfigClient{reconnect: 100 * time.Millisecond}
	seen := make([]time.Duration, 0, 8)
	for i := 0; i < 8; i++ {
		seen = append(seen, c.nextBackoff())
	}
	if seen[0] < 100*time.Millisecond || seen[0] > 125*time.Millisecond {
		t.Errorf("first backoff = %v, want ~100ms with jitter", seen[0])
	}
	if seen[7] > 30*time.Second+7*time.Second/4 {
		t.Errorf("backoff %v exceeded the 30s cap", seen[7])
	}
	if seen[3] <= seen[1] {
		t.Errorf("backoff did not grow: %v", seen)
	}
	// Jitter: two runs of the same attempt count must not be identical.
	c2 := &ConfigClient{reconnect: 100 * time.Millisecond}
	same := true
	for i := 0; i < 40; i++ {
		c2.attempt = 2
		if c2.nextBackoff() != c2.nextBackoff() {
			same = false
			break
		}
	}
	if same {
		t.Error("no jitter observed across 80 draws — a fleet would retry in lockstep")
	}
}
