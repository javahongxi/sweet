package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const cacheSource = "DEFAULT_GROUP/sweet-gateway-routes.json"

// newCacheGateway builds a gateway with a cache file and *no* routes: the
// embedded Handle path pre-registers a route, which would make every count in
// these tests ambiguous.
func newCacheGateway(t *testing.T, b backend, cacheFile string) *Gateway {
	t.Helper()
	g, err := New(Options{
		Registry:         b.reg,
		DialOptions:      []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Services:         map[string]ServiceTypes{greetService: greeterTypes},
		RouteCacheFile:   cacheFile,
		RouteCacheSource: cacheSource,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
	})
	return g
}

func TestRouteCacheRoundTrip(t *testing.T) {
	b := newBackend(t, "greeter")
	path := filepath.Join(t.TempDir(), "routes.cache.json")

	g := newCacheGateway(t, b, path)
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no cache file after an accepted table: %v", err)
	}

	// A second gateway — the cold start with no live source — must serve from it.
	g2 := newCacheGateway(t, b, path)
	if n := len(g2.Routes()); n != 0 {
		t.Fatalf("fresh gateway has %d routes, want 0 before a restore", n)
	}
	applied, age, err := g2.RestoreRoutes()
	if err != nil || !applied {
		t.Fatalf("RestoreRoutes = (%v, %v, %v), want applied with no error", applied, age, err)
	}
	if age < 0 {
		t.Errorf("age = %v, want non-negative", age)
	}
	if rec := post(t, g2, `{"name":"cold"}`); rec.Code != http.StatusOK {
		t.Fatalf("restored gateway: status = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
}

func TestRouteCacheRoundTripPreservesHTTPBindings(t *testing.T) {
	b := newBackend(t, "greeter")
	path := filepath.Join(t.TempDir(), "routes.cache.json")
	spec := greetSpec("/v1/cache/{name}")
	spec.Method = http.MethodGet
	spec.Body = stringPointer("")
	spec.ResponseBody = "message"

	g := newCacheGateway(t, b, path)
	if err := g.ApplyRoutes([]RouteSpec{spec}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	g2 := newCacheGateway(t, b, path)
	applied, _, err := g2.RestoreRoutes()
	if err != nil || !applied {
		t.Fatalf("RestoreRoutes = (%v, %v)", applied, err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/cache/cold", nil)
	rec := httptest.NewRecorder()
	g2.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "\"Hello, cold!|\"" {
		t.Fatalf("restored binding: status=%d body=%s", rec.Code, rec.Body.String())
	}
	restored := g2.Routes()[0]
	if restored.Body == nil || *restored.Body != "" || restored.ResponseBody != "message" {
		t.Fatalf("bindings lost in cache: body=%v responseBody=%q", restored.Body, restored.ResponseBody)
	}
}

// TestRouteCacheNotWrittenOnFailedApply is the property that makes the cache
// trustworthy: a table the gateway refused must never reach disk, or one typo'd
// publish would outlive the restart it was typed during.
func TestRouteCacheNotWrittenOnFailedApply(t *testing.T) {
	b := newBackend(t, "greeter")
	path := filepath.Join(t.TempDir(), "routes.cache.json")
	g := newCacheGateway(t, b, path)

	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("good apply: %v", err)
	}
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}

	bad := []RouteSpec{
		greetSpec("/v1/kept"),
		{Method: "POST", Path: "/v1/bad", Service: "greeter", FullMethod: "/sweet.example.v1.Greeter/Nope"},
	}
	if err := g.ApplyRoutes(bad); err == nil {
		t.Fatal("bad table should have been refused")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("re-read cache: %v", err)
	}
	if string(after) != string(good) {
		t.Errorf("cache changed after a refused table:\n before=%s\n after=%s", good, after)
	}
}

func TestRouteCacheRefusesMismatchedSource(t *testing.T) {
	b := newBackend(t, "greeter")
	path := filepath.Join(t.TempDir(), "routes.cache.json")
	writeRawCache(t, path, routeCacheRecord{
		SavedAt: time.Now(), Source: "OTHER_GROUP/some-other-dataId.json",
		Routes: []RouteSpec{greetSpec(greetPath)},
	})

	g := newCacheGateway(t, b, path)
	applied, _, err := g.RestoreRoutes()
	if err == nil || applied {
		t.Fatalf("RestoreRoutes = (%v, %v), want a refusal for a cache from another source", applied, err)
	}
	if !strings.Contains(err.Error(), "some-other-dataId.json") {
		t.Errorf("error should name what the file actually holds; got %v", err)
	}
	if n := len(g.Routes()); n != 0 {
		t.Errorf("%d routes installed after a refusal, want 0", n)
	}
}

func TestRouteCacheRefusesUndatedAndEmpty(t *testing.T) {
	b := newBackend(t, "greeter")
	cases := map[string]routeCacheRecord{
		"undated": {Source: cacheSource, Routes: []RouteSpec{greetSpec(greetPath)}},
		"empty":   {SavedAt: time.Now(), Source: cacheSource},
	}
	for name, rec := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routes.cache.json")
			writeRawCache(t, path, rec)
			g := newCacheGateway(t, b, path)
			applied, _, err := g.RestoreRoutes()
			if err == nil || applied {
				t.Fatalf("RestoreRoutes = (%v, %v), want a refusal for an %s cache", applied, err, name)
			}
		})
	}
}

func TestRouteCacheRefusesTableThatNoLongerResolves(t *testing.T) {
	// Types are compiled in; a cache can outlive the build that understood it.
	b := newBackend(t, "greeter")
	path := filepath.Join(t.TempDir(), "routes.cache.json")
	writeRawCache(t, path, routeCacheRecord{
		SavedAt: time.Now(), Source: cacheSource,
		Routes: []RouteSpec{{Method: "POST", Path: "/v1/x", Service: "greeter",
			FullMethod: "/gone.example.v1.Service/Gone"}},
	})
	g := newCacheGateway(t, b, path)
	applied, _, err := g.RestoreRoutes()
	if err == nil || applied {
		t.Fatalf("RestoreRoutes = (%v, %v), want a refusal naming the unresolvable method", applied, err)
	}
	if !strings.Contains(err.Error(), "gone.example.v1.Service") {
		t.Errorf("error should name the service that cannot be resolved; got %v", err)
	}
}

func TestRouteCacheDisabledByDefault(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newCacheGateway(t, b, "") // no cache file configured
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	applied, age, err := g.RestoreRoutes()
	if applied || err != nil || age != 0 {
		t.Fatalf("RestoreRoutes = (%v, %v, %v), want a no-op when no cache is configured", applied, age, err)
	}
}

func TestRestoreRoutesWithoutFileIsNotAnError(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newCacheGateway(t, b, filepath.Join(t.TempDir(), "absent.json"))
	applied, age, err := g.RestoreRoutes()
	if applied || err != nil || age != 0 {
		t.Fatalf("RestoreRoutes = (%v, %v, %v), want (false, 0, nil) when nothing was cached yet", applied, age, err)
	}
}

func writeRawCache(t *testing.T, path string, rec routeCacheRecord) {
	t.Helper()
	body, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}
