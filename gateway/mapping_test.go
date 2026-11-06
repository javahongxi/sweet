package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
)

func stringPointer(value string) *string { return &value }

func TestCompiledDescriptorBindsPathQueryAndResponseBody(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, nil) // no Options.Services: the compiled descriptor is used
	spec := greetSpec("/v1/greet/{name}")
	spec.Method = http.MethodGet
	spec.Body = stringPointer("")
	spec.ResponseBody = "message"
	if err := g.ApplyRoutes([]RouteSpec{spec}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	routes := g.Routes()
	if len(routes) != 1 {
		t.Fatalf("Routes = %d, want 1", len(routes))
	}
	if _, ok := routes[0].NewRequest().(*pb.GreetRequest); !ok {
		t.Fatalf("NewRequest = %T, want the generated Go request", routes[0].NewRequest())
	}

	// The query alias addresses a path-bound field and must not overwrite it.
	req := httptest.NewRequest(http.MethodGet, "/v1/greet/from-path?name=from-query", nil)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var message string
	if err := json.Unmarshal(rec.Body.Bytes(), &message); err != nil {
		t.Fatalf("responseBody is not a JSON string: %s: %v", rec.Body.String(), err)
	}
	if !strings.Contains(message, "from-path") || strings.Contains(message, "from-query") {
		t.Errorf("message = %q, want only the path value", message)
	}
}

func TestCompiledDescriptorInfersServerStream(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, nil)
	spec := RouteSpec{
		Method: http.MethodGet, Path: "/v1/stream/{name}", Service: "greeter",
		FullMethod: greetStreamMeth, Body: stringPointer(""), ResponseBody: "message",
		// ServerStream deliberately omitted.
	}
	if err := g.ApplyRoutes([]RouteSpec{spec}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	if !g.Routes()[0].ServerStream {
		t.Fatal("descriptor did not mark GreetStream as server-streaming")
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/stream/web?times=2", nil)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	frames := lines(rec.Body.String())
	if len(frames) != 2 {
		t.Fatalf("frames = %#v, want 2", frames)
	}
	for i, frame := range frames {
		var envelope struct {
			Result string `json:"result"`
		}
		if err := json.Unmarshal([]byte(frame), &envelope); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if !strings.Contains(envelope.Result, "Hello #") {
			t.Errorf("frame %d result = %q", i, envelope.Result)
		}
	}
}

func TestBodyFieldAndQueryBinding(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, nil)
	spec := greetSpec("/v1/greet-body")
	spec.Body = stringPointer("name")
	spec.ResponseBody = "message"
	if err := g.ApplyRoutes([]RouteSpec{spec}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	// name is body-bound, so a duplicate query value cannot replace it.
	req := httptest.NewRequest(http.MethodPost, "/v1/greet-body?name=query", strings.NewReader(`"body"`))
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "body") || strings.Contains(rec.Body.String(), "query") {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestQueryValidationAndLenientOverride(t *testing.T) {
	b := newBackend(t, "greeter")
	spec := greetSpec("/v1/greet-query")
	spec.Method = http.MethodGet
	spec.Body = stringPointer("")

	for _, tc := range []struct {
		name         string
		allowUnknown bool
		query        string
		want         int
	}{
		{"unknown is strict", false, "unknown=x", http.StatusBadRequest},
		{"unknown can be ignored", true, "unknown=x&name=web", http.StatusOK},
		{"bad integer", false, "name=web&times=bad", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateway(t, b, func(opts *Options) { opts.AllowUnknownFields = tc.allowUnknown })
			if err := g.ApplyRoutes([]RouteSpec{spec}); err != nil {
				t.Fatalf("ApplyRoutes: %v", err)
			}
			req := httptest.NewRequest(http.MethodGet, "/v1/greet-query?"+tc.query, nil)
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestTemplatePrecedenceDuplicatesAndAllow(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, nil)
	template := RouteSpec{Method: http.MethodGet, Path: "/v1/{name}", Service: "greeter", FullMethod: greetStreamMeth, Body: stringPointer("")}
	exact := RouteSpec{Method: http.MethodGet, Path: "/v1/me", Service: "greeter", FullMethod: greetMethod, Body: stringPointer("")}
	exactPost := exact
	exactPost.Method = http.MethodPost
	if err := g.ApplyRoutes([]RouteSpec{template, exact, exactPost}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/me?times=1", nil)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("literal route did not win: status=%d content-type=%q body=%s", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodDelete, "/v1/me", nil)
	rec = httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("405 status=%d Allow=%q", rec.Code, rec.Header().Get("Allow"))
	}

	duplicate := template
	duplicate.Path = "/v1/{other}"
	if err := g.ApplyRoutes([]RouteSpec{template, duplicate}); err == nil {
		t.Fatal("equivalent variable templates were accepted")
	}
	// The rejected table must leave the previously working exact route live.
	req = httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	rec = httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rejected table changed the live route: %d", rec.Code)
	}
}

func TestDescriptorRejectsBadBindingsAndClientStreaming(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, nil)
	for _, tc := range []struct {
		name   string
		mutate func(*RouteSpec)
	}{
		{"unknown body field", func(s *RouteSpec) { s.Body = stringPointer("missing") }},
		{"unknown response field", func(s *RouteSpec) { s.ResponseBody = "missing" }},
		{"unknown path field", func(s *RouteSpec) { s.Path = "/v1/{missing}"; s.Body = stringPointer("") }},
		{"client streaming", func(s *RouteSpec) { s.FullMethod = "/sweet.example.v1.Greeter/GreetAll" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := greetSpec("/v1/bad-binding")
			tc.mutate(&spec)
			if err := g.ApplyRoutes([]RouteSpec{spec}); err == nil {
				t.Fatal("invalid route was accepted")
			}
		})
	}
}
