package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
)

// greeterTypes is the compiled half of a config-driven table: the message
// factories a route spec can name by service and method.
var greeterTypes = ServiceTypes{
	"Greet": {
		Request: func() proto.Message { return &pb.GreetRequest{} },
		Reply:   func() proto.Message { return &pb.GreetReply{} },
	},
	"GreetStream": {
		Request: func() proto.Message { return &pb.GreetRequest{} },
		Reply:   func() proto.Message { return &pb.GreetReply{} },
	},
}

func greetSpec(path string) RouteSpec {
	return RouteSpec{
		Method: "POST", Path: path, Service: "greeter", FullMethod: greetMethod,
	}
}

func servicesOption(opts *Options) {
	opts.Services = map[string]ServiceTypes{greetService: greeterTypes}
}

// --- parsing ---------------------------------------------------------------

func TestParseRouteSpecsRejectsUnknownKey(t *testing.T) {
	// "serverStreams" with a stray s: the silent-drop version of a typo would
	// produce a gateway answering 400 on a streaming method.
	_, err := ParseRouteSpecs([]byte(`{"routes":[{"method":"POST","path":"/x","service":"s",` +
		`"fullMethod":"/a.B/C","serverStreams":true}]}`))
	if err == nil {
		t.Fatal("want an error for an unknown key, got none")
	}
	if !strings.Contains(err.Error(), "serverStreams") {
		t.Errorf("error should name the offending key; got %v", err)
	}
}

func TestParseRouteSpecsRefusesEmptyTable(t *testing.T) {
	if _, err := ParseRouteSpecs([]byte(`{"routes":[]}`)); err == nil {
		t.Fatal("an empty table must be refused, not installed")
	}
	if _, err := ParseRouteSpecs([]byte(`   `)); err == nil {
		t.Fatal("blank input must be refused")
	}
}

func TestParseRouteSpecsRejectsTrailingContent(t *testing.T) {
	if _, err := ParseRouteSpecs([]byte(`{"routes":[{"method":"POST","path":"/x","service":"s","fullMethod":"/a.B/C"}]}{} `)); err == nil {
		t.Fatal("two JSON documents in one dataId must not be silently accepted")
	}
}

// --- resolution ------------------------------------------------------------

func TestApplyRoutesResolvesTypesAndServes(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, servicesOption)

	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	rec := post(t, g, `{"name":"lily"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s, want 200", rec.Code, rec.Body.String())
	}
	if got := decodeMessage(t, rec); !strings.Contains(got, "lily") {
		t.Errorf("reply = %q, want it to carry the name", got)
	}
	routes := g.Routes()
	if len(routes) != 1 || routes[0].NewRequest == nil || routes[0].NewReply == nil {
		t.Fatalf("routes = %+v, want one route with both constructors bound", routes)
	}
}

func TestApplyRoutesRejectsBadTableAndKeepsTheLiveOne(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, servicesOption)
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("first ApplyRoutes: %v", err)
	}
	before := g.Routes()

	// Second route in the table names a method nobody registered. The whole
	// table must be refused — a half-installed table is the state an operator
	// cannot reason about.
	bad := []RouteSpec{
		greetSpec("/v1/kept"),
		{Method: "POST", Path: "/v1/bad", Service: "greeter", FullMethod: "/sweet.example.v1.Greeter/Nope"},
	}
	err := g.ApplyRoutes(bad)
	if err == nil {
		t.Fatal("want an error for an unregistered method")
	}
	if !strings.Contains(err.Error(), "Nope") {
		t.Errorf("error should name the unknown method; got %v", err)
	}

	after := g.Routes()
	if len(after) != len(before) || after[0].Path != before[0].Path || after[0].FullMethod != before[0].FullMethod {
		t.Fatalf("live table changed after a rejected apply: %+v", after)
	}
	if rec := post(t, g, `{"name":"lily"}`); rec.Code != http.StatusOK {
		t.Errorf("the route that worked before must still work; got %d", rec.Code)
	}
}

func TestApplyRoutesNormalizesMethodCase(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, servicesOption)
	spec := greetSpec(greetPath)
	spec.Method = "post" // as an operator would type it into a config file
	if err := g.ApplyRoutes([]RouteSpec{spec}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	if got := g.Routes()[0].Method; got != http.MethodPost {
		t.Fatalf("Method = %q, want %q", got, http.MethodPost)
	}
	if rec := post(t, g, `{"name":"lily"}`); rec.Code != http.StatusOK {
		t.Fatalf("lower-cased spec must still match a real request; got %d", rec.Code)
	}
}

func TestApplyRoutesRejectsDuplicateWithinOneTable(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, servicesOption)
	err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath), greetSpec(greetPath)})
	if err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("err = %v, want a duplicate-route error", err)
	}
}

func TestApplyRoutesRejectsUnknownServiceAndMalformedMethod(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, servicesOption)

	unknown := greetSpec("/v1/x")
	unknown.FullMethod = "/other.Service/Greet"
	if err := g.ApplyRoutes([]RouteSpec{unknown}); err == nil ||
		!strings.Contains(err.Error(), "Options.Services") {
		t.Fatalf("err = %v, want a message pointing at Options.Services", err)
	}

	for _, full := range []string{"", "no-leading-slash", "/OnlyService", "/a.B/", "/a.B/C/D"} {
		spec := greetSpec("/v1/x")
		spec.FullMethod = full
		if err := g.ApplyRoutes([]RouteSpec{spec}); err == nil {
			t.Errorf("fullMethod %q accepted, want rejection", full)
		}
	}
}

// --- hot swap over a real listener ----------------------------------------

// TestApplyRoutesSwapsLiveOnRealServer mounts the gateway on an actual server:
// httptest.ResponseRecorder hides what a live transport does with headers and
// write ordering, and a swap that only works against a recorder is not a swap.
func TestApplyRoutesSwapsLiveOnRealServer(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, servicesOption)
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("first ApplyRoutes: %v", err)
	}
	srv := httptest.NewServer(g)
	defer srv.Close()

	call := func(path string) int {
		t.Helper()
		resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(`{"name":"lily"}`))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if got := call(greetPath); got != http.StatusOK {
		t.Fatalf("initial table: %s = %d, want 200", greetPath, got)
	}

	const swapped = "/v1/greeter/second"
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(swapped)}); err != nil {
		t.Fatalf("second ApplyRoutes: %v", err)
	}
	if got := call(greetPath); got != http.StatusNotFound {
		t.Errorf("after swap: old %s = %d, want 404", greetPath, got)
	}
	if got := call(swapped); got != http.StatusOK {
		t.Errorf("after swap: new %s = %d, want 200", swapped, got)
	}
}
