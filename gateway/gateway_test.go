package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
	"github.com/javahongxi/sweet/registry/direct"
	"github.com/javahongxi/sweet/server"
)

// The backend these tests talk to is sweet's own generated greeter stub. A
// gateway test needs a real protobuf service, and greeterpb is the canonical
// generated one in this repo — importing it from a test file adds no
// dependency to the gateway package itself.
type testGreeter struct {
	pb.UnimplementedGreeterServer
}

// Greet is deliberately multi-purpose so one method can drive every assertion:
//
//   - "fail:<code number>" answers with that gRPC code, which is how the status
//     mapping gets tested code by code. grpc-go has no name-to-code parser
//     (codes.Code offers String and an UnmarshalJSON, nothing else — see
//     codes/codes.go:225), so the test drives the numeric code and asserts the
//     name that comes back.
//   - "sleep:<duration>" blocks first, which is how deadline propagation gets
//     tested.
//   - anything else comes back echoed in the message, together with the
//     metadata the gateway forwarded, so header policy is observable from the
//     HTTP side without a second RPC.
func (testGreeter) Greet(ctx context.Context, in *pb.GreetRequest) (*pb.GreetReply, error) {
	name := in.GetName()
	if strings.HasPrefix(name, "fail:") {
		raw := strings.TrimPrefix(name, "fail:")
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad fail code: "+raw)
		}
		code := codes.Code(n)
		return nil, status.Error(code, "backend message for "+raw)
	}
	if strings.HasPrefix(name, "sleep:") {
		d, err := time.ParseDuration(strings.TrimPrefix(name, "sleep:"))
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad sleep duration: "+err.Error())
		}
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	md, _ := metadata.FromIncomingContext(ctx)
	picked := []string{}
	for _, key := range []string{"x-request-id", "traceparent", "authorization", "x-note", "x-note-bin", "x-a!b", "grpc-internal"} {
		if v := md.Get(key); len(v) > 0 {
			picked = append(picked, key+"="+v[0])
		}
	}
	return &pb.GreetReply{Message: "Hello, " + name + "!|" + strings.Join(picked, ",")}, nil
}

// GreetStream is the streaming half of the same trick, with three failure
// shapes that the gateway has to treat differently:
//
//   - "fail:<code>" errors before sending anything, so the HTTP status is still
//     the gateway's to choose (an ordinary error body, not a stream).
//   - "failafter:<count>:<code>" sends <count> frames then errors, which is the
//     only case where 200 has already been committed and the error has to travel
//     in the body.
//   - "slow:<gap>:<count>" sends <count> frames one gap apart, so a caller
//     deadline lands mid-stream and so flush behaviour is observable at all.
//
// A plain request with Times=0 sends nothing and returns nil: zero frames is a
// successful stream, and the gateway must not turn that into an internal error.
func (testGreeter) GreetStream(in *pb.GreetRequest, stream pb.Greeter_GreetStreamServer) error {
	name := in.GetName()

	switch {
	case strings.HasPrefix(name, "fail:"):
		n, err := strconv.Atoi(strings.TrimPrefix(name, "fail:"))
		if err != nil {
			return status.Error(codes.InvalidArgument, "bad fail code: "+name)
		}
		return status.Error(codes.Code(n), "backend message before any frame")

	case strings.HasPrefix(name, "failafter:"):
		countStr, rest, _ := strings.Cut(strings.TrimPrefix(name, "failafter:"), ":")
		count, err1 := strconv.Atoi(countStr)
		codeNum, err2 := strconv.Atoi(rest)
		if err1 != nil || err2 != nil {
			return status.Error(codes.InvalidArgument, "bad failafter: "+name)
		}
		for i := 1; i <= count; i++ {
			if err := stream.Send(&pb.GreetReply{Message: fmt.Sprintf("frame %d of %d", i, count)}); err != nil {
				return err
			}
		}
		return status.Error(codes.Code(codeNum), "backend message after "+countStr+" frames")

	case strings.HasPrefix(name, "slow:"):
		gapStr, countStr, _ := strings.Cut(strings.TrimPrefix(name, "slow:"), ":")
		gap, err1 := time.ParseDuration(gapStr)
		count, err2 := strconv.Atoi(countStr)
		if err1 != nil || err2 != nil {
			return status.Error(codes.InvalidArgument, "bad slow: "+name)
		}
		for i := 1; i <= count; i++ {
			select {
			case <-time.After(gap):
			case <-stream.Context().Done():
				return status.FromContextError(stream.Context().Err()).Err()
			}
			if err := stream.Send(&pb.GreetReply{Message: fmt.Sprintf("frame %d of %d", i, count)}); err != nil {
				return err
			}
		}
		return nil
	}

	times := int(in.GetTimes())
	for i := 1; i <= times; i++ {
		if err := stream.Send(&pb.GreetReply{Message: fmt.Sprintf("Hello #%d, %s!", i, name)}); err != nil {
			return err
		}
	}
	return nil
}

// newBackend starts one sweet server and returns the registry the gateway
// should use to find it.
func newBackend(t *testing.T, serviceName string) backend {
	t.Helper()
	srv, err := server.New(server.Options{
		Address:     "127.0.0.1:0",
		ServiceName: serviceName,
		Registry:    direct.Nope(), // the gateway has its own registry; this one publishes nothing
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Register(&pb.Greeter_ServiceDesc, testGreeter{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	go func() {
		if err := srv.Serve(); err != nil {
			log.Printf("test backend Serve: %v", err)
		}
	}()
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(srv.Stop)

	reg, err := direct.New(srv.Addr())
	if err != nil {
		t.Fatalf("direct.New(%s): %v", srv.Addr(), err)
	}
	return backend{reg: reg, service: serviceName}
}

// backend bundles the lookup name with the implementation, because every test
// needs both and forgetting one is the whole class of bug this package has.
type backend struct {
	reg     *direct.Registry
	service string
}

const (
	greetService    = "sweet.example.v1.Greeter"
	greetMethod     = "/sweet.example.v1.Greeter/Greet"
	greetPath       = "/v1/greeter/greet"
	greetStreamMeth = "/sweet.example.v1.Greeter/GreetStream"
	greetStreamPath = "/v1/greeter/greet-stream"
)

func greetRoute(service string) Route {
	return Route{
		Method:     http.MethodPost,
		Path:       greetPath,
		Service:    service,
		FullMethod: greetMethod,
		NewRequest: func() proto.Message { return &pb.GreetRequest{} },
		NewReply:   func() proto.Message { return &pb.GreetReply{} },
	}
}

func greetStreamRoute(service string) Route {
	r := greetRoute(service)
	r.Path = greetStreamPath
	r.FullMethod = greetStreamMeth
	r.ServerStream = true
	return r
}

// newStreamingGateway registers only the server-streaming route, so a test that
// counts Routes() or asserts on 404s is not confused by the unary one.
func newStreamingGateway(t *testing.T, b backend, mutate func(*Options)) *Gateway {
	t.Helper()
	opts := Options{
		Registry:    b.reg,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
	if mutate != nil {
		mutate(&opts)
	}
	g, err := New(opts)
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	if err := g.Handle(greetStreamRoute(b.service)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
	})
	return g
}

// postStream is the streaming counterpart of post: it returns the raw body so a
// test can look at individual lines.
func postStream(t *testing.T, g *Gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, greetStreamPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

// lines splits an NDJSON body into its records, ignoring the trailing newline.
func lines(body string) []string {
	trimmed := strings.TrimSuffix(body, "\n")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

func newGateway(t *testing.T, b backend, mutate func(*Options)) *Gateway {
	t.Helper()
	opts := Options{
		Registry:    b.reg,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
	if mutate != nil {
		mutate(&opts)
	}
	g, err := New(opts)
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	if err := g.Handle(greetRoute(b.service)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
	})
	return g
}

func post(t *testing.T, g *Gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, greetPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) ErrorBody {
	t.Helper()
	var body ErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("error body is not JSON (%s): %v", rec.Body.String(), err)
	}
	return body
}

func decodeMessage(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("reply body is not JSON (%s): %v", rec.Body.String(), err)
	}
	msg, ok := out["message"].(string)
	if !ok {
		t.Fatalf("reply has no string field \"message\": %#v", out)
	}
	return msg
}

// The unary round trip, which is the claim the whole package rests on.
func TestUnaryRoundTrip(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	rec := post(t, g, `{"name":"web"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	got := decodeMessage(t, rec)
	if !strings.HasPrefix(got, "Hello, web!") {
		t.Fatalf("message = %q, want it to start with \"Hello, web!\"", got)
	}
}

// An empty body is "all fields default", not a client error. A provider method
// whose fields are all optional is a real shape.
func TestEmptyBodyMeansDefaults(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	rec := post(t, g, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if msg := decodeMessage(t, rec); !strings.HasPrefix(msg, "Hello, !") {
		t.Errorf("message = %q, want the zero-value request echoed back", msg)
	}
}

// A misspelt field name fails loudly by default, and only becomes silent when
// the caller opts into leniency. Both halves are asserted, because the option
// is worthless if it does not actually change behaviour.
func TestUnknownJSONField(t *testing.T) {
	b := newBackend(t, greetService)

	t.Run("strict by default", func(t *testing.T) {
		g := newGateway(t, b, nil)
		rec := post(t, g, `{"nam":"typo"}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
		}
		body := decodeError(t, rec)
		if body.Code != "INVALID_ARGUMENT" || body.CodeNumber != int32(codes.InvalidArgument) {
			t.Errorf("code = %s/%d, want INVALID_ARGUMENT/%d", body.Code, body.CodeNumber, codes.InvalidArgument)
		}
		if body.HTTPStatus != rec.Code {
			t.Errorf("httpStatus in body = %d, does not match the response status %d", body.HTTPStatus, rec.Code)
		}
	})

	t.Run("lenient when asked", func(t *testing.T) {
		g := newGateway(t, b, func(o *Options) { o.AllowUnknownFields = true })
		rec := post(t, g, `{"nam":"typo","name":"web"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("malformed JSON is 400", func(t *testing.T) {
		g := newGateway(t, b, nil)
		rec := post(t, g, `{"name":`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body %s)", rec.Code, rec.Body.String())
		}
	})
}

// The mapping table, one case per gRPC code. Each row is asserted through a
// real RPC rather than only through HTTPStatusOf, so a regression in either the
// table or the writer is caught.
func TestBackendStatusMapping(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	for _, tc := range []struct {
		number   int
		grpcName string
		wantHTTP int
	}{
		{3, "INVALID_ARGUMENT", http.StatusBadRequest},
		{5, "NOT_FOUND", http.StatusNotFound},
		{6, "ALREADY_EXISTS", http.StatusConflict},
		{7, "PERMISSION_DENIED", http.StatusForbidden},
		{16, "UNAUTHENTICATED", http.StatusUnauthorized},
		{8, "RESOURCE_EXHAUSTED", http.StatusTooManyRequests},
		{9, "FAILED_PRECONDITION", http.StatusBadRequest},
		{10, "ABORTED", http.StatusConflict},
		{11, "OUT_OF_RANGE", http.StatusBadRequest},
		{12, "UNIMPLEMENTED", http.StatusNotImplemented},
		{4, "DEADLINE_EXCEEDED", http.StatusGatewayTimeout},
		{14, "UNAVAILABLE", http.StatusServiceUnavailable},
		{13, "INTERNAL", http.StatusInternalServerError},
		{15, "DATA_LOSS", http.StatusInternalServerError},
		{2, "UNKNOWN", http.StatusInternalServerError},
	} {
		t.Run(tc.grpcName, func(t *testing.T) {
			rec := post(t, g, `{"name":"fail:`+strconv.Itoa(tc.number)+`"}`)
			if rec.Code != tc.wantHTTP {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.wantHTTP, rec.Body.String())
			}
			body := decodeError(t, rec)
			if body.Code != tc.grpcName {
				t.Errorf("code = %q, want the canonical spelling %q (grpc-go's own String would say %q)",
					body.Code, tc.grpcName, codes.Code(tc.number).String())
			}
			if body.CodeNumber != int32(tc.number) {
				t.Errorf("codeNumber = %d, want %d", body.CodeNumber, tc.number)
			}
			if body.HTTPStatus != rec.Code {
				t.Errorf("httpStatus in body = %d, does not match the response status %d", body.HTTPStatus, rec.Code)
			}
			// 4xx messages describe the caller's request and are always echoed;
			// the backend's own text for a 5xx is masked by default (see the
			// masking test below).
			if tc.wantHTTP < 500 {
				wantMsg := "backend message for " + strconv.Itoa(tc.number)
				if body.Message != wantMsg {
					t.Errorf("message = %q, want the backend's %q", body.Message, wantMsg)
				}
			}
		})
	}
}

// Canceled is the one code the backend cannot produce on its own, so it is
// asserted through the table directly instead of faked.
func TestCanceledMapsTo499(t *testing.T) {
	if got := HTTPStatusOf(codes.Canceled); got != 499 {
		t.Errorf("Canceled -> %d, want 499", got)
	}
	if got := HTTPStatusOf(codes.Code(9001)); got != http.StatusInternalServerError {
		t.Errorf("an unlisted code -> %d, want 500 as the default", got)
	}
}

func TestVerboseErrorsControlsMessageEcho(t *testing.T) {
	b := newBackend(t, greetService)

	t.Run("masked by default", func(t *testing.T) {
		g := newGateway(t, b, nil)
		rec := post(t, g, `{"name":"fail:13"}`) // INTERNAL
		body := decodeError(t, rec)
		if body.Message != maskedMessage {
			t.Fatalf("message = %q, want it masked as %q", body.Message, maskedMessage)
		}
		// The code is still reported: hiding the message is not the same as
		// hiding which failure happened.
		if body.Code != "INTERNAL" {
			t.Errorf("code = %q, want INTERNAL even when masked", body.Code)
		}
	})

	t.Run("echoed when asked", func(t *testing.T) {
		g := newGateway(t, b, func(o *Options) { o.VerboseErrors = true })
		rec := post(t, g, `{"name":"fail:13"}`)
		if body := decodeError(t, rec); body.Message != "backend message for 13" {
			t.Fatalf("message = %q, want the backend's own text", body.Message)
		}
	})
}

func TestRoutingErrors(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	t.Run("unknown path is 404 with codeNumber -1", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/nope", strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
		body := decodeError(t, rec)
		if body.CodeNumber != -1 {
			t.Errorf("codeNumber = %d, want -1: this request never became an RPC", body.CodeNumber)
		}
	})

	t.Run("wrong method is 405 and says what is allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, greetPath, nil)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", rec.Code)
		}
		if allow := rec.Header().Get("Allow"); allow != http.MethodPost {
			t.Errorf("Allow = %q, want POST", allow)
		}
		if body := decodeError(t, rec); body.Code != "METHOD_NOT_ALLOWED" || body.CodeNumber != -1 {
			t.Errorf("body = %+v, want METHOD_NOT_ALLOWED with codeNumber -1", body)
		}
	})

	t.Run("a doubled slash still finds the route", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "//v1//greeter//greet", strings.NewReader(`{"name":"web"}`))
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
	})
}

// The two deadline paths, asserted separately: a caller's own bound is honoured
// (and is not stretched by RequestTimeout), and a caller with no bound gets the
// gateway's floor.
func TestDeadlinePropagation(t *testing.T) {
	b := newBackend(t, greetService)

	t.Run("caller deadline wins", func(t *testing.T) {
		g := newGateway(t, b, func(o *Options) { o.RequestTimeout = 5 * time.Second })
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		req := httptest.NewRequest(http.MethodPost, greetPath, strings.NewReader(`{"name":"sleep:300ms"}`))
		req = req.WithContext(ctx)
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, req)
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("status = %d, want 504 — a 5s floor must not override a 30ms caller deadline (body %s)", rec.Code, rec.Body.String())
		}
		if body := decodeError(t, rec); body.Code != "DEADLINE_EXCEEDED" {
			t.Errorf("code = %q, want DEADLINE_EXCEEDED", body.Code)
		}
	})

	t.Run("gateway timeout bounds a caller with none", func(t *testing.T) {
		g := newGateway(t, b, func(o *Options) { o.RequestTimeout = 30 * time.Millisecond })
		rec := post(t, g, `{"name":"sleep:300ms"}`)
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("status = %d, want 504 (body %s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("slow enough caller still succeeds", func(t *testing.T) {
		g := newGateway(t, b, func(o *Options) { o.RequestTimeout = time.Second })
		rec := post(t, g, `{"name":"sleep:10ms"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
	})
}

// A route table is only as good as its validation: a typo in a route should
// fail at registration, not at request time.
func TestRouteValidation(t *testing.T) {
	b := newBackend(t, greetService)
	g, err := New(Options{
		Registry:    b.reg,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	base := greetRoute(b.service)
	for _, tc := range []struct {
		name   string
		mutate func(*Route)
	}{
		{"no method", func(r *Route) { r.Method = "" }},
		{"method with a space", func(r *Route) { r.Method = "P OST" }},
		{"relative path", func(r *Route) { r.Path = "v1/greet" }},
		{"non-canonical path", func(r *Route) { r.Path = "/v1//greet" }},
		{"no service", func(r *Route) { r.Service = "" }},
		{"bad full method", func(r *Route) { r.FullMethod = "sweet.example.v1.Greeter/Greet" }},
		{"no request ctor", func(r *Route) { r.NewRequest = nil }},
		{"no reply ctor", func(r *Route) { r.NewReply = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			tc.mutate(&r)
			if err := g.Handle(r); err == nil {
				t.Fatalf("Handle accepted an invalid route: %+v", r)
			}
		})
	}

	t.Run("duplicate is rejected", func(t *testing.T) {
		if err := g.Handle(base); err != nil {
			t.Fatalf("first Handle: %v", err)
		}
		if err := g.Handle(base); err == nil {
			t.Fatal("duplicate route was accepted")
		}
	})

	t.Run("missing registry is rejected", func(t *testing.T) {
		if _, err := New(Options{}); err == nil {
			t.Fatal("New accepted a gateway with no registry")
		}
	})
}

// One connection per service, not per route: two HTTP endpoints on the same
// provider must not open two conns, and the resolver's work would otherwise be
// repeated for every route.
func TestConnectionsAreSharedPerService(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	second := Route{
		Method:     http.MethodPost,
		Path:       "/v1/greeter/greet-again",
		Service:    b.service,
		FullMethod: greetMethod,
		NewRequest: func() proto.Message { return &pb.GreetRequest{} },
		NewReply:   func() proto.Message { return &pb.GreetReply{} },
	}
	if err := g.Handle(second); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	for _, body := range []string{`{"name":"a"}`, `{"name":"b"}`} {
		rec := post(t, g, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodPost, second.Path, strings.NewReader(`{"name":"c"}`))
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}

	g.mu.Lock()
	n := len(g.conns)
	g.mu.Unlock()
	if n != 1 {
		t.Errorf("%d connections for three routes on one service, want 1", n)
	}
	if got := len(g.Routes()); got != 2 {
		t.Errorf("Routes() = %d, want 2", got)
	}
}

// No reachable instance is the failure a caller actually hits first, and it has
// to be distinguishable from "your JSON was wrong".
func TestNoInstancesIs503(t *testing.T) {
	reg, err := direct.New("127.0.0.1:1") // nothing listens on port 1
	if err != nil {
		t.Fatalf("direct.New: %v", err)
	}
	g, err := New(Options{
		Registry:       reg,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		RequestTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := g.Handle(greetRoute(greetService)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
	}()

	rec := post(t, g, `{"name":"web"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (body %s)", rec.Code, rec.Body.String())
	}
	if body := decodeError(t, rec); body.Code != "UNAVAILABLE" {
		t.Errorf("code = %q, want UNAVAILABLE", body.Code)
	}
}

// Header policy, asserted at the function that implements it rather than
// through a request: the odd header names a real client cannot send are exactly
// the ones worth pinning.
func TestForwardedHeaderPolicy(t *testing.T) {
	h := http.Header{}
	h.Set("X-Request-Id", "req-1")            // default forward
	h.Set("traceparent", "00-abc-def-01")     // default forward
	h.Set("Authorization", "Bearer secret")   // NOT by default
	h.Set("X-Note", "普通中文")                   // becomes -bin + base64
	h.Set("X-A!B", "dropped-by-key-alphabet") // not a valid metadata key
	h.Set("Grpc-Custom", "reserved-prefix")   // grpc- is reserved
	h.Set("Content-Type", "application/json") // framing: never
	h.Set("X-Not-Listed", "quiet")            // not on the allow list

	// The policy is an allow list, so X-Note has to be named — which is the
	// point: a header reaching a provider is a decision, not a side effect.
	md := outgoing(t, withMetadata(context.Background(), h, []string{"X-Note"}))
	for key, want := range map[string]string{
		"x-request-id": "req-1",
		"traceparent":  "00-abc-def-01",
	} {
		if got := md[key]; len(got) != 1 || got[0] != want {
			t.Errorf("metadata[%q] = %v, want [%q]", key, got, want)
		}
	}
	if got := md["x-note"]; len(got) != 0 {
		t.Errorf("a UTF-8 value must not travel under the plain key, got %v", got)
	}
	encoded := md["x-note-bin"]
	if len(encoded) != 1 {
		t.Fatalf("metadata[x-note-bin] = %v, want one base64 value", encoded)
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded[0])
	if err != nil {
		t.Fatalf("x-note-bin is not valid base64: %v", err)
	}
	if string(decoded) != "普通中文" {
		t.Errorf("decoded x-note-bin = %q, want 普通中文", decoded)
	}
	for _, key := range []string{"authorization", "x-a!b", "grpc-custom", "content-type", "x-not-listed"} {
		if len(md[key]) != 0 {
			t.Errorf("metadata[%q] = %v, want it dropped", key, md[key])
		}
	}

	// Naming a header explicitly is what lets it through — including the ones
	// this package declines to forward on its own.
	h2 := http.Header{}
	h2.Set("Authorization", "Bearer secret")
	md2 := outgoing(t, withMetadata(context.Background(), h2, []string{"Authorization"}))
	if got := md2["authorization"]; len(got) != 1 || got[0] != "Bearer secret" {
		t.Errorf("explicitly forwarded authorization = %v, want [\"Bearer secret\"]", got)
	}
}

func outgoing(t *testing.T, ctx context.Context) metadata.MD {
	t.Helper()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		// Nothing was forwarded, which is a legitimate outcome — an empty MD is
		// what the caller should see, not a missing key.
		return metadata.MD{}
	}
	return md
}

// The same policy, end to end: what the provider actually sees.
func TestForwardedHeadersReachTheProvider(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, func(o *Options) { o.ForwardHeaders = []string{"X-Note"} })

	req := httptest.NewRequest(http.MethodPost, greetPath, strings.NewReader(`{"name":"web"}`))
	req.Header.Set("X-Request-Id", "req-42")
	req.Header.Set("Traceparent", "00-trace-span-01")
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("X-Note", "中文")
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	msg := decodeMessage(t, rec)
	if !strings.Contains(msg, "x-request-id=req-42") {
		t.Errorf("provider did not see x-request-id: %q", msg)
	}
	if !strings.Contains(msg, "traceparent=00-trace-span-01") {
		t.Errorf("provider did not see traceparent: %q", msg)
	}
	if !strings.Contains(msg, "x-note-bin=") {
		t.Errorf("provider did not see x-note-bin: %q", msg)
	}
	if strings.Contains(msg, "authorization=") {
		t.Errorf("authorization leaked to the provider: %q", msg)
	}
}

// protojson's output is intentionally unstable at the byte level (it inserts
// randomized whitespace), and EmitUnpopulated keeps zero fields present. This
// test pins the second property without depending on the first.
func TestReplyShapeIsStable(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	rec := post(t, g, `{"name":"web"}`)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("reply is not JSON (%s): %v", rec.Body.String(), err)
	}
	if _, ok := out["message"]; !ok {
		t.Fatalf("reply lost the message field: %s", rec.Body.String())
	}
	// Field names stay as the .proto spells them: "message", not "Message".
	for key := range out {
		if key != strings.ToLower(key) {
			t.Errorf("field %q was renamed to camelCase, want the proto name", key)
		}
	}
}

// A double WriteHeader is invisible to httptest.ResponseRecorder — the recorder
// keeps the first one and ignores the rest — while a real http.Server logs it as
// "superfluous response.WriteHeader call". This test mounts the gateway on a
// real server and asserts that server's own error log stays empty across every
// failure path, which is the only way that class of bug gets caught here.
func TestRealServerErrorLogStaysClean(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	var logs syncBuffer
	ts := httptest.NewUnstartedServer(g)
	ts.Config.ErrorLog = log.New(&logs, "", 0)
	ts.Start()
	defer ts.Close()

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{"405 wrong method", http.MethodGet, greetPath, "", http.StatusMethodNotAllowed},
		{"404 unknown path", http.MethodPost, "/v1/nope", "{}", http.StatusNotFound},
		{"400 gateway-side decode error", http.MethodPost, greetPath, `{"nam":"x"}`, http.StatusBadRequest},
		{"500 provider error, masked", http.MethodPost, greetPath, `{"name":"fail:13"}`, http.StatusInternalServerError},
		{"200 happy", http.MethodPost, greetPath, `{"name":"web"}`, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", resp.StatusCode, tc.want, data)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q, want application/json for every response", ct)
			}
		})
	}

	if got := logs.String(); got != "" {
		t.Errorf("the server logged something while serving these requests: %q", got)
	}
}

// syncBuffer is a bytes.Buffer that can be handed to log.New without racing the
// http.Server's goroutines; -race would otherwise flag the shared write.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// One line per frame, in arrival order, and no error record hiding inside a
// successful stream. Collecting the stream and printing it once would let a
// truncated body look complete, which is the failure this whole file is about.
func TestServerStreamEmitsOneLinePerFrame(t *testing.T) {
	b := newBackend(t, greetService)
	g := newStreamingGateway(t, b, nil)

	rec := postStream(t, g, `{"name":"web","times":3}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson", ct)
	}
	got := lines(rec.Body.String())
	if len(got) != 3 {
		t.Fatalf("%d lines, want 3: %#v", len(got), got)
	}
	for i, line := range got {
		var out map[string]any
		if err := json.Unmarshal([]byte(line), &out); err != nil {
			t.Fatalf("line %d is not one JSON object (%q): %v", i+1, line, err)
		}
		want := fmt.Sprintf("Hello #%d, web!", i+1)
		if out["message"] != want {
			t.Errorf("line %d message = %v, want %q", i+1, out["message"], want)
		}
		if _, isErr := out["error"]; isErr {
			t.Errorf("line %d is an error line in a successful stream: %s", i+1, line)
		}
	}
}

// Zero frames is a successful stream. With StreamDesc.ServerStreams left false
// grpc-go rewrites exactly this case into an Internal "cardinality violation"
// (stream.go:1290-1298), so the assertion here is the reason the flag is set.
func TestServerStreamZeroFramesIsSuccess(t *testing.T) {
	b := newBackend(t, greetService)
	g := newStreamingGateway(t, b, nil)

	rec := postStream(t, g, `{"name":"web","times":0}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body = %q, want empty", rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("Content-Type = %q, want application/x-ndjson even for an empty stream", ct)
	}
}

// A provider that fails before sending anything is still reportable as an HTTP
// status — that is the whole reason the stream loop tracks whether it committed.
func TestServerStreamErrorBeforeFirstFrameIsAnHTTPStatus(t *testing.T) {
	b := newBackend(t, greetService)
	g := newStreamingGateway(t, b, nil)

	rec := postStream(t, g, `{"name":"fail:7","times":3}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	body := decodeError(t, rec)
	if body.Code != "PERMISSION_DENIED" || body.CodeNumber != 7 {
		t.Errorf("body = %+v, want PERMISSION_DENIED/7", body)
	}
	if body.Message != "backend message before any frame" {
		t.Errorf("message = %q, want the 4xx text echoed", body.Message)
	}
	// Not a stream body: no NDJSON content type once the error path is taken.
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want the ordinary error media type", ct)
	}
}

// Once a frame has gone out, 200 is committed. The error then travels as one
// more line, and the httpStatus field is deliberately absent from it.
func TestServerStreamErrorMidStreamTravelsInBand(t *testing.T) {
	b := newBackend(t, greetService)
	g := newStreamingGateway(t, b, nil)

	rec := postStream(t, g, `{"name":"failafter:2:7","times":9}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 — the first frame was already out (body %s)", rec.Code, rec.Body.String())
	}
	got := lines(rec.Body.String())
	if len(got) != 3 {
		t.Fatalf("%d lines, want 2 frames plus one error line: %#v", len(got), got)
	}
	for i := 0; i < 2; i++ {
		if !strings.Contains(got[i], `"frame `+strconv.Itoa(i+1)+` of 2"`) {
			t.Errorf("line %d = %s, want the frame that was produced", i+1, got[i])
		}
	}

	var inBand struct {
		Error ErrorBody `json:"error"`
	}
	if err := json.Unmarshal([]byte(got[2]), &inBand); err != nil {
		t.Fatalf("last line is not an error record (%q): %v", got[2], err)
	}
	if inBand.Error.Code != "PERMISSION_DENIED" || inBand.Error.CodeNumber != 7 {
		t.Errorf("in-band error = %+v, want PERMISSION_DENIED/7", inBand.Error)
	}
	if inBand.Error.Message != "backend message after 2 frames" {
		t.Errorf("message = %q, want the 4xx text echoed", inBand.Error.Message)
	}
	if strings.Contains(got[2], "httpStatus") {
		t.Errorf("in-band line carries httpStatus, which would contradict the 200 on the wire: %s", got[2])
	}
}

// A caller deadline that lands mid-stream is the common real failure, and it must
// also surface in band rather than as an unexplained truncated body.
func TestServerStreamMidStreamDeadlineIsInBand(t *testing.T) {
	b := newBackend(t, greetService)
	g := newStreamingGateway(t, b, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, greetStreamPath, strings.NewReader(`{"name":"slow:30ms:20"}`))
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	got := lines(rec.Body.String())
	if len(got) < 2 {
		t.Fatalf("%d lines, want at least one frame plus the error line: %#v", len(got), got)
	}
	last := got[len(got)-1]
	if !strings.Contains(last, `"error"`) {
		t.Fatalf("last line is not an error record: %s", last)
	}
	if strings.Contains(last, "httpStatus") {
		t.Errorf("in-band line carries httpStatus: %s", last)
	}
	// DEADLINE_EXCEEDED maps to 504, so it is a 5xx and gets masked by default.
	if !strings.Contains(last, `"code":"DEADLINE_EXCEEDED"`) || !strings.Contains(last, `"codeNumber":4`) {
		t.Errorf("last line = %s, want DEADLINE_EXCEEDED/4", last)
	}
	if strings.Contains(last, maskedMessage) == false {
		t.Errorf("last line = %s, want the masked 5xx message", last)
	}
}

// The property a streaming gateway exists for: frames reach the caller while the
// provider is still producing them. Buffered output would still be *correct* JSON
// at the end, which is precisely why the assertion is about timing.
func TestServerStreamFlushesAsFramesArrive(t *testing.T) {
	b := newBackend(t, greetService)
	g := newStreamingGateway(t, b, nil)

	var logs syncBuffer
	ts := httptest.NewUnstartedServer(g)
	ts.Config.ErrorLog = log.New(&logs, "", 0)
	ts.Start()
	defer ts.Close()

	start := time.Now()
	req, err := http.NewRequest(http.MethodPost, ts.URL+greetStreamPath, strings.NewReader(`{"name":"slow:25ms:4"}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	frames := 0
	var firstFrameAt time.Duration
	for scanner.Scan() {
		if frames == 0 {
			firstFrameAt = time.Since(start)
		}
		frames++
	}
	total := time.Since(start)
	if err := scanner.Err(); err != nil {
		t.Fatalf("reading the stream: %v", err)
	}
	if frames != 4 {
		t.Fatalf("%d lines, want 4", frames)
	}
	// Four frames 25ms apart take roughly 100ms. A response that buffered would
	// report the first line only when the whole body was done.
	if firstFrameAt > total/2 {
		t.Errorf("first frame at %v of %v total — frames are not being flushed", firstFrameAt, total)
	}
	if got := logs.String(); got != "" {
		t.Errorf("the server logged while streaming a successful response: %q", got)
	}
}

// The gateway is an http.Handler, so mounting it under another server must work
// — that is how a real deployment wraps it in middleware.
func TestMountableAsHandler(t *testing.T) {
	b := newBackend(t, greetService)
	g := newGateway(t, b, nil)

	srv := httptest.NewServer(g)
	defer srv.Close()

	resp, err := srv.Client().Post(srv.URL+greetPath, "application/json", strings.NewReader(`{"name":"web"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	data, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(data), "Hello, web!") {
		t.Fatalf("body = %s, want the echoed greeting", data)
	}
}
