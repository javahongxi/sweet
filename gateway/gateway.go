// Package gateway turns HTTP requests into gRPC calls against services found
// through a registry.
//
// It is an edge adapter and deliberately nothing more. The services behind it
// still speak only gRPC, and sweet's server never learns that HTTP exists —
// which is what separates this package from an "HTTP transport" for sweet.
// Putting HTTP/1.1 back on the service-to-service path would be a regression;
// translating one client's REST call into one gRPC RPC at the boundary is
// simply how the gRPC ecosystem exposes services (Envoy, grpc-gateway).
//
// Scope, and why it stops where it does:
//
//   - Unary and server streaming. A streaming answer is newline-separated JSON
//     objects (NDJSON), flushed frame by frame, with the terminal error carried
//     in band when 200 is already committed — see serveStream. Client streaming
//     and bidi need a duplex transport (WebSocket, or SSE plus a second POST)
//     and are not half-implemented here.
//   - No protoc plugin, no generated proxy. Every route is declared in Go
//     together with the message types it uses, so each mapping rule is
//     assertable from this package's own tests instead of living in generated
//     code nobody reads. It is the same trade sweet makes in gen/: generate
//     messages, keep the method table in Go.
//   - No new dependency. JSON to protobuf goes through protojson, and
//     google.golang.org/protobuf is already required by the generated stubs.
//   - Routing is a table lookup, not http.ServeMux. ServeMux answers an
//     unmatched method with its own plain-text 405 that cannot be replaced, and
//     an edge that speaks two error formats is an edge nobody debugs. Exact
//     path matching is also all a gRPC route needs: a method is one operation,
//     so there is nothing for a wildcard to say.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/registry"
)

// Route is one HTTP endpoint mapped onto one gRPC method.
type Route struct {
	// Method is the HTTP method to match, e.g. "POST". One method per route: an
	// RPC is one operation, and letting GET reach the same method would mean
	// inventing a second encoding for query strings.
	Method string
	// Path is matched exactly, e.g. "/v1/greeter/greet".
	Path string
	// Service is the name handed to the registry. It is a lookup key and
	// nothing else — it need not equal the gRPC service name inside FullMethod,
	// and it does not whenever a provider registered itself under its own
	// convention (a jaws service is looked up as "jaws/<interface FQN>" while
	// its calls still travel as "/greeter.Greeter/…").
	Service string
	// FullMethod is the gRPC method that goes on the wire, e.g.
	// "/sweet.example.v1.Greeter/Greet".
	FullMethod string
	// ServerStream marks a method that answers many messages before closing.
	// The HTTP side then becomes newline-separated JSON objects (NDJSON,
	// RFC 8259 §2.4 style line-delimited records) with a flush after each frame,
	// so a caller sees frames as they arrive instead of at the end.
	//
	// Client streaming and bidi stay out of this package: they need a duplex
	// transport (WebSocket or SSE plus a second POST), and a half-built duplex
	// endpoint is worse than an absent one.
	ServerStream bool
	// NewRequest and NewReply construct the message types. They are functions
	// rather than values because every call needs a fresh message: one shared
	// instance would let concurrent requests decode into each other.
	NewRequest func() proto.Message
	NewReply   func() proto.Message
}

func (r Route) validate() error {
	switch {
	case r.Method == "":
		return errors.New("gateway: route needs an HTTP method")
	case !isToken(r.Method):
		return fmt.Errorf("gateway: route method %q is not an HTTP token", r.Method)
	case !strings.HasPrefix(r.Path, "/"):
		return fmt.Errorf("gateway: route path %q must start with /", r.Path)
	case r.Path != path.Clean(r.Path):
		return fmt.Errorf("gateway: route path %q is not in canonical form", r.Path)
	case r.Service == "":
		return fmt.Errorf("gateway: route %s %s needs a service name for the registry", r.Method, r.Path)
	case !strings.HasPrefix(r.FullMethod, "/"):
		return fmt.Errorf("gateway: route %s %s needs a full method like /pkg.Service/Method", r.Method, r.Path)
	case r.NewRequest == nil || r.NewReply == nil:
		return fmt.Errorf("gateway: route %s %s needs both message constructors", r.Method, r.Path)
	}
	return nil
}

// Options configures a Gateway.
type Options struct {
	// Registry resolves Route.Service. Required.
	Registry registry.Registry

	// DialOptions are applied to every connection the gateway opens.
	//
	// There is no default here on purpose. A public-facing HTTP edge that
	// silently connected to its backends in plaintext is a bug nobody notices,
	// so the caller has to say which credentials it wants.
	DialOptions []grpc.DialOption

	// RequestTimeout bounds the RPC when the incoming HTTP request carries no
	// deadline of its own. Zero means the request context is the only bound.
	//
	// It is a floor, not an override: a caller that asked for 200ms keeps its
	// 200ms, because honouring a shorter client deadline is the entire point of
	// propagating one.
	RequestTimeout time.Duration

	// AllowUnknownFields tolerates JSON properties the request message does not
	// declare. False by default, and that default is usually right: a misspelt
	// field name then fails loudly instead of being dropped on the floor. The
	// price is that a provider who adds a field cannot be served by a gateway
	// built against the older descriptor.
	AllowUnknownFields bool

	// ForwardHeaders are additional HTTP request headers copied into gRPC
	// metadata. Names are compared case-insensitively. See metadata.go for what
	// is always dropped, and why.
	ForwardHeaders []string

	// VerboseErrors surfaces the backend's status message at the edge. Off by
	// default: a gateway faces whoever can reach it, and an internal status
	// string is the information leak nobody scheduled a fix for.
	VerboseErrors bool
}

// entry is one path and all the methods registered on it — the shape the
// 405 response needs, since telling a caller what is allowed requires knowing
// the siblings of the route they missed.
type entry struct {
	byMethod map[string]Route
}

// Gateway is an http.Handler that turns matched requests into gRPC calls.
type Gateway struct {
	opts Options

	// mu guards everything below it. conns is keyed by Route.Service rather
	// than Route.Path because several HTTP endpoints usually sit on one gRPC
	// service, and a grpc.ClientConn is meant to be shared and long-lived —
	// re-dialling per request would discard the connection the resolver just
	// worked to establish.
	mu     sync.Mutex
	routes []Route
	byPath map[string]*entry
	conns  map[string]*grpc.ClientConn

	httpSrv *http.Server
	lis     net.Listener
}

// http.Handler is the contract this package is judged by: callers mount it on
// their own server, so the assertion belongs here rather than at each use.
var _ http.Handler = (*Gateway)(nil)

// New validates opts and returns a gateway with no routes. Call Handle to add
// them, then ListenAndServe or ServeHTTP.
func New(opts Options) (*Gateway, error) {
	if opts.Registry == nil {
		return nil, errors.New("gateway: Options.Registry required")
	}
	return &Gateway{
		opts:   opts,
		byPath: map[string]*entry{},
		conns:  map[string]*grpc.ClientConn{},
	}, nil
}

// Handle registers one route. Registering the same method and path twice is an
// error rather than a silent override: two routes answering one URL is a bug in
// the caller's table, and guessing which entry wins helps nobody.
func (g *Gateway) Handle(r Route) error {
	if err := r.validate(); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.byPath[r.Path]
	if e == nil {
		e = &entry{byMethod: map[string]Route{}}
		g.byPath[r.Path] = e
	}
	if _, dup := e.byMethod[r.Method]; dup {
		return fmt.Errorf("gateway: route %s %s is already registered", r.Method, r.Path)
	}
	e.byMethod[r.Method] = r
	g.routes = append(g.routes, r)
	return nil
}

// Routes returns the registered routes in registration order, so a caller can
// publish or assert the mapping table instead of scraping it out of a mux.
func (g *Gateway) Routes() []Route {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]Route(nil), g.routes...)
}

// ServeHTTP implements http.Handler, so a gateway can be mounted inside another
// server — behind a router, a middleware chain, or httptest.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	if p == "" {
		p = "/"
	}
	// Cleaned for the lookup so "/v1//greet" cannot dodge a route that
	// "/v1/greet" answers; the route table itself rejects non-canonical paths
	// at registration (see Route.validate).
	if cleaned := path.Clean(p); cleaned != p {
		p = cleaned
	}

	g.mu.Lock()
	e := g.byPath[p]
	g.mu.Unlock()
	if e == nil {
		// A -1 code number says what happened: the request never became an RPC,
		// so no gRPC status exists to report. Callers can tell a missing route
		// from a service that answered NOT_FOUND without parsing prose.
		writeLocalError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("no route for %s %s", r.Method, p), -1)
		return
	}
	route, ok := e.byMethod[r.Method]
	if !ok {
		allowed := make([]string, 0, len(e.byMethod))
		for m := range e.byMethod {
			allowed = append(allowed, m)
		}
		sort.Strings(allowed)
		// Allow is the only header set here: writeLocalError is the one that
		// writes, and doing Content-Type plus WriteHeader in this branch as
		// well produced a second WriteHeader — net/http logs that as
		// "superfluous response.WriteHeader call", a defect
		// httptest.ResponseRecorder hides and a real server does not. The
		// regression test is mounted on a real server for exactly that reason.
		w.Header().Set("Allow", strings.Join(allowed, ", "))
		writeLocalError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			fmt.Sprintf("%s is not allowed on %s; try %s", r.Method, p, strings.Join(allowed, " or ")), -1)
		return
	}
	g.serve(w, r, route)
}

// ListenAndServe binds addr and serves until Shutdown. Addr reports the bound
// address, so a caller binding ":0" can learn the real port.
func (g *Gateway) ListenAndServe(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("gateway: listen %s: %w", addr, err)
	}
	srv := &http.Server{Handler: g}

	g.mu.Lock()
	g.lis = lis
	g.httpSrv = srv
	g.mu.Unlock()

	if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Addr is the bound address, or "" before ListenAndServe.
func (g *Gateway) Addr() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lis == nil {
		return ""
	}
	return g.lis.Addr().String()
}

// Shutdown stops accepting HTTP requests and closes the gRPC connections this
// gateway opened. In-flight RPCs are cancelled with their request contexts.
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	srv, conns := g.httpSrv, g.conns
	g.httpSrv = nil
	g.conns = map[string]*grpc.ClientConn{}
	g.mu.Unlock()

	var err error
	if srv != nil {
		err = srv.Shutdown(ctx)
	}
	for _, c := range conns {
		if cerr := c.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// conn returns the shared connection for a registry lookup name, dialling on
// first use. grpc.NewClient is lazy, so a service temporarily absent from the
// registry still yields a usable connection object and the failure surfaces on
// the call — which is where the status mapping can turn it into a 503.
func (g *Gateway) conn(service string) (*grpc.ClientConn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.conns[service]; ok {
		return c, nil
	}
	c, err := client.Dial(g.opts.Registry, service, g.opts.DialOptions...)
	if err != nil {
		return nil, fmt.Errorf("gateway: dial %s: %w", service, err)
	}
	g.conns[service] = c
	return c, nil
}

// serve is the whole request path: decode, invoke, encode. Nothing in it is
// specific to any service — the lookup name, the gRPC method and the message
// types all come from the route table.
func (g *Gateway) serve(w http.ResponseWriter, r *http.Request, rt Route) {
	if rt.ServerStream {
		g.serveStream(w, r, rt)
		return
	}

	req := rt.NewRequest()
	if err := decodeRequest(r.Body, req, g.opts.AllowUnknownFields); err != nil {
		// The caller's JSON is wrong, so this is InvalidArgument by
		// construction. Letting it reach the RPC would produce a confusing
		// error from a service that never saw the bytes.
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}

	ctx, cancel := g.callContext(r)
	defer cancel()
	ctx = withMetadata(ctx, r.Header, g.opts.ForwardHeaders)

	conn, err := g.conn(rt.Service)
	if err != nil {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}

	reply := rt.NewReply()
	if err := conn.Invoke(ctx, rt.FullMethod, req, reply); err != nil {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}

	// Encode before committing headers. Marshalling a valid message can still
	// fail, and the body is the only place a 500 can still be told from a 200.
	body, err := marshalReply(reply)
	if err != nil {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// ndjsonContentType is the media type a streaming answer carries: one JSON
// object per line, no surrounding array. Declared as its own constant because a
// caller's client has to know how to split the body, and "application/json"
// would promise something else entirely.
const ndjsonContentType = "application/x-ndjson"

// serveStream answers a server-streaming method as line-delimited JSON,
// flushing after each frame so frames arrive as they are produced rather than
// at the end.
//
// The shape of the call is the one protoc-generated clients use — NewStream,
// SendMsg, CloseSend, then read until io.EOF (compare the generated
// example/greeter/greeterpb/greeter_grpc.pb.go:72-86). It passes a bare
// StreamDesc{ServerStreams: true} rather than the ServiceDesc's Streams entry:
// the client path only reads those two flags, and keeping the route table free
// of ServiceDesc means a route can be declared without importing the server
// half of the descriptor. ServerStreams is not decoration — with it false,
// grpc-go turns a stream that produced zero messages into "cardinality
// violation: received no response message from non-server-streaming RPC"
// (stream.go:1290-1298), and "the provider answered nothing" is a legitimate
// outcome a gateway should pass through as an empty body, not an internal error.
//
// Errors split on whether a frame has already been written:
//
//   - before the first frame: the HTTP status is still ours to choose, so the
//     failure is reported like any other failure (400/404/503…) with the normal
//     error body;
//   - after the first frame: 200 has been committed, so the terminal error
//     travels as one more JSON line — {"error":{…}} — and the stream ends there.
//
// HTTP trailers would be the protocol-native place for that, but a trailer is
// invisible to curl without --trailer and unreadable from a browser's fetch
// body, so the failure would silently look like a successful truncated stream.
// The in-band key is "error", which is the one collision risk this encoding has:
// a reply message with a top-level field named error is indistinguishable from a
// terminal line. Naming that field something else in the IDL is the fix; the
// alternative — wrapping every frame — would change the shape of the success
// path to serve a rare failure.
func (g *Gateway) serveStream(w http.ResponseWriter, r *http.Request, rt Route) {
	req := rt.NewRequest()
	if err := decodeRequest(r.Body, req, g.opts.AllowUnknownFields); err != nil {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}

	ctx, cancel := g.callContext(r)
	defer cancel()
	ctx = withMetadata(ctx, r.Header, g.opts.ForwardHeaders)

	conn, err := g.conn(rt.Service)
	if err != nil {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, rt.FullMethod)
	if err != nil {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}
	if err := stream.SendMsg(req); err != nil && !errors.Is(err, io.EOF) {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}
	if err := stream.CloseSend(); err != nil {
		writeStatus(w, err, g.opts.VerboseErrors)
		return
	}

	// A ResponseWriter that cannot flush still works — the body arrives, just
	// buffered. Not asserting here keeps the gateway usable behind wrappers that
	// do not forward Flush, at the cost of the real-time property.
	flusher, _ := w.(http.Flusher)
	committed := false

	for {
		frame := rt.NewReply()
		err := stream.RecvMsg(frame)
		if err == nil {
			data, merr := marshalReply(frame)
			if merr != nil {
				if !committed {
					writeStatus(w, merr, g.opts.VerboseErrors)
					return
				}
				writeInBandError(w, flusher, merr, g.opts.VerboseErrors)
				return
			}
			if !committed {
				w.Header().Set("Content-Type", ndjsonContentType)
				w.WriteHeader(http.StatusOK)
				committed = true
			}
			if _, werr := w.Write(append(data, '\n')); werr != nil {
				// The caller went away. Returning lets the deferred cancel send
				// RST_STREAM, which is the point: the provider stops working.
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			continue
		}

		if errors.Is(err, io.EOF) {
			// Successful end of stream. Zero frames is still a success, so the
			// headers get written and the body stays empty.
			if !committed {
				w.Header().Set("Content-Type", ndjsonContentType)
				w.WriteHeader(http.StatusOK)
			}
			return
		}

		if !committed {
			writeStatus(w, err, g.opts.VerboseErrors)
			return
		}
		writeInBandError(w, flusher, err, g.opts.VerboseErrors)
		return
	}
}

// callContext derives the RPC context from the HTTP request, so a client that
// hangs up cancels the RPC instead of leaving the provider working on a
// response nobody will read. RequestTimeout only supplies a bound when the
// caller gave the request none.
func (g *Gateway) callContext(r *http.Request) (context.Context, context.CancelFunc) {
	if _, ok := r.Context().Deadline(); ok {
		// The caller already set a bound; pass their context through untouched.
		return r.Context(), func() {}
	}
	if g.opts.RequestTimeout <= 0 {
		return r.Context(), func() {}
	}
	return context.WithTimeout(r.Context(), g.opts.RequestTimeout)
}

// isToken reports whether s is a valid HTTP token (RFC 9110 §5.6.2), which is
// what a method name has to be.
func isToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		if strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0 {
			continue
		}
		return false
	}
	return true
}
