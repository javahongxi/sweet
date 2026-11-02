package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"google.golang.org/protobuf/proto"
)

// RouteSpec is the data form of a Route: everything a route table can say
// without naming a Go type. The message constructors stay in Go, registered
// per service in Options.Services, because a config file cannot carry a
// func() proto.Message and pretending otherwise would mean generating or
// discovering types at runtime — which is the reflection route this package
// deliberately does not take (see doc/nacos-config-routes.md, 不做 #2).
type RouteSpec struct {
	Method       string `json:"method"`
	Path         string `json:"path"`
	Service      string `json:"service"`
	FullMethod   string `json:"fullMethod"`
	ServerStream bool   `json:"serverStream,omitempty"`
}

// MessageTypes constructs the two messages of one gRPC method. Fresh instances
// per call, for the reason spelled out on Route.NewRequest.
type MessageTypes struct {
	Request func() proto.Message
	Reply   func() proto.Message
}

// ServiceTypes maps gRPC method names (the part after the "/" in
// Route.FullMethod, e.g. "Greet") to their message types.
type ServiceTypes map[string]MessageTypes

// ParseRouteSpecs decodes a route table: {"routes":[…]}.
//
// Unknown JSON keys are rejected at any depth, not just the envelope
// (json.Decoder.DisallowUnknownFields applies to every object the decoder
// walks). A table is read by an operator under pressure, and a silently
// dropped "serverStreams": true (one letter off) produces a gateway that
// answers 400 on a streaming method — which reads like a backend bug, not like
// a typo in a config file.
func ParseRouteSpecs(data []byte) ([]RouteSpec, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("gateway: empty route table")
	}
	var doc struct {
		Routes []RouteSpec `json:"routes"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("gateway: decode route table: %w", err)
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return nil, errors.New("gateway: route table has trailing content")
	}
	// An empty table is refused on purpose. "Delete the config" and "route
	// nothing" are different intentions, and the edge going dark because a
	// dataId was removed is the more expensive mistake of the two.
	if len(doc.Routes) == 0 {
		return nil, errors.New("gateway: route table has no routes; refusing to install an empty one")
	}
	return doc.Routes, nil
}

// resolve turns one spec into a Route, binding it to the registered message
// types. Every failure is an error naming the spec, so a bad line in a table of
// forty is diagnosable from the log alone.
func (g *Gateway) resolve(s RouteSpec) (Route, error) {
	if s.Method != "" {
		// net/http hands r.Method to ServeHTTP upper-cased, so a table written
		// with "post" would never match. Normalising here is the difference
		// between a silent 404 and a route that works.
		s.Method = strings.ToUpper(s.Method)
	}
	service, method, err := splitFullMethod(s.FullMethod)
	if err != nil {
		return Route{}, fmt.Errorf("gateway: route %s %s: %w", s.Method, s.Path, err)
	}
	types, ok := g.opts.Services[service]
	if !ok {
		return Route{}, fmt.Errorf("gateway: route %s %s: gRPC service %q is not registered in Options.Services",
			s.Method, s.Path, service)
	}
	mt, ok := types[method]
	if !ok {
		names := make([]string, 0, len(types))
		for m := range types {
			names = append(names, m)
		}
		return Route{}, fmt.Errorf("gateway: route %s %s: method %q of service %q is not registered (known: %s)",
			s.Method, s.Path, method, service, strings.Join(names, ", "))
	}
	if mt.Request == nil || mt.Reply == nil {
		return Route{}, fmt.Errorf("gateway: route %s %s: service %q method %q has no message constructors",
			s.Method, s.Path, service, method)
	}
	r := Route{
		Method:       s.Method,
		Path:         s.Path,
		Service:      s.Service,
		FullMethod:   s.FullMethod,
		ServerStream: s.ServerStream,
		NewRequest:   mt.Request,
		NewReply:     mt.Reply,
	}
	if err := r.validate(); err != nil {
		return Route{}, err
	}
	return r, nil
}

// splitFullMethod parses "/pkg.Service/Method", the form gRPC puts on the wire.
func splitFullMethod(full string) (service, method string, err error) {
	if !strings.HasPrefix(full, "/") {
		return "", "", fmt.Errorf("fullMethod %q must start with /", full)
	}
	rest := full[1:]
	i := strings.IndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return "", "", fmt.Errorf("fullMethod %q must look like /pkg.Service/Method", full)
	}
	if strings.Contains(rest[i+1:], "/") {
		return "", "", fmt.Errorf("fullMethod %q has more than one /", full)
	}
	return rest[:i], rest[i+1:], nil
}

// ApplyRoutes replaces the whole route table with the given specs.
//
// The swap is all-or-nothing: the new table is built and validated first, and
// any bad spec returns an error with the live table untouched. Half-installed
// routes are the failure mode an operator cannot reason about — a path that
// answers while its sibling does not looks like a backend outage.
//
// It replaces routes added by Handle as well; a gateway whose table comes from a
// config source should get its whole table from that source.
//
// gRPC connections opened for services that leave the table are kept until
// Shutdown. They are idle rather than leaked — a grpc.ClientConn is designed to
// be long-lived, and closing one because a route moved would penalise the next
// caller with a cold reconnect.
func (g *Gateway) ApplyRoutes(specs []RouteSpec) error {
	routes := make([]Route, 0, len(specs))
	byPath := make(map[string]*entry, len(specs))
	for _, s := range specs {
		r, err := g.resolve(s)
		if err != nil {
			return err
		}
		e := byPath[r.Path]
		if e == nil {
			e = &entry{byMethod: map[string]Route{}}
			byPath[r.Path] = e
		}
		if _, dup := e.byMethod[r.Method]; dup {
			return fmt.Errorf("gateway: route %s %s appears twice in one table", r.Method, r.Path)
		}
		e.byMethod[r.Method] = r
		routes = append(routes, r)
	}

	g.mu.Lock()
	g.byPath = byPath
	g.routes = routes
	g.mu.Unlock()
	// Only after the swap: the cache must never hold a table this gateway refused.
	g.cacheRoutes(specs)
	return nil
}
