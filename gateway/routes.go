package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// RouteSpec is the data form of a Route. Method types come from explicit
// Options.Services overrides, DescriptorFiles, or compiled protobuf descriptors;
// the route config itself never names a Go type.
type RouteSpec struct {
	Method       string  `json:"method"`
	Path         string  `json:"path"`
	Service      string  `json:"service"`
	FullMethod   string  `json:"fullMethod"`
	ServerStream bool    `json:"serverStream,omitempty"`
	Body         *string `json:"body,omitempty"`
	ResponseBody string  `json:"responseBody,omitempty"`
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
func (g *Gateway) resolve(s RouteSpec, files *protoregistry.Files) (Route, error) {
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
	var mt MessageTypes
	serverStream := s.ServerStream
	if types, ok := g.opts.Services[service]; ok {
		var found bool
		mt, found = types[method]
		if !found {
			names := make([]string, 0, len(types))
			for m := range types {
				names = append(names, m)
			}
			return Route{}, fmt.Errorf("gateway: route %s %s: method %q of service %q is not registered (known: %s)",
				s.Method, s.Path, method, service, strings.Join(names, ", "))
		}
	} else {
		var err error
		mt, serverStream, err = resolveMethod(files, service, method)
		if err != nil {
			return Route{}, fmt.Errorf("gateway: route %s %s: %w", s.Method, s.Path, err)
		}
		serverStream = serverStream || s.ServerStream
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
		ServerStream: serverStream,
		Body:         s.Body,
		ResponseBody: s.ResponseBody,
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
	files, err := loadDescriptorFiles(g.opts.DescriptorFiles)
	if err != nil {
		return err
	}
	routes := make([]Route, 0, len(specs))
	byPath := make(map[string]*entry, len(specs))
	patterns := make([]Route, 0)
	seenTemplates := make(map[string]struct{})
	for _, s := range specs {
		r, err := g.resolve(s, files)
		if err != nil {
			return err
		}
		if r.pattern.templated() {
			key := r.Method + " " + r.pattern.key()
			if _, dup := seenTemplates[key]; dup {
				return fmt.Errorf("gateway: equivalent route template %s %s appears twice in one table", r.Method, r.Path)
			}
			seenTemplates[key] = struct{}{}
			patterns = append(patterns, r)
			routes = append(routes, r)
			continue
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
	sort.SliceStable(patterns, func(i, j int) bool {
		return patterns[i].pattern.literals > patterns[j].pattern.literals
	})

	g.mu.Lock()
	g.byPath = byPath
	g.patterns = patterns
	g.routes = routes
	g.mu.Unlock()
	// Only after the swap: the cache must never hold a table this gateway refused.
	g.cacheRoutes(specs)
	return nil
}
