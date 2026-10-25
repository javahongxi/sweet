// Package server wraps a grpc-go server with sweet's lifecycle: listen,
// expose to a registry, serve, and stop in the order that keeps in-flight
// calls from hitting a dead address.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/javahongxi/sweet/registry"
)

// Options configures a Server.
type Options struct {
	// Address to listen on. ":0" picks a free port; the chosen port is
	// reflected in the registered instance.
	Address string
	// ServiceName is the key peers look up. If empty it must be supplied by
	// every Register call's service descriptor name.
	ServiceName string
	// Weight is advertised to the load balancer through instance metadata.
	Weight int
	// Metadata are registry-specific attributes (group, cluster, version, ...).
	Metadata map[string]string
	// Registry carries this instance to peers. Required.
	Registry registry.Registry
	// AdvertiseHost overrides the address reported to the registry, e.g. when
	// listening on 0.0.0.0 behind a known hostname.
	AdvertiseHost string
	// ServerOptions are passed to grpc.NewServer.
	ServerOptions []grpc.ServerOption
	// DrainDelay is how long Stop waits after deregistering before it stops
	// accepting. It covers the window in which peers have not yet applied the
	// removal. Zero means no extra wait.
	DrainDelay time.Duration
}

// Server is a sweet-managed gRPC server. Serve runs on the caller's goroutine
// or in a goroutine of the caller's choosing, so everything it publishes is
// guarded by mu; Listen reports when that state is safe to read.
type Server struct {
	opts Options
	grpc *grpc.Server

	mu      sync.Mutex
	lis     net.Listener
	host    string
	port    int
	unreg   func()
	started bool
	stopped bool

	ready chan struct{} // closed once lis/host/port are populated
	fail  chan error    // closed with a non-nil value if the listener cannot bind
}

// New validates opts and creates the underlying grpc.Server. Call Register on
// it to attach service implementations, then Serve.
func New(opts Options) (*Server, error) {
	switch {
	case opts.Address == "":
		return nil, errors.New("server: Options.Address required")
	case opts.Registry == nil:
		return nil, errors.New("server: Options.Registry required")
	}
	if opts.Weight == 0 {
		opts.Weight = 100
	}
	return &Server{opts: opts, grpc: grpc.NewServer(opts.ServerOptions...), ready: make(chan struct{}), fail: make(chan error, 1)}, nil
}

// GRPCServer exposes the underlying server for service registration with
// protoc-gen-go-grpc generated code.
func (s *Server) GRPCServer() *grpc.Server { return s.grpc }

// Register attaches sd/ss and, when Options.ServiceName is empty, requires the
// service to be the only one so its descriptor name can be used.
func (s *Server) Register(sd *grpc.ServiceDesc, ss any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grpc.RegisterService(sd, ss)
	if s.opts.ServiceName == "" {
		s.opts.ServiceName = sd.ServiceName
	}
	return nil
}

// Listen blocks until the listener is bound and exposed, and reports the
// binding error if Serve could not start. Call it before Addr or Port.
func (s *Server) Listen() error {
	select {
	case <-s.ready:
		return nil
	case err := <-s.fail:
		return err
	}
}

// Addr is the listening address; valid after Listen returns nil.
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lis == nil {
		return ""
	}
	return s.lis.Addr().String()
}

// Port is the listening port; valid after Listen returns nil.
func (s *Server) Port() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.port
}

// DrainDelay is the configured grace period Stop waits after deregistering.
func (s *Server) DrainDelay() time.Duration { return s.opts.DrainDelay }

// ServiceName is the name this instance is exposed under.
func (s *Server) ServiceName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opts.ServiceName
}

// Serve listens, exposes the instance, and blocks until Stop is called.
//
// Registration happens before the listener starts accepting, so a peer can
// never resolve an address that is not yet ready to serve.
func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", s.opts.Address)
	if err != nil {
		err = fmt.Errorf("server: listen %s: %w", s.opts.Address, err)
		s.fail <- err
		return err
	}
	host, port, err := resolveAdvertise(lis.Addr().String(), s.opts.AdvertiseHost)
	if err != nil {
		s.fail <- err
		return err
	}

	s.mu.Lock()
	s.lis = lis
	s.host, s.port = host, port
	name := s.opts.ServiceName
	s.mu.Unlock()

	if name == "" {
		err = errors.New("server: no service name; set Options.ServiceName or Register exactly one service")
		s.fail <- err
		return err
	}
	inst := registry.Instance{Host: host, Port: port, Weight: s.opts.Weight, Metadata: s.opts.Metadata}
	unreg, err := s.opts.Registry.Expose(context.Background(), name, inst)
	if err != nil {
		err = fmt.Errorf("server: expose %s: %w", name, err)
		s.fail <- err
		return err
	}

	s.mu.Lock()
	s.unreg = unreg
	s.started = true
	s.mu.Unlock()
	close(s.ready)

	return s.grpc.Serve(lis)
}

// Stop deregisters first, waits out the drain window, then stops the gRPC
// server gracefully. Safe to call once.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped || !s.started {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	unreg := s.unreg
	s.mu.Unlock()

	if unreg != nil {
		unreg()
	}
	if s.opts.DrainDelay > 0 {
		time.Sleep(s.opts.DrainDelay)
	}
	s.grpc.GracefulStop()
}

func resolveAdvertise(listenAddr, override string) (string, int, error) {
	if override != "" {
		host, port, err := net.SplitHostPort(override)
		if err != nil {
			return "", 0, fmt.Errorf("server: AdvertiseHost %q: %w", override, err)
		}
		return host, atoi(port), nil
	}
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", 0, fmt.Errorf("server: listen address %q: %w", listenAddr, err)
	}
	if host == "" || host == "::" || host == "[::]" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return host, atoi(port), nil
}
