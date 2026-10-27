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
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

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

	// health is the stock gRPC health service, and reflection is registered
	// beside it. Both live inside grpc-go, so neither is a second dependency,
	// and both are wired unconditionally: an instance that peers can resolve
	// should also be an instance a standard tool can inspect and probe without
	// the framework's own code on the other side.
	health *health.Server
	names  []string // descriptor names passed to Register, for health status

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
	s := &Server{
		opts:   opts,
		grpc:   grpc.NewServer(opts.ServerOptions...),
		health: health.NewServer(),
		ready:  make(chan struct{}),
		fail:   make(chan error, 1),
	}
	// Registered straight on the grpc.Server, never through Register: the latter
	// adopts the descriptor name as this instance's service name when
	// Options.ServiceName is empty, and the last thing a peer should resolve is
	// a health probe.
	s.grpc.RegisterService(&healthpb.Health_ServiceDesc, s.health)
	reflection.Register(s.grpc)
	// health.NewServer starts the overall service at SERVING (see
	// grpc/health/server.go). Nothing is listening yet, so correct that: the
	// status moves to SERVING only when Serve has bound the listener and
	// exposed the instance, and back to NOT_SERVING at the start of Stop.
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	return s, nil
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
	s.names = append(s.names, sd.ServiceName)
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
	names := append([]string(nil), s.names...)
	s.mu.Unlock()

	// Statuses move to SERVING before peers are told the instance exists, and
	// this still happens before grpc.Serve starts accepting, so there is no
	// window in which a probe could observe SERVING for an address that refuses
	// connections — nor NOT_SERVING for one that is already answering.
	s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	for _, name := range names {
		s.health.SetServingStatus(name, healthpb.HealthCheckResponse_SERVING)
	}
	close(s.ready)

	return s.grpc.Serve(lis)
}

// Stop marks the instance unhealthy first, deregisters, waits out the drain
// window, then stops the gRPC server gracefully. Safe to call once.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.stopped || !s.started {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	unreg := s.unreg
	s.mu.Unlock()

	// Shutdown flips every status — the overall one and each registered service
	// — to NOT_SERVING and keeps them there. It comes before the deregistration
	// so that a probe-based router is never *later* to learn about the shutdown
	// than the registry is; both say "stop sending" in the same order.
	s.health.Shutdown()

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
