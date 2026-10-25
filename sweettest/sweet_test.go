package sweettest

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	"github.com/javahongxi/sweet/server"
)

// svcName is a service that exists only here: the ServiceDesc below is written
// by hand, so the test proves sweet needs no protoc-generated stub to carry a
// real gRPC service.
const svcName = "sweet.example.v1.Ping"

type pingServer struct{}

func (pingServer) Ping(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if in.GetValue() == "" {
		return nil, errors.New("empty ping")
	}
	return wrapperspb.String("pong"), nil
}

func (pingServer) Events(in *wrapperspb.StringValue, stream grpc.ServerStreamingServer[wrapperspb.StringValue]) error {
	for _, e := range strings.Split(in.GetValue(), ",") {
		if err := stream.Send(wrapperspb.String(e)); err != nil {
			return err
		}
	}
	return nil
}

var pingDesc = grpc.ServiceDesc{
	ServiceName: svcName,
	HandlerType: (*any)(nil),
	Metadata:    "sweet/example/ping.proto",
	Methods: []grpc.MethodDesc{{
		MethodName: "Ping",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.(pingServer).Ping(ctx, in)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Events",
		ServerStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			in := new(wrapperspb.StringValue)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			return srv.(pingServer).Events(in,
				&grpc.GenericServerStream[wrapperspb.StringValue, wrapperspb.StringValue]{ServerStream: stream})
		},
	}},
}

// ---- live registry: instances may come and go while tests run ----

type fakeRegistry struct {
	mu       sync.Mutex
	name     string
	insts    []registry.Instance
	watchers map[chan []registry.Instance]chan struct{}
	lastSeen []registry.Instance
}

func newFakeRegistry(scheme string) *fakeRegistry {
	return &fakeRegistry{name: scheme, watchers: map[chan []registry.Instance]chan struct{}{}}
}

func (f *fakeRegistry) Name() string { return f.name }

func (f *fakeRegistry) Expose(_ context.Context, _ string, in registry.Instance) (func(), error) {
	f.mu.Lock()
	f.insts = append(f.insts, in)
	f.publishLocked()
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		out := f.insts[:0]
		for _, x := range f.insts {
			if x.Addr() != in.Addr() {
				out = append(out, x)
			}
		}
		f.insts = out
		f.publishLocked()
		f.mu.Unlock()
	}, nil
}

func (f *fakeRegistry) publishLocked() {
	snap := append([]registry.Instance(nil), f.insts...)
	f.lastSeen = snap
	for ch := range f.watchers {
		select {
		case ch <- snap:
		default:
		}
	}
}

func (f *fakeRegistry) Watch(ctx context.Context, _ string) (<-chan []registry.Instance, error) {
	ch := make(chan []registry.Instance, 4)
	quit := make(chan struct{})
	f.mu.Lock()
	f.watchers[ch] = quit
	snap := append([]registry.Instance(nil), f.insts...)
	f.mu.Unlock()
	if len(snap) > 0 {
		ch <- snap
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-quit:
		}
		// Publishing is done under the same lock that guards removal, so no
		// send can be in flight once we close.
		f.mu.Lock()
		delete(f.watchers, ch)
		f.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

func (f *fakeRegistry) exposed() []registry.Instance {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]registry.Instance(nil), f.insts...)
}

func (f *fakeRegistry) resolved() []registry.Instance {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]registry.Instance(nil), f.lastSeen...)
}

// ---- helpers ----

func startServer(t *testing.T, reg registry.Registry) *server.Server {
	t.Helper()
	srv, err := server.New(server.Options{
		Address:     "127.0.0.1:0",
		ServiceName: svcName,
		Registry:    reg,
		DrainDelay:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Register(&pingDesc, pingServer{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	go func() { _ = srv.Serve() }()
	if err := srv.Listen(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	return srv
}

func callPair(t *testing.T, conn *grpc.ClientConn, ctx context.Context) (string, []string) {
	t.Helper()
	in := wrapperspb.String("hi")
	out := new(wrapperspb.StringValue)
	if err := conn.Invoke(ctx, "/"+svcName+"/Ping", in, out); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	cs, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/"+svcName+"/Events")
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	stream := &grpc.GenericClientStream[wrapperspb.StringValue, wrapperspb.StringValue]{ClientStream: cs}
	if err := stream.Send(wrapperspb.String("a,b,c")); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	var got []string
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		got = append(got, m.GetValue())
	}
	return out.GetValue(), got
}

// ---- S1 acceptance ----

// TestServeDialOverDirectRegistry: sweet's server and sweet's client talk end
// to end through sweet's resolver, with no registry server and no generated
// stubs. direct.Registry is the stand-in for "no Nacos needed to develop".
func TestServeDialOverDirectRegistry(t *testing.T) {
	srv := startServer(t, newFakeRegistry("nacos"))
	defer srv.Stop()

	reg, err := direct.New(srv.Addr())
	if err != nil {
		t.Fatalf("direct.New: %v", err)
	}
	conn, err := client.Dial(reg, srv.ServiceName(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pong, events := callPair(t, conn, ctx)
	if pong != "pong" {
		t.Fatalf("Ping = %q, want pong", pong)
	}
	if strings.Join(events, "|") != "a|b|c" {
		t.Fatalf("Events = %v, want a|b|c", events)
	}
}

// TestInstanceChangesReachClient proves Watch → UpdateState is live: a provider
// that appears after Dial becomes callable, and one that leaves stops being
// advertised, without re-dialling.
func TestInstanceChangesReachClient(t *testing.T) {
	reg := newFakeRegistry("nacos")
	srvA := startServer(t, reg)
	defer srvA.Stop()

	conn, err := client.Dial(reg, svcName,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if pong, _ := callPair(t, conn, ctx); pong != "pong" {
		t.Fatalf("first provider: pong = %q", pong)
	}

	srvB := startServer(t, reg)
	waitFor(t, 5*time.Second, func() bool { return len(reg.exposed()) == 2 })
	if got := len(reg.resolved()); got != 2 {
		t.Fatalf("registry published %d instances to the resolver, want 2", got)
	}
	if pong, _ := callPair(t, conn, ctx); pong != "pong" {
		t.Fatalf("after scale-up: pong = %q", pong)
	}

	srvB.Stop()
	waitFor(t, 5*time.Second, func() bool { return len(reg.exposed()) == 1 })
	if got := len(reg.resolved()); got != 1 {
		t.Fatalf("registry published %d instances after a provider left, want 1", got)
	}
}

// TestStopDeregistersFirst pins the order graceful-offline depends on: the
// instance leaves the registry before the listener stops.
func TestStopDeregistersFirst(t *testing.T) {
	reg := newFakeRegistry("nacos")
	srv := startServer(t, reg)
	if got := len(reg.exposed()); got != 1 {
		t.Fatalf("exposed instances = %d, want 1", got)
	}
	srv.Stop()
	if got := len(reg.exposed()); got != 0 {
		t.Fatalf("after Stop, exposed instances = %d, want 0", got)
	}
}

// TestDirectRegistryFeed covers direct.Registry's own contract: a fixed list
// delivered once and then closed, and Expose acknowledged without publishing.
func TestDirectRegistryFeed(t *testing.T) {
	reg, err := direct.New("127.0.0.1:12345,127.0.0.1:12346")
	if err != nil {
		t.Fatalf("direct.New: %v", err)
	}
	if _, err := reg.Expose(context.Background(), svcName, registry.Instance{}); err == nil {
		t.Fatal("direct.Expose should reject an instance with no usable address")
	}
	unreg, err := reg.Expose(context.Background(), svcName, registry.Instance{Host: "127.0.0.1", Port: 12345})
	if err != nil {
		t.Fatalf("direct.Expose: %v", err)
	}
	if got := reg.Exposed(svcName); got != 1 {
		t.Fatalf("Exposed = %d, want 1", got)
	}
	unreg()
	unreg() // idempotent
	if got := reg.Exposed(svcName); got != 0 {
		t.Fatalf("Exposed after double unreg = %d, want 0", got)
	}
	ch, err := reg.Watch(context.Background(), svcName)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	insts, ok := <-ch
	if !ok || len(insts) != 2 {
		t.Fatalf("Watch returned %d instances (ok=%v), want 2", len(insts), ok)
	}
	if _, ok := <-ch; ok {
		t.Fatal("direct.Watch should close after the fixed snapshot")
	}
}

// TestServerUpAndDialledThroughDirect is the claim the README makes: with the
// direct registry a server comes up and a client reaches it, no Nacos needed.
// S1 only proved this against a live fake registry, so the shipped path was
// untested — and direct.Registry used to refuse Expose, which broke exactly
// this demo.
func TestServerUpAndDialledThroughDirect(t *testing.T) {
	srv := startServer(t, newFakeRegistry("nacos"))
	defer srv.Stop()

	reg, err := direct.New(srv.Addr())
	if err != nil {
		t.Fatalf("direct.New: %v", err)
	}
	if got := reg.Exposed(svcName); got != 0 {
		t.Fatalf("direct registry should not have seen the fake registry's server, got %d", got)
	}

	conn, err := client.Dial(reg, srv.ServiceName(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pong, events := callPair(t, conn, ctx)
	if pong != "pong" || strings.Join(events, "|") != "a|b|c" {
		t.Fatalf("got pong=%q events=%v", pong, events)
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}
