// Package interoptest measures sweet against a stack we do not control: the
// stock grpc-go health service and its generated stubs. A test that only ever
// pairs sweet with sweet proves internal consistency, nothing more.
package interoptest

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/example/helloworld/ping"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
	"github.com/javahongxi/sweet/server"
)

// healthService is the real gRPC name of the stock health service, and the
// same string is used as the service name in the registry: a framework that
// rewrites either one would break here.
const healthService = "grpc.health.v1.Health"

// TestNativeClientCallsSweetServer: a stock grpc-go client, with no sweet code
// on its side, must reach a service registered on sweet's server.
//
// The service it reaches is sweet's own health service — registered by the
// framework, not by this test — so a SERVING answer here is also evidence that
// the framework flipped the status once Serve had bound and exposed.
func TestNativeClientCallsSweetServer(t *testing.T) {
	srv, err := server.New(server.Options{
		// ":0" and let Serve bind it: pre-binding a listener and handing its
		// address to Serve would ask for the same port twice.
		Address:  "127.0.0.1:0",
		Registry: direct.Nope(), // this leg measures the RPC plane, not discovery
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Register(&ping.ServiceDesc, pingServer{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	go func() { _ = srv.Serve() }()
	if err := srv.Listen(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer srv.Stop()

	conn, err := grpc.NewClient(srv.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("native dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The overall service, and the registered one by name: the second is what
	// proves sweet tracked Register rather than just reporting "I am up".
	for _, name := range []string{"", ping.ServiceName} {
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: name})
		if err != nil {
			t.Fatalf("native client → sweet server (service %q): %v", name, err)
		}
		if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("service %q status = %v, want SERVING", name, resp.GetStatus())
		}
		t.Logf("stock grpc-go client reached sweet's server: %q → %v", name, resp.GetStatus())
	}

	// Guard for how health and reflection are wired: they are registered straight
	// on the grpc.Server, so they must not be mistaken for the instance's service
	// name when Options.ServiceName is left to Register's descriptor.
	if got := srv.ServiceName(); got != ping.ServiceName {
		t.Fatalf("ServiceName() = %q, want %q — a built-in service leaked into the inference", got, ping.ServiceName)
	}
}

// pingServer is the smallest thing that satisfies the hand-written ping service:
// these tests only need a registered descriptor name to exist.
type pingServer struct{}

func (pingServer) Ping(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String("pong"), nil
}

func (pingServer) Events(*wrapperspb.StringValue, grpc.ServerStreamingServer[wrapperspb.StringValue]) error {
	return nil
}

func (pingServer) Echo(grpc.BidiStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error {
	return nil
}

func (pingServer) Ack(grpc.ClientStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error {
	return nil
}

// TestReflectionListsRegisteredService: reflection is built in, so a standard
// tool with no sweet code in it can enumerate what an instance serves — the
// grpcurl equivalent is measured in the README.
func TestReflectionListsRegisteredService(t *testing.T) {
	srv, err := server.New(server.Options{
		Address:  "127.0.0.1:0",
		Registry: direct.Nope(),
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Register(&ping.ServiceDesc, pingServer{}); err != nil {
		t.Fatalf("register: %v", err)
	}
	go func() { _ = srv.Serve() }()
	if err := srv.Listen(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer srv.Stop()

	conn, err := grpc.NewClient(srv.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("native dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rc, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatalf("ServerReflectionInfo: %v", err)
	}
	if err := rc.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: "*"},
	}); err != nil {
		t.Fatalf("ListServices send: %v", err)
	}
	got, err := rc.Recv()
	if err != nil {
		t.Fatalf("ListServices recv: %v", err)
	}
	var names []string
	for _, s := range got.GetListServicesResponse().GetService() {
		names = append(names, s.GetName())
	}
	t.Logf("reflection lists %d services: %v", len(names), names)
	for _, want := range []string{ping.ServiceName, healthService} {
		found := false
		for _, n := range names {
			if n == want {
				found = true
			}
		}
		if !found {
			t.Fatalf("reflection did not list %q; got %v", want, names)
		}
	}
}

// TestNativeServerAcceptsGeneratedStreamViaSweet: streaming must survive the
// same way, since Health.Watch is the generated server-streaming method. The
// health service watched here is the one sweet registers itself.
func TestNativeServerAcceptsGeneratedStreamViaSweet(t *testing.T) {
	srv, err := server.New(server.Options{
		Address:     "127.0.0.1:0",
		ServiceName: healthService,
		Registry:    direct.Nope(),
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	go func() { _ = srv.Serve() }()
	if err := srv.Listen(); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	defer srv.Stop()

	reg, err := direct.New(srv.Addr())
	if err != nil {
		t.Fatalf("direct registry: %v", err)
	}
	conn, err := client.Dial(reg, healthService,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("sweet dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("Watch via sweet resolver: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Watch recv: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("watched status = %v", resp.GetStatus())
	}
	t.Logf("generated server-streaming method works through sweet's resolver")
}

// TestSweetClientDiscoversNativeServer is the one that matters most: a stock
// grpc-go server, published by a third party, must be findable by service name
// and callable with the stock stub — i.e. sweet's discovery and its wire
// behaviour do not invent anything.
func TestSweetClientDiscoversNativeServer(t *testing.T) {
	addr := os.Getenv("NACOS_GRPC_ADDR")
	if addr == "" {
		t.Skip("set NACOS_GRPC_ADDR=127.0.0.1:9848")
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hs := health.NewServer()
	native := grpc.NewServer()
	native.RegisterService(&healthpb.Health_ServiceDesc, hs)
	hs.SetServingStatus(healthService, healthpb.HealthCheckResponse_SERVING)
	go func() { _ = native.Serve(lis) }()
	defer native.Stop()

	// A *provider* written with the stock stub, published through sweet's
	// registry: the discovery plane is what is under test, not the peer.
	providerReg, err := nacosregistry.New(nacosregistry.Options{ServerAddr: addr})
	if err != nil {
		t.Fatalf("provider registry: %v", err)
	}
	defer providerReg.Close()
	host, port := split(t, lis.Addr().String())
	unreg, err := providerReg.Expose(context.Background(), healthService,
		registry.Instance{Host: host, Port: port, Weight: 100})
	if err != nil {
		t.Fatalf("publish native server: %v", err)
	}
	defer unreg()

	consumerReg, err := nacosregistry.New(nacosregistry.Options{ServerAddr: addr})
	if err != nil {
		t.Fatalf("consumer registry: %v", err)
	}
	defer consumerReg.Close()
	conn, err := client.Dial(consumerReg, healthService,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("sweet dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var last error
	for time.Now().Before(deadline(ctx)) {
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: healthService})
		if err == nil {
			t.Logf("sweet client reached the stock grpc-go server via Nacos discovery: %v", resp.GetStatus())
			return
		}
		last = err
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("never reached the native server: %v", last)
}

func deadline(ctx context.Context) time.Time {
	d, ok := ctx.Deadline()
	if !ok {
		return time.Now().Add(5 * time.Second)
	}
	return d
}

func split(t *testing.T, hostport string) (string, int) {
	t.Helper()
	for i := len(hostport) - 1; i >= 0; i-- {
		if hostport[i] == ':' {
			var port int
			for _, c := range hostport[i+1:] {
				port = port*10 + int(c-'0')
			}
			return hostport[:i], port
		}
	}
	t.Fatalf("bad address %q", hostport)
	return "", 0
}
