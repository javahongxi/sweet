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

	"github.com/javahongxi/sweet/client"
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
func TestNativeClientCallsSweetServer(t *testing.T) {
	srv, err := server.New(server.Options{
		// ":0" and let Serve bind it: pre-binding a listener and handing its
		// address to Serve would ask for the same port twice.
		Address:     "127.0.0.1:0",
		ServiceName: healthService,
		Registry:    direct.Nope(), // this leg measures the RPC plane, not discovery
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	hs := health.NewServer()
	srv.GRPCServer().RegisterService(&healthpb.Health_ServiceDesc, hs)
	hs.SetServingStatus(healthService, healthpb.HealthCheckResponse_SERVING)

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
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: healthService})
	if err != nil {
		t.Fatalf("native client → sweet server: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v, want SERVING", resp.GetStatus())
	}
	t.Logf("stock grpc-go client reached sweet's server: %v", resp.GetStatus())
}

// TestNativeServerAcceptsGeneratedStreamViaSweet: streaming must survive the
// same way, since Health.Watch is the generated server-streaming method.
func TestNativeServerAcceptsGeneratedStreamViaSweet(t *testing.T) {
	srv, err := server.New(server.Options{
		Address:     "127.0.0.1:0",
		ServiceName: healthService,
		Registry:    direct.Nope(),
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	hs := health.NewServer()
	srv.GRPCServer().RegisterService(&healthpb.Health_ServiceDesc, hs)
	hs.SetServingStatus(healthService, healthpb.HealthCheckResponse_SERVING)
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
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{Service: healthService})
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
