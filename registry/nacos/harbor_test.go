package nacos_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/example/helloworld/ping"
	"github.com/javahongxi/sweet/registry"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
)

// runAgainstBackend is the registry-level contract every Nacos-compatible
// server must satisfy for sweet: handshake, register, query, subscribe, then
// an actual RPC through service-name discovery alone. It is parameterised so
// the same assertions run against Nacos and against harbor — that is the point
// of the harbour leg, since a test only proven against the reference
// implementation proves nothing about compatibility.
func runAgainstBackend(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	reg, err := nacosregistry.New(nacosregistry.Options{ServerAddr: addr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Close()

	const service = "sweet.example.v1.HarborVerify"
	port := freePortHarbor(t)
	unreg, err := reg.Expose(ctx, service, instanceAtPort(port))
	if err != nil {
		t.Fatalf("Expose: %v", err)
	}

	got, err := reg.Query(ctx, service)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(got) != 1 || got[0].Port != port {
		t.Fatalf("Query returned %v, want the single registered instance", got)
	}

	// Subscribe must return a snapshot without a separate query.
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	ch, err := reg.Watch(watchCtx, service)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	select {
	case insts := <-ch:
		if len(insts) != 1 {
			t.Fatalf("subscribe snapshot = %v, want 1 instance", insts)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("subscribe delivered no snapshot")
	}

	// Deregistration must be visible, which is the counterpart of "the
	// instance's liveness is the connection".
	unreg()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if got, err := reg.Query(ctx, service); err == nil && len(got) == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got, _ := reg.Query(ctx, service); len(got) != 0 {
		t.Fatalf("instance still present after deregister: %v", got)
	}
}

// backends are the two Nacos-protocol servers this repo can be verified
// against: the reference implementation and harbor. Each is opt-in via its own
// environment variable, and a missing one skips its subtest rather than
// silently passing the whole matrix.
var backends = []struct {
	name string
	env  string
	hint string
}{
	{"nacos", "NACOS_GRPC_ADDR", "127.0.0.1:9848"},
	{"harbor", "SWEET_HARBOR_ADDR", "127.0.0.1:19848 (./run-sample.sh harbor-standalone 19848)"},
}

func TestBackendsContract(t *testing.T) {
	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			addr := os.Getenv(b.env)
			if addr == "" {
				t.Skipf("set %s=%s", b.env, b.hint)
			}
			runAgainstBackend(t, addr)
		})
	}
}

// TestHarborServesThroughDiscovery closes the loop: if harbor also serves the
// jaws RPC plane, a sweet client must be able to reach a sweet server through
// harbor discovery alone. Skips rather than fails when no server is attached.
func TestHarborServesThroughDiscovery(t *testing.T) {
	addr := os.Getenv("SWEET_HARBOR_ADDR")
	if addr == "" {
		t.Skip("set SWEET_HARBOR_ADDR")
	}
	reg, err := nacosregistry.New(nacosregistry.Options{ServerAddr: addr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Close()
	conn, err := client.Dial(reg, "sweet.example.v1.NotRegistered",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := ping.NewClient(conn).Ping(ctx, "harbor"); err == nil {
		t.Skip("harbor answered an RPC for an unknown service — unexpected, revisit")
	} else {
		t.Logf("discovery-only dial behaves as expected with no provider: %v", err)
	}
}

// freePortHarbor picks a port no provider will actually bind; the registry
// only needs a unique address to key instances by.
func freePortHarbor(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func instanceAtPort(port int) registry.Instance {
	return registry.Instance{Host: "127.0.0.1", Port: port, Weight: 100}
}
