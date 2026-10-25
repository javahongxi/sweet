package nacos

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/javahongxi/sweet/registry"
)

// Set NACOS_GRPC_ADDR=127.0.0.1:9848 to run against a real server.
const envAddr = "NACOS_GRPC_ADDR"

func liveAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s (e.g. 127.0.0.1:9848)", envAddr)
	}
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("%s unreachable: %v", addr, err)
	}
	_ = c.Close()
	return addr
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// TestLiveRegisterQueryDeregister is S3's acceptance: an instance registered by
// hand-written protocol code must become visible to a query, and disappear
// after deregistration.
func TestLiveRegisterQueryDeregister(t *testing.T) {
	reg, err := New(Options{ServerAddr: liveAddr(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Close()

	const service = "sweet.example.v1.LiveTest"
	port := freePort(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	unreg, err := reg.Expose(ctx, service, registry.Instance{Host: "127.0.0.1", Port: port, Weight: 100})
	if err != nil {
		t.Fatalf("Expose: %v", err)
	}

	got, err := reg.query(ctx, service)
	if err != nil {
		t.Fatalf("query after register: %v", err)
	}
	if len(got) != 1 || got[0].Port != port {
		t.Fatalf("query returned %v, want exactly the registered instance", got)
	}

	unreg()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err = reg.query(ctx, service)
		if err == nil && len(got) == 0 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("instance still present after deregister: %v", got)
}

// TestLiveWatchSeedsAndPushes proves the push path: Watch must return the
// current snapshot immediately and then a changed list when a second instance
// registers.
func TestLiveWatchSeedsAndPushes(t *testing.T) {
	reg, err := New(Options{ServerAddr: liveAddr(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Close()

	const service = "sweet.example.v1.LiveWatch"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ch, err := reg.Watch(ctx, service)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	select {
	case seed := <-ch:
		t.Logf("seed snapshot: %d instance(s)", len(seed))
	case <-time.After(10 * time.Second):
		t.Fatal("Watch delivered no initial snapshot")
	}

	unreg, err := reg.Expose(ctx, service, registry.Instance{Host: "127.0.0.1", Port: freePort(t), Weight: 100})
	if err != nil {
		t.Fatalf("Expose: %v", err)
	}
	defer unreg()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case insts := <-ch:
			if len(insts) >= 1 {
				t.Logf("push delivered %d instance(s)", len(insts))
				return
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatal("no push observed after a registration")
}
