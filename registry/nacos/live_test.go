package nacos

import (
	"context"
	"fmt"
	"net"
	"os"
	"slices"
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

	// A fresh registration is not visible to a query at the instant the register
	// call returns. Measured on a live 3.2.3: register → visible took 305ms and
	// 310ms for two services that did not exist beforehand, and 0s for one that
	// already did. So "query once and assert" was green only when the server
	// happened to carry leftover state from an earlier run — which is how this test
	// used to pass. Poll for appearance the same way the deregistration below polls
	// for disappearance; "graceful" is not symmetric, but visibility lag is.
	registeredAt := time.Now()
	appearedAt := time.Time{}
	var lastErr error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err := reg.Query(ctx, service)
		if err != nil {
			lastErr = err // a transient query error must not decide the outcome
		} else if len(got) == 1 && got[0].Port == port {
			appearedAt = time.Now()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if appearedAt.IsZero() {
		t.Fatalf("the registered instance never became visible within 10s (service %s, port %d, last query error: %v)",
			service, port, lastErr)
	}
	t.Logf("register → visible in %s", appearedAt.Sub(registeredAt).Round(10*time.Millisecond))

	unreg()
	goneDeadline := time.Now().Add(10 * time.Second)
	var last []registry.Instance
	for time.Now().Before(goneDeadline) {
		got, err := reg.Query(ctx, service)
		if err == nil && len(got) == 0 {
			return
		}
		if err == nil {
			last = got
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("instance still present after deregister: %v", last)
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

// TestLiveWatchSharedSnapshotAndCancellation exercises the adapter contract
// against Nacos itself: a later watcher gets the latest pushed view, cancelling
// it leaves its sibling subscribed, and cancelling the last watcher clears the
// shared subscription intent.
func TestLiveWatchSharedSnapshotAndCancellation(t *testing.T) {
	reg, err := New(Options{ServerAddr: liveAddr(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Close()

	service := fmt.Sprintf("sweet.example.v1.LiveWatchShared.%d", time.Now().UnixNano())
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first, err := reg.Watch(firstCtx, service)
	if err != nil {
		t.Fatalf("first Watch: %v", err)
	}
	select {
	case seed := <-first:
		if len(seed) != 0 {
			t.Fatalf("initial snapshot for a new service = %v, want empty", seed)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first Watch delivered no initial snapshot")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	port1 := freePort(t)
	unreg1, err := reg.Expose(ctx, service, instanceAt(port1))
	if err != nil {
		t.Fatalf("first Expose: %v", err)
	}
	defer unreg1()
	waitForLiveInstance(t, first, port1)

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	second, err := reg.Watch(secondCtx, service)
	if err != nil {
		t.Fatalf("second Watch: %v", err)
	}
	// No second wire subscription or extra push is needed: this list must come
	// from the view cached while the first watcher received its push.
	waitForLiveInstance(t, second, port1)
	cancelSecond()
	requireWatcherClosed(t, second)
	waitForWatchers(t, reg, service, 1)
	if !slices.Contains(reg.client.Subscribed(), service) {
		t.Fatal("cancelling one watcher cleared the surviving watcher's subscription")
	}

	port2 := freePort(t)
	unreg2, err := reg.Expose(ctx, service, instanceAt(port2))
	if err != nil {
		t.Fatalf("second Expose: %v", err)
	}
	defer unreg2()
	waitForLiveInstance(t, first, port2)

	cancelFirst()
	requireWatcherClosed(t, first)
	waitForWatchers(t, reg, service, 0)
	deadline := time.Now().Add(5 * time.Second)
	for slices.Contains(reg.client.Subscribed(), service) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if slices.Contains(reg.client.Subscribed(), service) {
		t.Fatal("last watcher cancellation left a live subscription intent")
	}
}

func waitForLiveInstance(t *testing.T, ch <-chan []registry.Instance, port int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last []registry.Instance
	for time.Now().Before(deadline) {
		select {
		case insts, ok := <-ch:
			if !ok {
				t.Fatalf("watcher closed while waiting for port %d", port)
			}
			last = insts
			for _, in := range insts {
				if in.Port == port {
					return
				}
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("no snapshot or push contained port %d within 15s; last list = %v", port, last)
}

func instanceAt(port int) registry.Instance {
	return registry.Instance{Host: "127.0.0.1", Port: port, Weight: 100}
}
