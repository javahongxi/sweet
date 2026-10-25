package nacos

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/javahongxi/sweet/nacos/conn"
	"github.com/javahongxi/sweet/registry"
)

// TestProbeWhoActuallyPushes answers, with frames rather than theory: does a
// subscriber on its OWN connection hear about a registration made on the SAME
// connection, and does a second connection hear about a registration made by
// the first one? Same-connection silence would be Nacos behaviour worth
// documenting, not a client bug.
func TestProbeWhoActualPushes(t *testing.T) {
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s", envAddr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Watcher on a raw conn we read ourselves.
	c, err := conn.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("setup: %v", err)
	}
	const service = "sweet.example.v1.ProbePush"
	_, _, err = c.Unary(ctx, conn.SubscribeServiceRequest{
		Namespace: "public", ServiceName: service, GroupName: "DEFAULT_GROUP", Subscribe: true,
	})
	if err != nil {
		t.Fatalf("subscribe on raw conn: %v", err)
	}

	// Publisher on a separate registry instance (separate connection).
	pub, err := New(Options{ServerAddr: addr})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer pub.Close()
	unreg, err := pub.Expose(ctx, service, registry.Instance{Host: "127.0.0.1", Port: freePort(t), Weight: 100})
	if err != nil {
		t.Fatalf("Expose: %v", err)
	}

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case p, ok := <-c.Pushes():
			if !ok {
				t.Fatal("push channel closed")
			}
			t.Logf("frame on subscriber conn: type=%s body=%s", p.Type, first(string(p.Body), 220))
		case <-time.After(time.Second):
		}
	}
	t.Logf("subscriber saw %d frame(s) in 15s; now deregistering", 0)
	unreg()
	time.Sleep(2 * time.Second)
	select {
	case p := <-c.Pushes():
		t.Logf("after deregister: type=%s body=%s", p.Type, first(string(p.Body), 220))
	default:
		t.Log("no frame after deregister")
	}
}

func first(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

var _ = json.Marshal
