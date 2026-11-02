package nacos

import (
	"context"
	"os"
	"testing"
	"time"

	naming "github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

// TestPushReachesAnotherConnection is the consumer-facing half of the contract:
// a subscriber on one connection must learn about a registration made on a
// different one. If this ever stops holding, every resolver in the fleet is
// blind and no amount of reconnect logic saves it.
//
// It replaced a probe that only logged frames and could not fail.
func TestPushReachesAnotherConnection(t *testing.T) {
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s (e.g. 127.0.0.1:9848)", envAddr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const service = "sweet.example.v1.PushTopology"
	inst := registry.Instance{Host: "127.0.0.1", Port: 20811, Weight: 100}

	sub, err := naming.NewNamingClient(naming.NamingOptions{Addr: addr, Namespace: "public"})
	if err != nil {
		t.Fatalf("subscriber client: %v", err)
	}
	defer func() { _ = sub.Close() }()
	if _, err := sub.Subscribe(ctx, service); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	pub, err := naming.NewNamingClient(naming.NamingOptions{Addr: addr, Namespace: "public"})
	if err != nil {
		t.Fatalf("publisher client: %v", err)
	}
	defer func() { _ = pub.Close() }()
	if err := pub.Register(ctx, service, wireInstance(inst)); err != nil {
		t.Fatalf("Register on the other connection: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case info := <-sub.Updates():
			if hasPort(info.Hosts, inst.Port) {
				return // the push arrived
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("subscriber never received a push for %s registered on another connection", service)
}

// TestSameConnectionRegistrationTakesEffect asks the question the probe left
// open — does a subscriber hear about a registration made on its *own*
// connection? — by separating the two claims instead of blurring them: the
// registration taking effect is asserted through a query, while whether Nacos
// pushes to the connection that caused the change is measured and reported.
//
// Same-connection silence would be server behaviour worth documenting; a
// registration that never takes effect is a client bug. The old probe could not
// tell those apart because it asserted nothing.
func TestSameConnectionRegistrationTakesEffect(t *testing.T) {
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s (e.g. 127.0.0.1:9848)", envAddr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const service = "sweet.example.v1.SameConnPush"
	inst := registry.Instance{Host: "127.0.0.1", Port: 20812, Weight: 100}

	c, err := naming.NewNamingClient(naming.NamingOptions{Addr: addr, Namespace: "public"})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Subscribe(ctx, service); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := c.Register(ctx, service, wireInstance(inst)); err != nil {
		t.Fatalf("Register on the same connection: %v", err)
	}

	// The assertion: the server believes the instance exists.
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := c.Query(ctx, service)
		if err != nil {
			t.Fatalf("Query: %v", err)
		}
		if hasPort(info.Hosts, inst.Port) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the registration never took effect on the server: %+v", info.Hosts)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The measurement: did a push also arrive on this same connection?
	select {
	case info := <-c.Updates():
		if hasPort(info.Hosts, inst.Port) {
			t.Log("same-connection push: YES — Nacos pushed to the connection that registered")
		} else {
			t.Logf("same-connection push: arrived with other hosts: %+v", info.Hosts)
		}
	case <-time.After(3 * time.Second):
		t.Log("same-connection push: NO push on the registering connection — consumers " +
			"on other connections are covered by TestPushReachesAnotherConnection")
	}
}

func wireInstance(in registry.Instance) naming.Instance {
	return naming.Instance{
		IP: in.Host, Port: in.Port, Weight: float64(in.Weight),
		Healthy: true, Enabled: true, Ephemeral: true, ClusterName: "DEFAULT",
	}
}

func hasPort(hosts []naming.Instance, port int) bool {
	for _, h := range hosts {
		if h.Port == port {
			return true
		}
	}
	return false
}
