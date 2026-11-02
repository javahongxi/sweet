package nacos

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// ConnectResetRequest is the server's "please move elsewhere" push, sent when it
// is shedding connections or an operator reloads it
// (ConnectionManager.java:286-319, ServerLoaderControllerV3 reloadClient). The
// official client acks with ConnectResetResponse on the *current* stream and then
// switches nodes (RpcClient.java:397-428); it does not reconnect in place, because
// the point of the push is to leave.
//
// These tests pin that sequence, and the replay that must follow it: a healthy
// new connection means the supervisor will never fire, so a switch that does not
// re-register / re-listen simply loses the state.

func splitHostPort(t *testing.T, addr string) (string, string) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	return host, port
}

func TestConnectResetMovesTheConfigConnectionAndReListens(t *testing.T) {
	a := newConfigStub("v-a", "md5-a")
	addrA := startConfigStub(t, a)
	b := newConfigStub("v-b", "md5-b")
	addrB := startConfigStub(t, b)

	c, err := NewConfigClient(ConfigOptions{
		Servers:          []string{addrA, addrB},
		ReconnectBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	mustReadyConfig(t, c, 5*time.Second)
	if got := c.CurrentAddr(); got != addrA {
		t.Fatalf("CurrentAddr = %q, want the first entry %q", got, addrA)
	}

	updates := make(chan Config, 8)
	key := ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}
	if err := c.Watch(context.Background(), key, func(cfg Config) { updates <- cfg }); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if seed := <-updates; seed.Content != "v-a" {
		t.Fatalf("seed = %+v, want the first node's content", seed)
	}

	before := c.Handshakes()
	if err := a.PushRaw(TypeConnectResetRequest, map[string]any{"requestId": "9"}); err != nil {
		t.Fatalf("PushRaw: %v", err)
	}

	// The switch itself.
	waitWithin(t, 8*time.Second, "the config connection to move to the second node", func() bool {
		return c.CurrentAddr() == addrB
	})
	if got := c.Handshakes() - before; got != 1 {
		t.Errorf("handshakes grew by %d, want exactly 1 — the retired connection must not "+
			"trigger a second reconnect", got)
	}

	// Ack on the OLD stream, with the official class name and the requestId echoed.
	acks := a.rawBodies("ConnectResetResponse")
	if len(acks) != 1 {
		t.Fatalf("first node saw %d ConnectResetResponse frames, want 1 (types: %v)", len(acks), a.ackTypes())
	}
	if !strings.Contains(acks[0], `"requestId":"9"`) {
		t.Errorf("ack must echo the push requestId or the server's 3s ack wait fails: %s", acks[0])
	}

	// The replay: the listen table is keyed by connectionId, so the new node must
	// be told about the watch or updates stop forever.
	waitWithin(t, 5*time.Second, "the listen to be replayed on the new node", func() bool {
		return len(b.rawBodies("ConfigBatchListenRequest")) >= 1
	})

	// And the watch must still deliver, now from the second node.
	// The replay's own stale-subset pull may deliver the new node's current
	// value first — that is the re-sync working, not noise — so drain until the
	// value we published after the switch shows up.
	b.setContent("v-b2", "md5-b2")
	b.push <- ConfigChangeNotifyRequest{RequestID: "1", DataID: "routes.json", Group: "DEFAULT_GROUP"}
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case got := <-updates:
			if got.Content == "v-b2" {
				return
			}
		case <-time.After(time.Until(deadline)):
			t.Fatal("the watch never delivered from the new node")
		}
	}
}

func TestConnectResetFollowsTheRecommendedAddress(t *testing.T) {
	a := newConfigStub("v-a", "md5-a")
	addrA := startConfigStub(t, a)
	b := newConfigStub("v-b", "md5-b")
	addrB := startConfigStub(t, b)
	hostB, portB := splitHostPort(t, addrB)

	c, err := NewConfigClient(ConfigOptions{
		Servers:          []string{addrA, addrB},
		ReconnectBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	mustReadyConfig(t, c, 5*time.Second)

	// serverPort is a string on the wire (ConnectResetRequest.java:31).
	if err := a.PushRaw(TypeConnectResetRequest, map[string]any{
		"requestId": "11", "serverIp": hostB, "serverPort": portB,
	}); err != nil {
		t.Fatalf("PushRaw: %v", err)
	}
	waitWithin(t, 8*time.Second, "the recommended node to be adopted", func() bool {
		return c.CurrentAddr() == addrB
	})
}

// A recommendation is only honoured if that host is already in our list
// (RpcClient.java:319-339); dialing a stranger the server named would be a
// redirect the operator never sanctioned.
func TestConnectResetIgnoresRecommendationOutsideTheList(t *testing.T) {
	a := newConfigStub("v-a", "md5-a")
	addrA := startConfigStub(t, a)
	b := newConfigStub("v-b", "md5-b")
	addrB := startConfigStub(t, b)

	c, err := NewConfigClient(ConfigOptions{
		Servers:          []string{addrA, addrB},
		ReconnectBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	mustReadyConfig(t, c, 5*time.Second)

	if err := a.PushRaw(TypeConnectResetRequest, map[string]any{
		"requestId": "12", "serverIp": "203.0.113.7", "serverPort": "1234",
	}); err != nil {
		t.Fatalf("PushRaw: %v", err)
	}
	// It must still leave the overloaded node — just to one of ours.
	waitWithin(t, 8*time.Second, "a move to the in-list node", func() bool {
		return c.CurrentAddr() == addrB
	})
	if got := c.CurrentAddr(); strings.HasPrefix(got, "203.0.113.7") {
		t.Errorf("client dialled a host outside its server list: %q", got)
	}
}

// Single-node list: nowhere to move, so the reset is honoured by re-establishing
// the same node rather than being dropped on the floor.
func TestConnectResetOnSingleNodeListRebuildsInPlace(t *testing.T) {
	a := newConfigStub("v-a", "md5-a")
	addrA := startConfigStub(t, a)

	c, err := NewConfigClient(ConfigOptions{
		Servers:          []string{addrA},
		ReconnectBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	mustReadyConfig(t, c, 5*time.Second)

	before := c.Handshakes()
	if err := a.PushRaw(TypeConnectResetRequest, map[string]any{"requestId": "13"}); err != nil {
		t.Fatalf("PushRaw: %v", err)
	}
	waitWithin(t, 8*time.Second, "a rebuilt connection on the only node", func() bool {
		return c.Handshakes() > before && c.CurrentAddr() == addrA
	})
	if len(a.rawBodies("ConnectResetResponse")) != 1 {
		t.Error("the reset must still be acked when there is nowhere to go")
	}
}

// An unrecognised push must not be answered: there is no EmptyResponse class in
// 3.2.3, so anything we invent is a payload the server cannot resolve
// (GrpcUtils.java:132-133) — the same failure mode as a mis-mapped ack class.
func TestUnknownPushIsLeftUnAcked(t *testing.T) {
	a := newConfigStub("v", "m")
	addr := startConfigStub(t, a)

	c, err := NewConfigClient(ConfigOptions{Addr: addr, ReconnectBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	mustReadyConfig(t, c, 5*time.Second)

	if err := a.PushRaw("SomeFutureThingRequest", map[string]any{"requestId": "77"}); err != nil {
		t.Fatalf("PushRaw: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	for _, typ := range a.ackTypes() {
		if strings.Contains(typ, "SomeFutureThing") || typ == typeEmptyResponse {
			t.Fatalf("client answered an unknown push with %q; the official client stays silent", typ)
		}
	}
	// The connection must survive an unrecognised push.
	if !c.conn.Ready() {
		t.Errorf("an unknown push killed the connection: %v", c.LastErr())
	}
}

// The naming side of the same signal, including the replay that matters most
// there: registrations live and die with the connection.
func TestConnectResetReplaysNamingRegistrations(t *testing.T) {
	a := newNamingStub()
	addrA := startNamingStub(t, a)
	b := newNamingStub()
	addrB := startNamingStub(t, b)

	n, err := NewNamingClient(NamingOptions{
		Servers:          []string{addrA, addrB},
		ReconnectBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewNamingClient: %v", err)
	}
	defer func() { _ = n.Close() }()
	mustReady(t, n, 5*time.Second)

	const service = "sweet.example.v1.Reset"
	if _, err := n.Subscribe(context.Background(), service); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := n.Register(context.Background(), service, testInstance(20901)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := n.CurrentAddr(); got != addrA {
		t.Fatalf("CurrentAddr = %q, want %q", got, addrA)
	}

	if err := a.PushRaw(TypeConnectResetRequest, map[string]any{"requestId": "21"}); err != nil {
		t.Fatalf("PushRaw: %v", err)
	}
	waitWithin(t, 8*time.Second, "the naming connection to move", func() bool {
		return n.CurrentAddr() == addrB
	})
	// The replay is the point: on the new node the instance does not exist until
	// we re-register it, and the supervisor will not do it because the new
	// connection is healthy.
	waitWithin(t, 5*time.Second, "registration and subscription to be replayed on the new node", func() bool {
		b.mu.Lock()
		defer b.mu.Unlock()
		return len(b.regs) >= 1 && len(b.subs) >= 1
	})
	if got := n.Registered(); len(got) != 1 || !strings.Contains(got[0], service) {
		t.Errorf("Registered() = %v, want the intent still recorded", got)
	}
}

func mustReadyConfig(t *testing.T, c *ConfigClient, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}
