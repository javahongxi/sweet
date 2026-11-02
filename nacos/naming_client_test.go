package nacos

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/javahongxi/sweet/gen"
)

// namingStub is a Nacos naming server for lifecycle tests: it answers the
// handshake, records what each connection asked for, can drop its stream on
// command, and can push a subscriber notification.
//
// Registrations and subscriptions are counted across all connections and the
// tests pair that with Handshakes(): "no replay happened" and "no replay was
// needed" look identical otherwise.
type namingStub struct {
	mu sync.Mutex

	conns  int // serverCheck calls, i.e. handshakes completed
	regs   []InstanceRequest
	subs   []SubscribeServiceRequest
	acks   []string // Metadata.type of every frame a client wrote on a stream
	setups []string // raw body of each ConnectionSetupRequest

	hosts []Instance // what the stub says the service has

	killNow chan struct{}
	pending chan *gen.Payload
	pushSeq int
	srv     *grpc.Server
}

func newNamingStub() *namingStub {
	return &namingStub{
		killNow: make(chan struct{}),
		pending: make(chan *gen.Payload, 8),
		hosts:   []Instance{testInstance(20501)},
	}
}

func testInstance(port int) Instance {
	return Instance{IP: "127.0.0.1", Port: port, Weight: 100, Healthy: true, Enabled: true, Ephemeral: true}
}

func (s *namingStub) snapshot() (regs, subs int, acks []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.regs), len(s.subs), append([]string(nil), s.acks...)
}

// Kill ends the current bidi stream. The next one the client opens behaves
// normally, which is what makes a reconnect observable rather than terminal.
func (s *namingStub) Kill() {
	s.mu.Lock()
	ch := s.killNow
	s.killNow = make(chan struct{})
	s.mu.Unlock()
	close(ch)
}

// PushNotify queues a subscriber notification for the stream handler to send.
func (s *namingStub) PushNotify(service string, hosts []Instance) error {
	s.mu.Lock()
	s.hosts = hosts
	s.pushSeq++
	id := fmt.Sprint(s.pushSeq)
	pending := s.pending
	s.mu.Unlock()

	// A real push always carries a requestId (ServerRequest.requestId); without
	// one the client has nothing to echo and Conn.Ack no-ops by design.
	body := NotifySubscriber{
		RequestID:   id,
		ServiceInfo: ServiceInfo{Name: service, GroupName: "DEFAULT_GROUP", Hosts: hosts, LastRefTime: time.Now().UnixMilli()},
	}
	select {
	case pending <- frame(TypeNotifySubscriberRequest, body):
		return nil
	case <-time.After(2 * time.Second):
		return fmt.Errorf("nacos: no live stream to push on")
	}
}

func (s *namingStub) unary(in *gen.Payload) *gen.Payload {
	t, _ := unmarshal(in, nil)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch t {
	case typeServerCheckRequest:
		s.conns++
		return frame("ServerCheckResponse", map[string]any{
			"resultCode": 200, "success": true, "connectionId": fmt.Sprintf("naming-conn-%d", s.conns),
		})
	case typeInstanceRequest:
		var req InstanceRequest
		_, _ = unmarshal(in, &req)
		s.regs = append(s.regs, req)
		return frame("InstanceResponse", map[string]any{"resultCode": 200, "success": true})
	case typeSubscribeServiceRequest:
		var req SubscribeServiceRequest
		_, _ = unmarshal(in, &req)
		s.subs = append(s.subs, req)
		return frame(TypeSubscribeServiceResponse, SubscribeServiceResponse{
			Response:    Response{ResultCode: 200, Success: true},
			ServiceInfo: ServiceInfo{Name: req.ServiceName, GroupName: req.GroupName, Hosts: s.hosts},
		})
	case typeServiceQueryRequest:
		var req ServiceQueryRequest
		_, _ = unmarshal(in, &req)
		return frame(typeQueryServiceResponse, QueryServiceResponse{
			Response:    Response{ResultCode: 200, Success: true},
			ServiceInfo: ServiceInfo{Name: req.ServiceName, GroupName: req.GroupName, Hosts: s.hosts},
		})
	case typeHealthCheckRequest:
		return frame("HealthCheckResponse", map[string]any{"resultCode": 200, "success": true})
	}
	return frame(typeEmptyResponse, map[string]any{"resultCode": 200, "success": true})
}

func (s *namingStub) bidi(stream grpc.BidiStreamingServer[gen.Payload, gen.Payload]) error {
	s.mu.Lock()
	kill, pending := s.killNow, s.pending
	s.mu.Unlock()

	recv := make(chan *gen.Payload, 64)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			p, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case recv <- p:
			case <-done:
				return
			}
		}
	}()

	for {
		select {
		case <-kill:
			return nil
		case p, ok := <-recv:
			if !ok {
				return nil
			}
			t, _ := unmarshal(p, nil)
			if t == typeConnectionSetupRequest {
				var req ConnectionSetupRequest
				_, _ = unmarshal(p, &req)
				raw := string(p.GetBody().GetValue())
				s.mu.Lock()
				s.acks = append(s.acks, t)
				s.setups = append(s.setups, raw)
				s.mu.Unlock()
				if req.AbilityTable == nil {
					continue // faithful: the server stays silent
				}
				if err := stream.Send(frame(typeSetupAckRequest, SetupAckRequest{AbilityTable: map[string]bool{}})); err != nil {
					return err
				}
				continue
			}
			s.mu.Lock()
			s.acks = append(s.acks, t)
			s.mu.Unlock()
		case payload := <-pending:
			if err := stream.Send(payload); err != nil {
				return err
			}
		}
	}
}

func startNamingStub(t *testing.T, s *namingStub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	s.mu.Lock()
	s.srv = srv
	s.mu.Unlock()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "Request", HandlerType: (*any)(nil), Metadata: "nacos_grpc_service.proto",
		Methods: []grpc.MethodDesc{{
			MethodName: "request",
			Handler: func(h any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				in := new(gen.Payload)
				if err := dec(in); err != nil {
					return nil, err
				}
				return h.(*namingStub).unary(in), nil
			},
		}},
	}, s)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "BiRequestStream", HandlerType: (*any)(nil), Metadata: "nacos_grpc_service.proto",
		Streams: []grpc.StreamDesc{{
			StreamName: "requestBiStream", ServerStreams: true, ClientStreams: true,
			Handler: func(h any, st grpc.ServerStream) error {
				return h.(*namingStub).bidi(&grpc.GenericServerStream[gen.Payload, gen.Payload]{ServerStream: st})
			},
		}},
	}, s)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func quickNamingClient(t *testing.T, opts NamingOptions) *NamingClient {
	t.Helper()
	if opts.ReconnectBackoff == 0 {
		opts.ReconnectBackoff = 20 * time.Millisecond
	}
	n, err := NewNamingClient(opts)
	if err != nil {
		t.Fatalf("NewNamingClient: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func mustReady(t *testing.T, n *NamingClient, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	if err := n.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func waitUpdate(t *testing.T, n *NamingClient, within time.Duration) ServiceInfo {
	t.Helper()
	select {
	case info := <-n.Updates():
		return info
	case <-time.After(within):
		t.Fatal("no update delivered")
		return ServiceInfo{}
	}
}

func ackCount(acks []string, want string) int {
	n := 0
	for _, a := range acks {
		if a == want {
			n++
		}
	}
	return n
}

// --- the lifecycle guarantees ----------------------------------------------

// TestNamingClientReplaysAfterReconnect is the reason this type exists: an
// ephemeral instance's liveness *is* the connection, so a reconnect that does not
// re-register leaves the service empty on the server while this process still
// believes it publishes it.
func TestNamingClientReplaysAfterReconnect(t *testing.T) {
	stub := newNamingStub()
	addr := startNamingStub(t, stub)

	n := quickNamingClient(t, NamingOptions{Addr: addr})
	mustReady(t, n, 5*time.Second)

	const service = "sweet.example.v1.Lifecycle"
	if _, err := n.Subscribe(context.Background(), service); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := n.Register(context.Background(), service, testInstance(20501)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if regs, subs, _ := stub.snapshot(); regs != 1 || subs != 1 {
		t.Fatalf("before the drop: regs=%d subs=%d, want 1 and 1", regs, subs)
	}

	before := n.Handshakes()
	stub.Kill()

	// WaitReady returns the moment a connection is published and the replay runs
	// after that, so these assertions poll. Snapshotting immediately would
	// measure the race rather than the behaviour.
	waitWithin(t, 8*time.Second, "a second handshake plus the replay of both intents", func() bool {
		if n.Handshakes() <= before {
			return false
		}
		regs, subs, _ := stub.snapshot()
		return regs >= 2 && subs >= 2
	})

	stub.mu.Lock()
	last := stub.regs[len(stub.regs)-1]
	stub.mu.Unlock()
	if last.Type != TypeRegisterInstance || last.Instance.Port != 20501 {
		t.Errorf("replayed registration = %+v, want registerInstance on port 20501", last)
	}
}

// TestDeregisterSurvivesAReconnectRace pins 注销优先: the intent is marked
// not-live before its deregistration is sent, so a reconnect racing the caller
// cannot resurrect an instance the application just withdrew.
func TestDeregisterSurvivesAReconnectRace(t *testing.T) {
	stub := newNamingStub()
	addr := startNamingStub(t, stub)

	n := quickNamingClient(t, NamingOptions{Addr: addr})
	mustReady(t, n, 5*time.Second)

	const service = "sweet.example.v1.Withdraw"
	if err := n.Register(context.Background(), service, testInstance(20601)); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := n.Deregister(context.Background(), service, testInstance(20601)); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	stub.mu.Lock()
	kinds := make([]string, 0, len(stub.regs))
	for _, r := range stub.regs {
		kinds = append(kinds, r.Type)
	}
	stub.mu.Unlock()
	if len(kinds) != 2 || kinds[0] != TypeRegisterInstance || kinds[1] != TypeDeregisterInstance {
		t.Fatalf("requests = %v, want register then deregister", kinds)
	}

	before := n.Handshakes()
	stub.Kill()
	waitWithin(t, 8*time.Second, "a second handshake", func() bool { return n.Handshakes() > before })
	time.Sleep(300 * time.Millisecond) // let any (wrong) replay land

	stub.mu.Lock()
	defer stub.mu.Unlock()
	for i, r := range stub.regs {
		if i >= 2 && r.Type == TypeRegisterInstance {
			t.Fatalf("replay resurrected a withdrawn instance: %+v", stub.regs)
		}
	}
}

// TestStaleConnectionConsumerIsInert pins the guard registry/nacos learned when
// it started logging "ack …: stream not open": a consumer belonging to a dead
// connection must stop touching anything, or it acks and delivers on a stream the
// server already forgot.
func TestStaleConnectionConsumerIsInert(t *testing.T) {
	stub := newNamingStub()
	addr := startNamingStub(t, stub)

	n := quickNamingClient(t, NamingOptions{Addr: addr})
	mustReady(t, n, 5*time.Second)

	const service = "sweet.example.v1.Pushes"
	if _, err := n.Subscribe(context.Background(), service); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := stub.PushNotify(service, []Instance{testInstance(20701)}); err != nil {
		t.Fatalf("PushNotify: %v", err)
	}
	first := waitUpdate(t, n, 3*time.Second)
	if len(first.Hosts) != 1 || first.Hosts[0].Port != 20701 {
		t.Fatalf("first update = %+v, want port 20701", first)
	}

	// The ack must be observed on the first connection before it is torn down:
	// SendMsg only queues, so killing the stream immediately would race the frame
	// out of existence and the test would then be measuring its own timing.
	waitWithin(t, 3*time.Second, "the first ack to land", func() bool {
		_, _, a := stub.snapshot()
		return ackCount(a, "NotifySubscriberResponse") == 1
	})

	before := n.Handshakes()
	stub.Kill()
	waitWithin(t, 8*time.Second, "a second handshake", func() bool { return n.Handshakes() > before })

	// One push on the new connection: exactly one more update and one more ack.
	// A stale consumer would double either.
	if err := stub.PushNotify(service, []Instance{testInstance(20702)}); err != nil {
		t.Fatalf("PushNotify after reconnect: %v", err)
	}
	second := waitUpdate(t, n, 3*time.Second)
	if len(second.Hosts) != 1 || second.Hosts[0].Port != 20702 {
		t.Fatalf("second update = %+v, want port 20702", second)
	}
	select {
	case extra := <-n.Updates():
		t.Errorf("duplicate delivery: %+v", extra)
	case <-time.After(300 * time.Millisecond):
	}

	time.Sleep(500 * time.Millisecond)
	_, _, acks := stub.snapshot()
	if got := ackCount(acks, "NotifySubscriberResponse"); got != 2 {
		t.Errorf("acks = %v (handshakes=%d), want exactly two NotifySubscriberResponse",
			acks, n.Handshakes())
	}
}

// TestNamingClientServerListFailover is the naming mirror of the config-domain
// criterion: the first address has nothing behind it, so the client must come up
// on the second without anyone reconfiguring it.
func TestNamingClientServerListFailover(t *testing.T) {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	stub := newNamingStub()
	liveAddr := startNamingStub(t, stub)

	n := quickNamingClient(t, NamingOptions{Servers: []string{deadAddr, liveAddr}})
	mustReady(t, n, 8*time.Second)
	if got := n.CurrentAddr(); got != liveAddr {
		t.Errorf("CurrentAddr = %q, want the reachable node %q", got, liveAddr)
	}
	info, err := n.Query(context.Background(), "sweet.example.v1.Failover")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(info.Hosts) == 0 {
		t.Error("Query returned no hosts through the second node")
	}
	if !strings.Contains(n.addrs(), deadAddr) {
		t.Errorf("addrs() should still list the dead node, got %q", n.addrs())
	}
}

// TestNamingClientLabelsConnectionNaming keeps the label guarantee on the wire:
// module=naming is what makes the naming module see this connection at all
// (ConnectionBasedClientManager.java:60-64 makes it ignore anything else).
func TestNamingClientLabelsConnectionNaming(t *testing.T) {
	stub := newNamingStub()
	addr := startNamingStub(t, stub)
	n := quickNamingClient(t, NamingOptions{Addr: addr})
	mustReady(t, n, 5*time.Second)
	if _, err := n.Query(context.Background(), "sweet.example.v1.Labels"); err != nil {
		t.Fatalf("Query: %v", err)
	}

	stub.mu.Lock()
	setups := append([]string(nil), stub.setups...)
	stub.mu.Unlock()
	if len(setups) == 0 {
		t.Fatal("the stub recorded no ConnectionSetupRequest")
	}
	if !strings.Contains(setups[0], `"module":"naming"`) {
		t.Errorf("setup labels = %s, want module=naming", setups[0])
	}
	if !strings.Contains(setups[0], `"abilityTable"`) {
		t.Errorf("abilityTable must be present or the server never acks: %s", setups[0])
	}
}

// TestNewNamingClientRejectsBadOptions keeps the constructor's contract: it
// fails on structure only, never on reachability.
func TestNewNamingClientRejectsBadOptions(t *testing.T) {
	if _, err := NewNamingClient(NamingOptions{Addr: "127.0.0.1:1", Labels: map[string]string{"module": "config"}}); err == nil {
		t.Fatal("want an error when a caller overrides module")
	}
	if _, err := NewNamingClient(NamingOptions{}); err == nil {
		t.Fatal("want an error with no address")
	}
	// An unreachable address must NOT be an error — WaitReady reports that.
	n, err := NewNamingClient(NamingOptions{Addr: "127.0.0.1:1", ReconnectBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatalf("New must not fail on an unreachable server: %v", err)
	}
	defer func() { _ = n.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if werr := n.WaitReady(ctx); werr == nil {
		t.Fatal("WaitReady must report the unreachable list")
	}
}
