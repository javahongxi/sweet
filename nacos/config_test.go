package nacos

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/javahongxi/sweet/gen"
)

// configStub is a Nacos config-domain server for behaviour tests. It answers
// the handshake, serves ConfigQueryRequest and ConfigBatchListenRequest, records
// what the client sent (raw JSON, so field names are assertable), and can push a
// ConfigChangeNotifyRequest or drop the stream on command.
type configStub struct {
	mu sync.Mutex

	connSeq   int
	queries   []*gen.Payload
	listens   []*gen.Payload
	clientFra []*gen.Payload // everything the client wrote on the bidi stream

	content string
	md5     string

	// staleOnListen decides whether a listen reports the key as changed: the
	// real server does that when the client's md5 is not current.
	staleOnListen bool

	push chan ConfigChangeNotifyRequest
	kill chan struct{}
}

func newConfigStub(content, md5 string) *configStub {
	return &configStub{
		content: content,
		md5:     md5,
		push:    make(chan ConfigChangeNotifyRequest, 4),
		kill:    make(chan struct{}),
	}
}

func (s *configStub) setContent(content, md5 string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.content, s.md5 = content, md5
}

func (s *configStub) snapshot() (queries, listens, clientFrames []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	types := func(ps []*gen.Payload) []string {
		out := make([]string, 0, len(ps))
		for _, p := range ps {
			out = append(out, p.GetMetadata().GetType())
		}
		return out
	}
	return types(s.queries), types(s.listens), types(s.clientFra)
}

// rawBodies returns the JSON bodies the client sent for a given Metadata.type,
// so a test can assert on wire field names rather than on Go struct fields.
func (s *configStub) rawBodies(wantType string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	collect := func(ps []*gen.Payload) {
		for _, p := range ps {
			if p.GetMetadata().GetType() == wantType {
				out = append(out, string(p.GetBody().GetValue()))
			}
		}
	}
	collect(s.queries)
	collect(s.listens)
	collect(s.clientFra)
	return out
}

func (s *configStub) unary(in *gen.Payload) *gen.Payload {
	t, _ := unmarshal(in, nil)
	s.mu.Lock()
	switch t {
	case typeServerCheckRequest:
		s.connSeq++
		id := "cfg-conn-1"
		if s.connSeq > 1 {
			id = "cfg-conn-" + itoa(s.connSeq)
		}
		s.mu.Unlock()
		return frame("ServerCheckResponse", map[string]any{
			"resultCode": 200, "success": true, "connectionId": id,
		})
	case "ConfigQueryRequest":
		s.queries = append(s.queries, in)
		content, md5 := s.content, s.md5
		s.mu.Unlock()
		return frame("ConfigQueryResponse", map[string]any{
			"resultCode": 200, "success": true, "content": content, "md5": md5,
			"contentType": "application/json", "lastModified": 1730000000000,
		})
	case "ConfigBatchListenRequest":
		s.listens = append(s.listens, in)
		var req ConfigBatchListenRequest
		_, _ = unmarshal(in, &req)
		resp := map[string]any{"resultCode": 200, "success": true, "changedConfigs": []any{}}
		// Faithful to ConfigChangeBatchListenRequestHandler.java:82-88: the server
		// compares the md5 the client sent with the one it holds and lists the
		// keys that differ. A stub that always answered "nothing stale" would let
		// a client that never pulls look perfectly healthy.
		changed := make([]any, 0, len(req.ConfigListenContexts))
		for _, c := range req.ConfigListenContexts {
			if s.staleOnListen || c.MD5 != s.md5 {
				changed = append(changed, ConfigContext{Group: c.Group, DataID: c.DataID, Tenant: c.Tenant})
			}
		}
		resp["changedConfigs"] = changed
		s.mu.Unlock()
		return frame("ConfigChangeBatchListenResponse", resp)
	}
	s.mu.Unlock()
	return frame(typeEmptyResponse, map[string]any{"success": true})
}

func (s *configStub) bidi(stream grpc.BidiStreamingServer[gen.Payload, gen.Payload]) error {
	s.mu.Lock()
	kill := s.kill
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
			// Record first, dispatch second: a test asserting "what the client
			// wrote" must see every frame, the handshake included.
			s.mu.Lock()
			s.clientFra = append(s.clientFra, p)
			s.mu.Unlock()

			if t, _ := unmarshal(p, nil); t == typeConnectionSetupRequest {
				var req ConnectionSetupRequest
				_, _ = unmarshal(p, &req)
				if req.AbilityTable == nil {
					continue // faithful: the server stays silent
				}
				if err := stream.Send(frame(typeSetupAckRequest, SetupAckRequest{
					AbilityTable: map[string]bool{},
				})); err != nil {
					return err
				}
			}
		case note := <-s.push:
			if err := stream.Send(frame(TypeConfigChangeNotifyRequest, note)); err != nil {
				return err
			}
		}
	}
}

// Kill drops the current stream once; the next stream the client opens is
// unaffected, which is what makes a reconnect observable rather than permanent.
func (s *configStub) Kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.kill)
	s.kill = make(chan struct{})
}

func startConfigStub(t *testing.T, s *configStub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serveConfigStub(t, lis, s)
	return lis.Addr().String()
}

// serveConfigStub runs the stub on an already-bound listener, so a test can
// reserve an address, let a client fail against it, and then make the config
// centre appear on that same address.
func serveConfigStub(t *testing.T, lis net.Listener, s *configStub) {
	t.Helper()
	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "Request",
		HandlerType: (*any)(nil),
		Metadata:    "nacos_grpc_service.proto",
		Methods: []grpc.MethodDesc{{
			MethodName: "request",
			Handler: func(h any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				in := new(gen.Payload)
				if err := dec(in); err != nil {
					return nil, err
				}
				return h.(*configStub).unary(in), nil
			},
		}},
	}, s)
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "BiRequestStream",
		HandlerType: (*any)(nil),
		Metadata:    "nacos_grpc_service.proto",
		Streams: []grpc.StreamDesc{{
			StreamName:    "requestBiStream",
			ServerStreams: true,
			ClientStreams: true,
			Handler: func(h any, st grpc.ServerStream) error {
				return h.(*configStub).bidi(
					&grpc.GenericServerStream[gen.Payload, gen.Payload]{ServerStream: st})
			},
		}},
	}, s)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	t.Cleanup(func() { closeQuietly(s.kill) })
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return strings.Trim(string(b), `"`)
}

func closeQuietly(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}

func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	waitWithin(t, 3*time.Second, what, cond)
}

func waitWithin(t *testing.T, budget time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newTestConfigClient(t *testing.T, addr string) *ConfigClient {
	t.Helper()
	c, err := NewConfigClient(ConfigOptions{
		Addr:             addr,
		ReconnectBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewConfigClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestConfigQueryRoundTrip asserts the wire contract, not the Go struct: the
// Metadata.type string and the JSON field names the server actually reads.
func TestConfigQueryRoundTrip(t *testing.T) {
	stub := newConfigStub(`{"routes":[]}`, "md5-seed")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	got, err := c.Query(context.Background(), ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Content != `{"routes":[]}` || got.MD5 != "md5-seed" {
		t.Fatalf("Query = %+v, want the scripted content and md5", got)
	}
	if got.Tenant != "" {
		t.Errorf("Tenant = %q, want the public namespace", got.Tenant)
	}

	bodies := stub.rawBodies("ConfigQueryRequest")
	if len(bodies) != 1 {
		t.Fatalf("server saw %d ConfigQueryRequest frames, want 1", len(bodies))
	}
	for _, want := range []string{`"dataId":"routes.json"`, `"group":"DEFAULT_GROUP"`} {
		if !strings.Contains(bodies[0], want) {
			t.Errorf("request body %s lacks %s — Jackson reads these exact names", bodies[0], want)
		}
	}
	if strings.Contains(bodies[0], `"headers"`) {
		t.Errorf("headers must not ride in the body (GrpcUtils.java:108-113): %s", bodies[0])
	}
}

// TestConfigWatchPullsAfterNotify is the pull-after-notify cycle in one test:
// push → ack with the same requestId → query → callback → re-listen with the new
// md5. Drop any of the four and the server either re-pushes forever or keeps
// treating this connection as stale.
func TestConfigWatchPullsAfterNotify(t *testing.T) {
	stub := newConfigStub("v1", "md5-v1")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	updates := make(chan Config, 8)
	key := ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}
	if err := c.Watch(context.Background(), key, func(cfg Config) { updates <- cfg }); err != nil {
		t.Fatalf("Watch: %v", err)
	}

	seed := <-updates
	if seed.Content != "v1" || seed.MD5 != "md5-v1" {
		t.Fatalf("seed = %+v, want v1/md5-v1", seed)
	}
	waitForCond(t, "the first listen", func() bool {
		return len(stub.rawBodies("ConfigBatchListenRequest")) == 1
	})
	if first := stub.rawBodies("ConfigBatchListenRequest")[0]; !strings.Contains(first, `"md5":"md5-v1"`) {
		t.Errorf("first listen should carry the seeded md5: %s", first)
	}
	firstBody := stub.rawBodies("ConfigBatchListenRequest")[0]
	if !strings.Contains(firstBody, `"listen":true`) {
		t.Errorf("first listen should be a subscription: %s", firstBody)
	}

	// The server-side change, then the push.
	stub.setContent("v2", "md5-v2")
	stub.push <- ConfigChangeNotifyRequest{RequestID: "7", DataID: "routes.json", Group: "DEFAULT_GROUP"}

	var applied Config
	select {
	case applied = <-updates:
	case <-time.After(3 * time.Second):
		t.Fatal("no update after the notify")
	}
	if applied.Content != "v2" || applied.MD5 != "md5-v2" {
		t.Fatalf("update = %+v, want v2/md5-v2", applied)
	}

	acks := stub.rawBodies("ConfigChangeNotifyResponse")
	if len(acks) != 1 {
		t.Fatalf("server got %d acks, want exactly 1", len(acks))
	}
	if !strings.Contains(acks[0], `"requestId":"7"`) {
		t.Errorf("ack must echo the push requestId or the server re-pushes: %s", acks[0])
	}

	// A pull, not a guess: the notify carried keys only.
	if q := stub.rawBodies("ConfigQueryRequest"); len(q) != 2 {
		t.Errorf("server saw %d queries, want 2 (seed + refresh after notify)", len(q))
	}

	// And the re-listen must carry the *new* md5.
	waitForCond(t, "the re-listen with the new md5", func() bool {
		bodies := stub.rawBodies("ConfigBatchListenRequest")
		return len(bodies) >= 2 && strings.Contains(bodies[len(bodies)-1], `"md5":"md5-v2"`)
	})
}

// TestConfigReplayAfterReconnect: the server keys its listen table by
// connectionId, so a reconnect that does not replay leaves the client watching
// nothing while every local signal says it is connected.
func TestConfigReplayAfterReconnect(t *testing.T) {
	stub := newConfigStub("v1", "md5-v1")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	updates := make(chan Config, 8)
	key := ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}
	if err := c.Watch(context.Background(), key, func(cfg Config) { updates <- cfg }); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	<-updates // seed

	// Move the config forward so the md5 the client holds is not the initial one.
	stub.setContent("v2", "md5-v2")
	stub.push <- ConfigChangeNotifyRequest{RequestID: "11", DataID: "routes.json", Group: "DEFAULT_GROUP"}
	got := <-updates
	if got.MD5 != "md5-v2" {
		t.Fatalf("md5 before the disconnect = %q, want md5-v2", got.MD5)
	}
	waitForCond(t, "re-listen before the drop", func() bool {
		return len(stub.rawBodies("ConfigBatchListenRequest")) >= 2
	})

	before := c.ConnectionID()
	listensBefore := len(stub.rawBodies("ConfigBatchListenRequest"))
	stub.Kill() // the server walks away from this stream only

	waitForCond(t, "a new handshake", func() bool {
		id := c.ConnectionID()
		return id != "" && id != before
	})
	// The replay must be a *new* listen and it must carry the md5 the client
	// actually holds — checking only "some listen says md5-v2" would pass on the
	// pre-drop re-listen above, i.e. the assertion would fire on nothing.
	waitForCond(t, "a replayed listen with the current md5", func() bool {
		bodies := stub.rawBodies("ConfigBatchListenRequest")
		return len(bodies) > listensBefore &&
			strings.Contains(bodies[len(bodies)-1], `"md5":"md5-v2"`)
	})
}

// TestListenResponseStaleSubsetIsPulled covers the other half of the sync
// protocol: the batch listen answers immediately with the keys whose md5 the
// server considers stale (ConfigChangeBatchListenRequestHandler.java:82-88).
// Without acting on that list, a change landing between the seed query and the
// listen would wait for a push the server already sent.
func TestListenResponseStaleSubsetIsPulled(t *testing.T) {
	stub := newConfigStub("v1", "md5-v1")
	stub.staleOnListen = true // the server claims every listened key is behind
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)

	updates := make(chan Config, 4)
	key := ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}
	if err := c.Watch(context.Background(), key, func(cfg Config) { updates <- cfg }); err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if seed := <-updates; seed.MD5 != "md5-v1" {
		t.Fatalf("seed = %+v, want md5-v1", seed)
	}
	waitForCond(t, "a pull triggered by the stale listen response", func() bool {
		return len(stub.rawBodies("ConfigQueryRequest")) >= 2
	})
	// The md5 guard still holds: re-pulling an unchanged config must not fire
	// the callback again, or a route table would be applied twice.
	select {
	case extra := <-updates:
		t.Fatalf("callback fired again with %+v; an unchanged md5 must not re-apply", extra)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestAckClassIsARegisteredPayload pins the class names the server can actually
// parse. The old code appended "Response" to the request class, which yields
// NotifySubscriberRequestResponse — not a Nacos class, so the server throws
// (GrpcUtils.java:132-133) and the push is re-sent as if it were never acked.
// Each expectation below is a class in
// api/src/main/resources/META-INF/services/com.alibaba.nacos.api.remote.Payload.
func TestAckClassIsARegisteredPayload(t *testing.T) {
	for push, want := range map[string]string{
		"NotifySubscriberRequest":   "NotifySubscriberResponse",
		"ConfigChangeNotifyRequest": "ConfigChangeNotifyResponse",
		"ClientDetectionRequest":    "ClientDetectionResponse",
		"HealthCheckRequest":        "HealthCheckResponse",
		"ConnectResetRequest":       typeEmptyResponse, // no ConnectResetResponse exists
		"SomeFutureThingRequest":    typeEmptyResponse,
	} {
		if got := ackClassFor(push); got != want {
			t.Errorf("ackClassFor(%q) = %q, want %q", push, got, want)
		}
	}
}

// TestConfigClientIsLabelledAsConfig: the module label is what makes this a
// config-domain connection at all (ConnectionBasedClientManager.java:60-64 shows
// the naming module ignoring any other label), so it must be on the wire.
func TestConfigClientIsLabelledAsConfig(t *testing.T) {
	stub := newConfigStub("v", "m")
	addr := startConfigStub(t, stub)
	c := newTestConfigClient(t, addr)
	if c.ConnectionID() == "" {
		t.Fatal("no connectionId after setup")
	}
	bodies := stub.rawBodies(typeConnectionSetupRequest)
	if len(bodies) == 0 {
		t.Fatal("the stub saw no ConnectionSetupRequest")
	}
	if !strings.Contains(bodies[0], `"module":"config"`) {
		t.Errorf("setup labels should carry module=config: %s", bodies[0])
	}
	if !strings.Contains(bodies[0], `"abilityTable"`) {
		t.Errorf("abilityTable must be present or the server never acks: %s", bodies[0])
	}
}

// TestConfigClientHealsWhenCentreAppears is the reason NewConfigClient does not
// fail on an unreachable server. The gateway's route cache is worthless if a
// cold start against a down config centre kills the process before it can serve
// from disk, so the client must come up degraded, keep the Watch registration,
// and deliver content the moment a server exists — without anyone calling Watch
// again.
func TestConfigClientHealsWhenCentreAppears(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve addr: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	c, err := NewConfigClient(ConfigOptions{Addr: addr, ReconnectBackoff: 50 * time.Millisecond})
	if c == nil {
		t.Fatalf("no client for an unreachable server: %v", err)
	}
	if err == nil {
		t.Error("the first handshake failure should still be reported, even though the client is usable")
	}
	defer func() { _ = c.Close() }()
	if id := c.ConnectionID(); id != "" {
		t.Errorf("ConnectionID = %q before any server exists", id)
	}

	var mu sync.Mutex
	var got Config
	key := ConfigKey{DataID: "routes.json", Group: "DEFAULT_GROUP"}
	if werr := c.Watch(context.Background(), key, func(cfg Config) {
		mu.Lock()
		got = cfg
		mu.Unlock()
	}); werr == nil {
		t.Error("Watch should report the failed seed pull while the centre is down")
	}
	mu.Lock()
	if got.Content != "" {
		mu.Unlock()
		t.Fatalf("callback fired early with %q while nothing was reachable", got.Content)
	}
	mu.Unlock()

	// The config centre appears on the same address. A just-closed loopback port
	// can take a moment to be reusable, so retry the bind rather than skip it.
	var bound net.Listener
	for i := 0; i < 40; i++ {
		if bound, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if bound == nil {
		t.Fatalf("could not rebind %s: %v", addr, err)
	}
	serveConfigStub(t, bound, newConfigStub("v9", "md5-v9"))

	waitWithin(t, 20*time.Second, "the client to heal and deliver the content on its own", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got.Content == "v9" && got.MD5 == "md5-v9"
	})
}
