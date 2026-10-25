package conn

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/javahongxi/sweet/gen"
)

// fakeNacos is an in-process stand-in that follows the protocol shape verified
// in nacos @3.2.3: the server answers a ConnectionSetupRequest with
// SetupAckRequest, then waits for SetupAckResponse before considering the
// connection usable (GrpcClient.java:607-625 is the client half of that).
type fakeNacos struct {
	mu            sync.Mutex
	setupsSeen    []ConnectionSetupRequest
	metaTypes     []string // Metadata.type of every frame the server received
	unaryTypes    []string
	unaryClientIP string
	handshakes    int
	pushAfterACK  bool
}

func (f *fakeNacos) requestBiStream(stream grpc.BidiStreamingServer[gen.Payload, gen.Payload]) error {
	var setupSeen *gen.Payload
	for {
		p, err := stream.Recv()
		if err != nil {
			return nil
		}
		t, err := unmarshal(p, nil)
		if err != nil {
			continue
		}
		f.mu.Lock()
		f.metaTypes = append(f.metaTypes, t)
		f.mu.Unlock()

		switch t {
		case typeConnectionSetupRequest:
			var req ConnectionSetupRequest
			_, _ = unmarshal(p, &req)
			f.mu.Lock()
			f.setupsSeen = append(f.setupsSeen, req)
			f.mu.Unlock()
			// Faithful to GrpcBiStreamRequestAcceptor.java:206 — the server
			// stays silent unless the client's abilityTable is non-null, which
			// is exactly the bug this stub must be able to catch.
			if req.AbilityTable == nil {
				continue
			}
			ack := marshalServerFrame(typeSetupAckRequest, SetupAckRequest{
				AbilityTable: map[string]bool{"remoteRegionDisable": false},
			})
			if err := stream.Send(ack); err != nil {
				return err
			}
			setupSeen = p
		case typeSetupAckResponse:
			if setupSeen == nil {
				// Out of order: a client that acks before setting up.
				return nil
			}
			f.mu.Lock()
			f.handshakes++
			push := f.pushAfterACK
			f.mu.Unlock()
			if push {
				if err := stream.Send(marshalServerFrame(typeNotifySubscriberRequest, map[string]any{
					"serviceName": "sweet.example.v1.Ping",
				})); err != nil {
					return err
				}
			}
		}
	}
}

func (f *fakeNacos) request(_ context.Context, p *gen.Payload) (*gen.Payload, error) {
	t, _ := unmarshal(p, nil)
	f.mu.Lock()
	f.unaryTypes = append(f.unaryTypes, t)
	f.unaryClientIP = p.GetMetadata().GetClientIp()
	f.mu.Unlock()
	return marshalServerFrame(typeEmptyResponse, map[string]any{"success": true}), nil
}

// marshalServerFrame builds the server's reply shape: Nacos'
// convert(Response) sets Metadata.type only (GrpcUtils.java:98-106).
func marshalServerFrame(t string, body any) *gen.Payload {
	b, _ := json.Marshal(body)
	return &gen.Payload{
		Metadata: &gen.Metadata{Type: t},
		Body:     &anypb.Any{Value: b},
	}
}

var fakeDesc = grpc.ServiceDesc{
	ServiceName: "Request",
	HandlerType: (*any)(nil),
	Metadata:    "nacos_grpc_service.proto",
	Methods: []grpc.MethodDesc{{
		MethodName: "request",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(gen.Payload)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.(*fakeNacos).request(ctx, in)
		},
	}},
}

var fakeBidiDesc = grpc.ServiceDesc{
	ServiceName: "BiRequestStream",
	HandlerType: (*any)(nil),
	Metadata:    "nacos_grpc_service.proto",
	Streams: []grpc.StreamDesc{{
		StreamName:    "requestBiStream",
		ServerStreams: true,
		ClientStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			return srv.(*fakeNacos).requestBiStream(
				&grpc.GenericServerStream[gen.Payload, gen.Payload]{ServerStream: stream})
		},
	}},
}

func (f *fakeNacos) completedHandshakes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.handshakes
}

func (f *fakeNacos) setupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.setupsSeen)
}

func (f *fakeNacos) unaryCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.unaryTypes)
}

func (f *fakeNacos) snapshotTypes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.metaTypes...)
}

// waitFor polls cond until it reports want or 3s elapse.
func waitFor(cond func() int, want int) bool { return waitForCount(cond, want) >= want }

func waitForCount(cond func() int, want int) int {
	deadline := time.Now().Add(3 * time.Second)
	for {
		if n := cond(); n >= want {
			return n
		}
		if time.Now().After(deadline) {
			return cond()
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startFakeNacos(t *testing.T, f *fakeNacos) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	s.RegisterService(&fakeDesc, f)
	s.RegisterService(&fakeBidiDesc, f)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// TestHandshakeCompletesThreeFrames is the point of S2: Setup returns only once
// the server's SetupAckRequest has been received (and we answer it the way
// Nacos' own client does).
func TestHandshakeCompletesThreeFrames(t *testing.T) {
	f := &fakeNacos{}
	addr := startFakeNacos(t, f)

	c, err := Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	// Setup returning only proves we *sent* the third frame; the server
	// processes it asynchronously, so its counters need a wait, not a glance.
	// (Asserting straight after Setup is the same "green but never crossed the
	// wire" mistake this whole step exists to avoid.)
	if !waitFor(f.completedHandshakes, 1) {
		t.Fatalf("server never saw a completed handshake (frames: %v)", f.snapshotTypes())
	}
	if got := waitForCount(f.setupCount, 1); got != 1 {
		t.Fatalf("setups seen = %d, want 1", got)
	}
	f.mu.Lock()
	setup := f.setupsSeen[0]
	meta := strings.Join(f.metaTypes, ",")
	f.mu.Unlock()
	if setup.ClientVersion != ClientVersion {
		t.Fatalf("clientVersion = %q", setup.ClientVersion)
	}
	if setup.Tenant != "public" {
		t.Fatalf("tenant = %q, want public", setup.Tenant)
	}
	if setup.Labels["source"] != "sdk" || setup.Labels["module"] != "naming" {
		t.Fatalf("labels = %v, want source=sdk module=naming (RemoteConstants.java:32,40)", setup.Labels)
	}
	if want := typeConnectionSetupRequest + "," + typeSetupAckResponse; meta != want {
		t.Fatalf("frames the server received = %q, want %q", meta, want)
	}
	if len(c.Abilities()) == 0 {
		t.Fatal("abilities from SetupAck were dropped")
	}
}

// TestUnaryCarriesTypeAndClientIp pins what a naming call puts on the wire.
func TestUnaryCarriesTypeAndClientIp(t *testing.T) {
	f := &fakeNacos{}
	addr := startFakeNacos(t, f)

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := c.CallNoResponse(ctx, ServerCheckRequest{}); err != nil {
		t.Fatalf("CallNoResponse: %v", err)
	}

	if !waitFor(f.unaryCount, 1) {
		t.Fatalf("server saw no unary call (types: %v)", f.snapshotTypes())
	}
	f.mu.Lock()
	types := strings.Join(f.unaryTypes, ",")
	ip := f.unaryClientIP
	f.mu.Unlock()
	if types != typeServerCheckRequest {
		t.Fatalf("unary types = %q, want %s", types, typeServerCheckRequest)
	}
	if ip == "" {
		t.Fatal("Metadata.clientIp was empty on the unary call")
	}
}

// TestPushSurfacesOnChannel proves the read loop does not swallow non-handshake
// frames — the plumbing S4 will build on.
func TestPushSurfacesOnChannel(t *testing.T) {
	f := &fakeNacos{pushAfterACK: true}
	addr := startFakeNacos(t, f)

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	select {
	case push, ok := <-c.Pushes():
		if !ok {
			t.Fatal("push channel closed")
		}
		if push.Type != typeNotifySubscriberRequest {
			t.Fatalf("push type = %q, want %s", push.Type, typeNotifySubscriberRequest)
		}
		if !strings.Contains(string(push.Body), "sweet.example.v1.Ping") {
			t.Fatalf("push body = %s", push.Body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no push arrived")
	}
}

// TestSetupSendsNonNullAbilityTable is the regression the real server taught
// us: with "abilityTable" omitted, Nacos answers nothing at all and the client
// hangs until its setup timeout.
func TestSetupSendsNonNullAbilityTable(t *testing.T) {
	f := &fakeNacos{}
	addr := startFakeNacos(t, f)

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	f.mu.Lock()
	setup := f.setupsSeen[0]
	f.mu.Unlock()
	if setup.AbilityTable == nil {
		t.Fatal("setup carried a nil abilityTable; the server would never answer")
	}
}
