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
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/javahongxi/sweet/gen"
)

// probeStub is a minimal Nacos server for behaviour tests: unary serverCheck,
// bidi setup → SetupAckRequest, HealthCheckRequest → HealthCheckResponse, and an
// optional "this stream is over" right after the ack.
type probeStub struct {
	mu             sync.Mutex
	dropAfterSetup bool
	setups         int
	dropCount      int
	health         int
}

func (s *probeStub) drops() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropCount
}

func (s *probeStub) healthChecks() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health
}

func (s *probeStub) bidi(stream grpc.BidiStreamingServer[gen.Payload, gen.Payload]) error {
	for {
		p, err := stream.Recv()
		if err != nil {
			return nil
		}
		t, _ := unmarshal(p, nil)
		switch t {
		case typeConnectionSetupRequest:
			var req ConnectionSetupRequest
			_, _ = unmarshal(p, &req)
			s.mu.Lock()
			s.setups++
			drop := s.dropAfterSetup
			s.mu.Unlock()
			if req.AbilityTable == nil {
				continue // faithful: no ack for a client that omits the table
			}
			if err := stream.Send(frame(typeSetupAckRequest, SetupAckRequest{
				AbilityTable: map[string]bool{"fuzzyWatch": true},
			})); err != nil {
				return err
			}
			if drop {
				time.Sleep(150 * time.Millisecond) // let the client's ack land
				s.mu.Lock()
				s.dropCount++
				s.mu.Unlock()
				return nil
			}
		case typeHealthCheckRequest:
			s.mu.Lock()
			s.health++
			s.mu.Unlock()
			if err := stream.Send(frame("HealthCheckResponse", map[string]any{"success": true})); err != nil {
				return err
			}
		}
	}
}

func (s *probeStub) unary(p *gen.Payload) *gen.Payload {
	t, _ := unmarshal(p, nil)
	if t == typeHealthCheckRequest {
		s.mu.Lock()
		s.health++
		s.mu.Unlock()
		return frame("HealthCheckResponse", map[string]any{"success": true})
	}
	if t == typeServerCheckRequest {
		return frame("ServerCheckResponse", map[string]any{
			"resultCode": 200, "success": true, "connectionId": "probe-connection-1",
		})
	}
	return frame(typeEmptyResponse, map[string]any{"success": true})
}

func frame(t string, body any) *gen.Payload {
	b, _ := json.Marshal(body)
	return &gen.Payload{Metadata: &gen.Metadata{Type: t}, Body: &anypb.Any{Value: b}}
}

func startProbeStub(t *testing.T, s *probeStub) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
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
				return h.(*probeStub).unary(in), nil
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
				return h.(*probeStub).bidi(
					&grpc.GenericServerStream[gen.Payload, gen.Payload]{ServerStream: st})
			},
		}},
	}, s)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// --- package-wide test helpers ----------------------------------------------
//
// These live here rather than in a domain's test file because every suite in the
// package uses them: auth_test.go, naming_client_test.go, config_client_test.go.
// Keeping them in one harness file is what stops "which file owns waitWithin" from
// being decided by whoever added it first.

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

// waitWithin polls cond until it holds or the budget runs out. Assertions in this
// package are about ordering and side effects that land asynchronously, so a bare
// sleep would either flake or hide a real failure behind a long wait.
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
