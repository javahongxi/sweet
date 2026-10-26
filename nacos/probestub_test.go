package nacos

import (
	"context"
	"encoding/json"
	"net"
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
