// Package ping is a hand-written gRPC service used by sweet's example: no
// protoc, no generated stubs. It carries one unary method and one
// server-streaming method over wrapperspb.StringValue.
package ping

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ServiceName is the gRPC service name, i.e. "<package>.<Service>".
const ServiceName = "sweet.example.v1.Ping"

// Server is implemented by the example server.
type Server interface {
	Ping(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
	Events(*wrapperspb.StringValue, grpc.ServerStreamingServer[wrapperspb.StringValue]) error
}

// ServiceDesc wires Server to grpc-go. Register it with sweet's server.Register.
var ServiceDesc = grpc.ServiceDesc{
	ServiceName: ServiceName,
	HandlerType: (*Server)(nil),
	Metadata:    "sweet/example/ping.proto",
	Methods: []grpc.MethodDesc{{
		MethodName: "Ping",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.(Server).Ping(ctx, in)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Events",
		ServerStreams: true,
		Handler: func(srv any, stream grpc.ServerStream) error {
			in := new(wrapperspb.StringValue)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			return srv.(Server).Events(in,
				&grpc.GenericServerStream[wrapperspb.StringValue, wrapperspb.StringValue]{ServerStream: stream})
		},
	}},
}

// Client calls the service over a grpc.ClientConn.
type Client struct{ cc grpc.ClientConnInterface }

// NewClient builds a client for conn.
func NewClient(cc grpc.ClientConnInterface) *Client { return &Client{cc: cc} }

// Ping performs the unary call.
func (c *Client) Ping(ctx context.Context, msg string) (string, error) {
	out := new(wrapperspb.StringValue)
	if err := c.cc.Invoke(ctx, "/"+ServiceName+"/Ping", wrapperspb.String(msg), out); err != nil {
		return "", err
	}
	return out.GetValue(), nil
}

// Events performs the server-streaming call and returns every event.
func (c *Client) Events(ctx context.Context, csv string) ([]string, error) {
	cs, err := c.cc.NewStream(ctx, &ServiceDesc.Streams[0], "/"+ServiceName+"/Events")
	if err != nil {
		return nil, err
	}
	stream := &grpc.GenericClientStream[wrapperspb.StringValue, wrapperspb.StringValue]{ClientStream: cs}
	if err := stream.Send(wrapperspb.String(csv)); err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	var got []string
	for {
		m, err := stream.Recv()
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				return got, nil
			}
			return got, err
		}
		got = append(got, m.GetValue())
	}
}
