// Package ping is a hand-written gRPC service used by sweet's example: no
// protoc, no generated stubs. It carries all four gRPC method shapes over
// wrapperspb.StringValue — Ping (unary), Events (server-streaming), Echo
// (bidirectional) and Ack (client-streaming) — so that the only difference from
// example/greeter is hand-written vs protoc-generated stubs.
package ping

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ServiceName is the gRPC service name, i.e. "<package>.<Service>".
const ServiceName = "sweet.example.v1.Ping"

// Server is implemented by the example server. These are the signatures the
// generated code would have produced: grpc-go's stream types are interfaces, so
// writing them out here is exactly what protoc-gen-go-grpc does for you.
type Server interface {
	Ping(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
	Events(*wrapperspb.StringValue, grpc.ServerStreamingServer[wrapperspb.StringValue]) error
	Echo(grpc.BidiStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error
	Ack(grpc.ClientStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error
}

// ServiceDesc wires Server to grpc-go. Register it with sweet's server.Register.
var ServiceDesc = grpc.ServiceDesc{
	ServiceName: ServiceName,
	HandlerType: (*Server)(nil),
	Metadata:    "sweet/example/ping.proto",
	Methods: []grpc.MethodDesc{{
		MethodName: "Ping",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			// grpc-go hands the chained unary interceptor to the handler and the
			// handler is the one that has to call it — this is not applied further
			// up the stack, so a hand-written desc that drops this argument serves
			// RPCs with every server-side unary interceptor silently skipped. Same
			// shape protoc-gen-go-grpc emits.
			if interceptor == nil {
				return srv.(Server).Ping(ctx, in)
			}
			info := &grpc.UnaryServerInfo{
				Server:     srv,
				FullMethod: "/" + ServiceName + "/Ping",
			}
			return interceptor(ctx, in, info, func(ctx context.Context, req any) (any, error) {
				return srv.(Server).Ping(ctx, req.(*wrapperspb.StringValue))
			})
		},
	}},
	Streams: []grpc.StreamDesc{
		{
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
		},
		{
			// Bidirectional: nothing to decode up front — both directions are
			// message-by-message, so the handler gets the raw stream and the
			// generic wrapper is what gives it typed Recv/Send.
			StreamName:    "Echo",
			ClientStreams: true,
			ServerStreams: true,
			Handler: func(srv any, stream grpc.ServerStream) error {
				return srv.(Server).Echo(
					&grpc.GenericServerStream[wrapperspb.StringValue, wrapperspb.StringValue]{ServerStream: stream})
			},
		},
		{
			// Client-streaming: one reply out, so the handler's return is the
			// SendAndClose, not a value it hands back to grpc-go.
			StreamName:    "Ack",
			ClientStreams: true,
			Handler: func(srv any, stream grpc.ServerStream) error {
				return srv.(Server).Ack(
					&grpc.GenericServerStream[wrapperspb.StringValue, wrapperspb.StringValue]{ServerStream: stream})
			},
		},
	},
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

// Events performs the server-streaming call, handing each event to onEvent as it
// arrives. The recv loop stays inside this hand-written client — that is the point
// of the package — but delivery is incremental, so a caller can show frames landing
// one by one instead of printing a collected slice after the fact.
//
// io.EOF from Recv is the server closing the stream and is reported as a nil error;
// errors.Is is what distinguishes that from an error that merely mentions EOF, which
// a substring test would swallow.
func (c *Client) Events(ctx context.Context, csv string, onEvent func(string)) error {
	cs, err := c.cc.NewStream(ctx, &ServiceDesc.Streams[0], "/"+ServiceName+"/Events")
	if err != nil {
		return err
	}
	stream := &grpc.GenericClientStream[wrapperspb.StringValue, wrapperspb.StringValue]{ClientStream: cs}
	if err := stream.Send(wrapperspb.String(csv)); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		onEvent(m.GetValue())
	}
}

// Echo performs the bidirectional call: every value goes out as its own request
// message and every reply is handed to onEvent as it arrives.
//
// The requests are all sent before any reply is read here, which is safe for a
// handful of small messages — they fit in the flow-control window, so the send
// loop cannot block while nothing is reading. A conversation that must truly
// interleave needs a second goroutine for the sending side; the generated stub
// does not spare you that either, it only spares you the stream plumbing below.
func (c *Client) Echo(ctx context.Context, values []string, onEvent func(string)) error {
	cs, err := c.cc.NewStream(ctx, &ServiceDesc.Streams[1], "/"+ServiceName+"/Echo")
	if err != nil {
		return err
	}
	stream := &grpc.GenericClientStream[wrapperspb.StringValue, wrapperspb.StringValue]{ClientStream: cs}
	for _, v := range values {
		if err := stream.Send(wrapperspb.String(v)); err != nil {
			return err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		onEvent(m.GetValue())
	}
}

// Ack performs the client-streaming call: many request messages in, one reply
// out. CloseSend is what tells the server the input is complete, and unlike the
// two streaming methods there is no io.EOF to wait for — the first Recv is the
// answer, so a stream that closes without one is a failed call rather than an
// empty result. That is the same conversion the generated stub makes, where
// io.EOF is rewritten to io.ErrUnexpectedEOF.
func (c *Client) Ack(ctx context.Context, values []string) (string, error) {
	cs, err := c.cc.NewStream(ctx, &ServiceDesc.Streams[2], "/"+ServiceName+"/Ack")
	if err != nil {
		return "", err
	}
	stream := &grpc.GenericClientStream[wrapperspb.StringValue, wrapperspb.StringValue]{ClientStream: cs}
	for _, v := range values {
		if err := stream.Send(wrapperspb.String(v)); err != nil {
			return "", err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return "", err
	}
	m, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return "", errors.New("ping: " + ServiceName + "/Ack closed without a reply message")
	}
	if err != nil {
		return "", err
	}
	return m.GetValue(), nil
}
