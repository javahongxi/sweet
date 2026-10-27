package ping

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type stubServer struct{}

func (stubServer) Ping(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String("pong:" + in.GetValue()), nil
}
func (stubServer) Events(*wrapperspb.StringValue, grpc.ServerStreamingServer[wrapperspb.StringValue]) error {
	return nil
}
func (stubServer) Echo(grpc.BidiStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error {
	return nil
}
func (stubServer) Ack(grpc.ClientStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error {
	return nil
}

// decodeWith stands in for grpc-go's message decoder: the handler must call it to
// get the request out of the wire. Only the payload field is copied — a protobuf
// message carries a mutex, so assigning the struct would be a vet-level bug.
func decodeWith(in *wrapperspb.StringValue) func(any) error {
	return func(target any) error {
		dst, ok := target.(*wrapperspb.StringValue)
		if !ok {
			return errors.New("unexpected decode target")
		}
		dst.Value = in.GetValue()
		return nil
	}
}

// The hand-written descriptor has to route unary calls through the interceptor
// grpc-go hands it — that chain is not applied further up the stack, so a
// descriptor which drops the argument serves RPCs with every server-side unary
// interceptor silently skipped. Tested against the descriptor's handler directly
// because that is exactly where the omission would come back.
func TestUnaryHandlerInvokesTheInterceptor(t *testing.T) {
	var seen *grpc.UnaryServerInfo
	handlerRan := false

	interceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		seen = info
		out, err := h(ctx, req)
		handlerRan = true
		return out, err
	}

	reply, err := ServiceDesc.Methods[0].Handler(
		stubServer{}, context.Background(), decodeWith(wrapperspb.String("hi")), interceptor)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := reply.(*wrapperspb.StringValue).GetValue(); got != "pong:hi" {
		t.Fatalf("reply = %q, want %q", got, "pong:hi")
	}
	if seen == nil {
		t.Fatal("the unary interceptor was never called: the descriptor drops the argument grpc-go hands it")
	}
	if !handlerRan {
		t.Fatal("interceptor ran but never called through to the service method")
	}
	if want := "/" + ServiceName + "/Ping"; seen.FullMethod != want {
		t.Fatalf("FullMethod = %q, want %q", seen.FullMethod, want)
	}
	if seen.Server == nil {
		t.Fatal("UnaryServerInfo.Server is empty; interceptors that dispatch on it would break")
	}
}

// grpc-go passes a nil interceptor when none is configured; the handler must then
// call the method itself instead of dereferencing nil.
func TestUnaryHandlerWorksWithoutInterceptors(t *testing.T) {
	reply, err := ServiceDesc.Methods[0].Handler(
		stubServer{}, context.Background(), decodeWith(wrapperspb.String("x")), nil)
	if err != nil {
		t.Fatalf("handler with nil interceptor: %v", err)
	}
	if got := reply.(*wrapperspb.StringValue).GetValue(); got != "pong:x" {
		t.Fatalf("reply = %q, want %q", got, "pong:x")
	}
}

// An interceptor's error must surface unchanged, since the framework's error
// classification (errors.As down a %w chain) depends on it.
func TestUnaryHandlerPropagatesInterceptorError(t *testing.T) {
	sentinel := errors.New("sentinel")
	_, err := ServiceDesc.Methods[0].Handler(
		stubServer{}, context.Background(), decodeWith(wrapperspb.String("x")),
		func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
			return nil, sentinel
		})
	if !errors.Is(err, sentinel) {
		t.Fatalf("errors.Is(err, sentinel) = false for %v", err)
	}
}
