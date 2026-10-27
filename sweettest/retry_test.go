package sweettest

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/example/helloworld/ping"
	"github.com/javahongxi/sweet/registry/direct"
	"github.com/javahongxi/sweet/server"
)

// retryServiceConfig is stock gRPC service config: sweet adds no retry knob of
// its own. It reaches the channel through grpc.WithDefaultServiceConfig because
// sweet's resolver returns addresses and no service config, which is exactly the
// case grpc-go applies a default for.
const retryServiceConfig = `{"loadBalancingConfig":[{"round_robin":{}}],` +
	`"methodConfig":[{"name":[{}],"retryPolicy":{"maxAttempts":5,` +
	`"initialBackoff":"0.001s","maxBackoff":"0.01s","backoffMultiplier":2,` +
	`"retryableStatusCodes":["UNAVAILABLE"]}}]}`

// retryServer exports the hand-written ping service, whose unary handler routes
// through the interceptor grpc-go hands it — without that, the injection below
// would silently do nothing and both tests here would pass for the wrong reason.
func retryServer(t *testing.T, failFor int32, attempts *int32) *server.Server {
	t.Helper()
	srv, err := server.New(server.Options{
		Address:     "127.0.0.1:0",
		ServiceName: ping.ServiceName,
		Registry:    direct.Nope(),
		DrainDelay:  10 * time.Millisecond,
		ServerOptions: []grpc.ServerOption{grpc.UnaryInterceptor(
			func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
				if atomic.AddInt32(attempts, 1) <= failFor {
					return nil, status.Error(codes.Unavailable, "injected")
				}
				return h(ctx, req)
			},
		)},
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if err := srv.Register(&ping.ServiceDesc, pingServiceImpl{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	go func() { _ = srv.Serve() }()
	if err := srv.Listen(); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(srv.Stop)
	return srv
}

type pingServiceImpl struct{}

func (pingServiceImpl) Ping(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String("pong"), nil
}
func (pingServiceImpl) Events(*wrapperspb.StringValue, grpc.ServerStreamingServer[wrapperspb.StringValue]) error {
	return nil
}
func (pingServiceImpl) Echo(grpc.BidiStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error {
	return nil
}
func (pingServiceImpl) Ack(grpc.ClientStreamingServer[wrapperspb.StringValue, wrapperspb.StringValue]) error {
	return nil
}

func pingOnce(ctx context.Context, conn *grpc.ClientConn) (string, error) {
	out := new(wrapperspb.StringValue)
	if err := conn.Invoke(ctx, "/"+ping.ServiceName+"/Ping", wrapperspb.String("hi"), out); err != nil {
		return "", err
	}
	return out.GetValue(), nil
}

// TestNativeRetryAppliesThroughSweetDial: gRPC's own retry works over a channel
// dialled by sweet. If it ever stops working, the resolver and grpc's config
// plumbing are where to look — the fix is not a retry implementation in sweet.
func TestNativeRetryAppliesThroughSweetDial(t *testing.T) {
	var attempts int32
	const failFor = int32(2)
	srv := retryServer(t, failFor, &attempts)

	peer, err := direct.New(srv.Addr())
	if err != nil {
		t.Fatalf("direct.New: %v", err)
	}
	conn, err := client.Dial(peer, ping.ServiceName,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(retryServiceConfig))
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := pingOnce(ctx, conn)
	if err != nil {
		t.Fatalf("call with retry policy failed: %v", err)
	}
	if want := failFor + 1; attempts != want {
		t.Fatalf("server saw %d attempts, want %d: the injection is not doing what the test assumes", attempts, want)
	}
	t.Logf("recovered with gRPC-native retry: %q after %d server-side attempts", got, attempts)
}

// TestWithoutRetryConfigTheInjectionSticks is the negative control: the same
// injection must fail the call when no retry policy is configured. Without it,
// the test above could pass simply because the interceptor never fired.
func TestWithoutRetryConfigTheInjectionSticks(t *testing.T) {
	var attempts int32
	srv := retryServer(t, 2, &attempts)

	peer, err := direct.New(srv.Addr())
	if err != nil {
		t.Fatalf("direct.New: %v", err)
	}
	conn, err := client.Dial(peer, ping.ServiceName,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = pingOnce(ctx, conn)
	if err == nil {
		t.Fatalf("call succeeded without a retry policy, so the injection did nothing (attempts=%d)", attempts)
	}
	if !strings.Contains(err.Error(), codes.Unavailable.String()) {
		t.Fatalf("error = %v, want it to surface the injected UNAVAILABLE", err)
	}
	t.Logf("no retry policy → caller sees the failure after %d server-side attempt(s): %v", attempts, err)
}
