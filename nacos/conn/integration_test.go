package conn

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// NACOS_GRPC_ADDR opts the tests into talking to a real Nacos server.
// Example: NACOS_GRPC_ADDR=127.0.0.1:9848 go test ./nacos/conn -run Live -v
const envAddr = "NACOS_GRPC_ADDR"

func liveAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s (e.g. 127.0.0.1:9848) to test against a real Nacos", envAddr)
	}
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("%s unreachable: %v", addr, err)
	}
	_ = c.Close()
	return addr
}

// TestLiveSetupHandshakeIsAcknowledged is S2's acceptance: three frames must
// complete, otherwise Setup would hang or time out.
func TestLiveSetupHandshakeIsAcknowledged(t *testing.T) {
	c, err := Dial(liveAddr(t))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !c.Ready() {
		t.Fatal("Ready() false after Setup returned nil")
	}
	t.Logf("handshake ok with %s, abilities=%v", c.Addr(), c.Abilities())
}

// TestLiveUnaryServerCheck proves the unary RPC path (method name + Payload
// envelope) is accepted by a real server; the naming calls ride on top of it.
func TestLiveUnaryServerCheck(t *testing.T) {
	c, err := Dial(liveAddr(t))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := c.CallNoResponse(ctx, ServerCheckRequest{}); err != nil {
		t.Fatalf("ServerCheck: %v", err)
	}
	t.Logf("unary %s accepted by %s", ServerCheckRequest{}.typeName(), c.Addr())
}

// TestLiveSetupIsNotTornDownByCallerTimeout pins the fix that the bidi stream
// hangs off the connection's context: a caller whose Setup deadline expires
// must not kill an already-established connection.
func TestLiveSetupIsNotTornDownByCallerTimeout(t *testing.T) {
	c, err := Dial(liveAddr(t))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	tight, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := c.Setup(tight, "public", nil); err != nil && !strings.Contains(err.Error(), "context deadline") {
		t.Fatalf("Setup with 1ms budget should report the deadline, got %v", err)
	}

	// Second attempt on the same Conn must still be able to finish.
	ctx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup retry: %v", err)
	}
	if err := c.CallNoResponse(ctx, ServerCheckRequest{}); err != nil {
		t.Fatalf("ServerCheck after retry: %v", err)
	}
}

// TestMarshalEnvelopeShape pins the wire facts that would otherwise be guesses:
// type is the Java simple name, clientIp is a top-level metadata field
// (camelCase, not client_ip), and the body is the object's JSON.
func TestMarshalEnvelopeShape(t *testing.T) {
	p, err := marshalRequest(ConnectionSetupRequest{
		ClientVersion: ClientVersion,
		Tenant:        "public",
		Labels:        map[string]string{"source": "sdk"},
	}, "10.0.0.1")
	if err != nil {
		t.Fatalf("marshalRequest: %v", err)
	}
	if got := p.GetMetadata().GetType(); got != "ConnectionSetupRequest" {
		t.Fatalf("type = %q", got)
	}
	if got := p.GetMetadata().GetClientIp(); got != "10.0.0.1" {
		t.Fatalf("clientIp = %q", got)
	}
	body := string(p.GetBody().GetValue())
	for _, want := range []string{`"clientVersion"`, `"tenant":"public"`, `"labels":{"source":"sdk"}`} {
		if !strings.Contains(body, want) {
			t.Fatalf("body %q missing %s", body, want)
		}
	}
	if strings.Contains(body, "headers") {
		t.Fatalf("body must not carry the headers map (GrpcUtils.java:108-113): %s", body)
	}
}
