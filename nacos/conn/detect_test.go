package conn

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestProbeDetectsSilentlyEndedStream is the measurement that decided this
// design. Against a server that ends *only* the bidi stream, both signals gRPC
// could have given us are absent:
//
//	RecvMsg  → keeps blocking (no per-stream close surfaced to the client)
//	SendMsg  → keeps returning nil (writes into a stream already gone)
//
// So the only reliable detector is a probe whose answer never arrives, which is
// also what Nacos' own contract says: "失败 → UNHEALTHY → 重连"
// (jaws/doc/harbor-vs-nacos.md §8.3). Without it a client happily keeps a
// registration the server removed 20–40s ago — the exact bug that showed up as
// "instance gone from the console while the process is alive".
func TestProbeDetectsSilentlyEndedStream(t *testing.T) {
	f := &probeStub{dropAfterSetup: true}
	addr := startProbeStub(t, f)

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	lost := make(chan error, 1)
	c.SetStreamLostHook(func(cause error) {
		select {
		case lost <- cause:
		default:
		}
	})
	c.KeepAlive = 200 * time.Millisecond
	c.ServerCheckTimeout = 500 * time.Millisecond // probe answer window

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !c.Ready() {
		t.Fatalf("Ready() false right after handshake: %v", c.StreamErr())
	}
	t.Logf("connected: connectionId=%s ready=%v", c.ConnectionID(), c.Ready())

	select {
	case cause := <-lost:
		t.Logf("loss detected: %v", cause)
		if c.Ready() {
			t.Fatal("Ready() true after a reported loss")
		}
		if c.ConnectionID() != "" {
			t.Fatal("stale connectionId retained after loss")
		}
		// Recovery on the same Conn/channel: new stream, new handshake, epoch+.1
		c.CloseStream()
		if err := c.Setup(ctx, "public", nil); err != nil {
			t.Fatalf("re-Setup after loss: %v", err)
		}
		if !c.Ready() {
			t.Fatal("Ready() false after recovery")
		}
		if c.Epoch() < 2 {
			t.Fatalf("Epoch() = %d, want >= 2 after one recovery", c.Epoch())
		}
		if !c.Ready() {
			t.Fatal("not ready after re-handshake")
		}
	case <-time.After(8 * time.Second):
		t.Fatalf("nothing detected: ready=%v streamErr=%v drops=%d",
			c.Ready(), c.StreamErr(), f.drops())
	}
}

// TestProbeIsAnsweredByServer keeps the stub honest: an answered probe must not
// tear the connection down, or every idle client would reconnect forever.
func TestProbeIsAnsweredByServer(t *testing.T) {
	f := &probeStub{}
	addr := startProbeStub(t, f)
	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	c.KeepAlive = 150 * time.Millisecond
	c.ServerCheckTimeout = time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if !c.Ready() {
		t.Fatalf("answered probes killed the connection: %v", c.StreamErr())
	}
	if f.healthChecks() == 0 {
		t.Fatal("no HealthCheckRequest reached the server during 1.5s of idleness")
	}
	t.Logf("idle probes answered: %d HealthCheckRequest, connection still ready", f.healthChecks())
}

// TestLiveProbeChannelFacts records where a real 3.2.3 server answers the
// health probe. Measured: on the bidi stream it goes unanswered, on the unary
// channel it is answered — which is why keepAlive() uses /Request/request and
// mirrors RpcClient.healthCheck() calling currentConnection.request(...).
func TestLiveProbeChannelFacts(t *testing.T) {
	addr := os.Getenv("NACOS_GRPC_ADDR")
	if addr == "" {
		t.Skip("set NACOS_GRPC_ADDR=127.0.0.1:9848")
	}
	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	if _, _, err := c.Unary(ctx, HealthCheckRequest{RequestID: c.nextRequestID()}); err != nil {
		t.Fatalf("unary probe should be answered: %v", err)
	}
	t.Log("unary HealthCheckRequest answered")

	// The unary answer does not necessarily imply the stream one works; on the
	// live server it does not, so keepAlive must not rely on it.
	p, err := marshalRequest(HealthCheckRequest{RequestID: c.nextRequestID()}, c.localIP())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.send(p); err != nil {
		t.Fatalf("stream probe send: %v", err)
	}
	quiet := time.After(3 * time.Second)
	for {
		select {
		case push := <-c.Pushes():
			if push.Type == typeHealthCheckResponse {
				t.Fatal("server did answer on the stream after all — revisit keepAlive's channel")
			}
			continue // unrelated pushes are fine
		case <-quiet:
			t.Log("no stream answer within 3s; keepAlive stays on the unary channel")
			return
		}
	}
}
