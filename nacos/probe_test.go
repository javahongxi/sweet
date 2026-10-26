package nacos

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/javahongxi/sweet/gen"
)

// TestProbeUnaryWithoutSetup answers the first question when a handshake hangs:
// is the envelope even understood? A unary ServerCheck needs no setup, so if it
// returns a Nacos-shaped answer, framing is fine and the fault is in the bidi
// handshake path.
func TestProbeUnaryWithoutSetup(t *testing.T) {
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s", envAddr)
	}
	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := marshalRequest(ServerCheckRequest{}, "127.0.0.1")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out gen.Payload
	if err := c.cc.Invoke(ctx, methodUnary, p, &out); err != nil {
		t.Fatalf("unary ServerCheck without setup: %v", err)
	}
	b, _ := json.Marshal(out.GetMetadata())
	t.Logf("reply metadata=%s body=%s", b, out.GetBody().GetValue())
}

// TestProbeBidiFramesDumpsEverythingReceived shows every frame the server
// pushes on the stream after we send ConnectionSetupRequest — the ack may
// simply be a type this client does not recognise yet.
func TestProbeBidiFramesDumpsEverythingReceived(t *testing.T) {
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s", envAddr)
	}
	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if err := c.openStream(); err != nil {
		t.Fatalf("openStream: %v", err)
	}
	setup := ConnectionSetupRequest{ClientVersion: ClientVersion, Tenant: "public",
		Labels: map[string]string{"source": "sdk", "module": "naming"}}
	p, err := marshalRequest(setup, "127.0.0.1")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := c.send(p); err != nil {
		t.Fatalf("send: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var frame gen.Payload
		rctx, rcancel := context.WithTimeout(ctx, time.Second)
		_ = rctx
		cs := c.stream
		rcancel()
		if cs == nil {
			break
		}
		done := make(chan error, 1)
		go func() { done <- cs.RecvMsg(&frame) }()
		select {
		case err := <-done:
			if err != nil {
				t.Logf("recv ended: %v", err)
				return
			}
			meta, _ := json.Marshal(frame.GetMetadata())
			t.Logf("frame: metadata=%s body=%s", meta, frame.GetBody().GetValue())
		case <-time.After(2 * time.Second):
			t.Log("no further frame within 2s")
			return
		}
	}
}

var _ = anypb.Any{}
