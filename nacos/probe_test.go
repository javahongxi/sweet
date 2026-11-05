package nacos

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
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
	c, err := dialConn(addr)
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
	c, err := dialConn(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Do not use openStream here: it starts readLoop, and a gRPC stream must
	// have only one receiver. This raw probe owns the stream so it can inspect
	// the setup ack that readLoop would otherwise consume internally. The stream
	// context also releases a blocked RecvMsg without leaving a goroutine behind.
	cs, err := c.cc.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "requestBiStream",
		ServerStreams: true,
		ClientStreams: true,
	}, methodBidi)
	if err != nil {
		t.Fatalf("open raw stream: %v", err)
	}
	setup := ConnectionSetupRequest{ClientVersion: clientVersion, Tenant: "public",
		Labels: map[string]string{"source": "sdk", "module": "naming"}}
	p, err := marshalRequest(setup, "127.0.0.1")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := cs.SendMsg(p); err != nil {
		t.Fatalf("send: %v", err)
	}
	for {
		var frame gen.Payload
		if err := cs.RecvMsg(&frame); err != nil {
			if ctx.Err() != nil {
				t.Log("probe window closed: no further frame")
			} else {
				t.Logf("recv ended: %v", err)
			}
			return
		}
		meta, _ := json.Marshal(frame.GetMetadata())
		t.Logf("frame: metadata=%s body=%s", meta, frame.GetBody().GetValue())
	}
}

var _ = anypb.Any{}
