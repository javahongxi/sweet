package nacos

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestGuardConvertsPanicToStreamLoss proves the contract of Conn.guard, which
// readLoop and keepAlive both defer: a panic in one of the connection's
// long-lived goroutines must surface as a stream loss (the event the owner
// already turns into a reconnect), not as a dead goroutine behind a
// Ready()==true connection, and not as a crashed process.
//
// The panic is raised in a goroutine protected by the same method the real
// loops use. Feeding readLoop a hand-crafted poison frame instead would mean
// adding a decode path that can panic, i.e. an injection seam that only tests
// use — the thing worth pinning down here is what guard does, not what Go does
// with a nil map.
func TestGuardConvertsPanicToStreamLoss(t *testing.T) {
	addr := startProbeStub(t, &probeStub{})
	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	c.KeepAlive = time.Hour // keep the probe out of this test's way

	lost := make(chan error, 1)
	c.SetStreamLostHook(func(cause error) {
		select {
		case lost <- cause:
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !c.Ready() {
		t.Fatal("not ready after handshake")
	}
	before := c.ConnectionID()
	if before == "" {
		t.Fatal("no connectionId after handshake")
	}

	runProtected := func(done chan struct{}) {
		defer func() { done <- struct{}{} }()
		defer c.guard("test-panic")
		panic("boom")
	}
	done := make(chan struct{})
	go runProtected(done)

	var cause error
	select {
	case cause = <-lost:
	case <-time.After(5 * time.Second):
		t.Fatal("the panic never reached the stream-lost hook")
	}
	<-done // the protected goroutine unwound instead of killing the process

	if !strings.Contains(cause.Error(), "panic in test-panic: boom") {
		t.Fatalf("hook got %v, want the panic recorded as a stream failure", cause)
	}
	if err := c.StreamErr(); err == nil || !strings.Contains(err.Error(), "panic in") {
		t.Fatalf("StreamErr() = %v, want a panic-derived cause", err)
	}
	if c.Ready() {
		t.Fatal("Ready() true after the guard fired")
	}
	if c.ConnectionID() != "" {
		t.Fatalf("connectionId %q survived a stream loss; the server dropped it", c.ConnectionID())
	}

	// Recovery uses the same path as a server-side drop: release the stream,
	// re-handshake on the still-live channel.
	c.CloseStream()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("re-Setup after panic: %v", err)
	}
	if !c.Ready() {
		t.Fatal("not ready after recovering from a panic")
	}
	if c.ConnectionID() == "" {
		t.Fatal("no connectionId after recovery")
	}
	if n := c.Epoch(); n < 2 {
		t.Fatalf("Epoch() = %d, want a completed handshake to have incremented it", n)
	}
}

// TestGuardIsNoOpWithoutPanic pins the other half of the contract: an ordinary
// return through the guard must not touch the connection.
func TestGuardIsNoOpWithoutPanic(t *testing.T) {
	addr := startProbeStub(t, &probeStub{})
	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	c.KeepAlive = time.Hour

	lost := make(chan error, 1)
	c.SetStreamLostHook(func(cause error) {
		select {
		case lost <- cause:
		default:
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer c.guard("test-clean")
		close(done)
	}()
	<-done

	select {
	case cause := <-lost:
		t.Fatalf("guard reported a stream loss with no panic: %v", cause)
	case <-time.After(300 * time.Millisecond):
	}
	if !c.Ready() {
		t.Fatal("Ready() false after a clean pass through the guard")
	}
}
