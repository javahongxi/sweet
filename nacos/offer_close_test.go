package nacos

import (
	"fmt"
	"sync"
	"testing"
)

// TestOfferVersusCloseIsSerialized pins the lock discipline around the push
// channel: sdkConn.Close closes it under pushMu, so offer must still be holding
// pushMu when it sends. Checking `closed` and then releasing the lock before the
// send leaves a window in which the two overlap — the race detector flags it, and
// unrecovered it is a "send on closed channel" panic inside readLoop, which
// guard() silently converts into a stream loss and a spurious reconnect.
//
// A drainer keeps the channel mostly empty so the offers really take the send
// path instead of the drop branch; without it the test would exercise the branch
// that cannot race.
//
// Analysis in doc/nacos-sdk-hardening.md §关停与投递必须串行.
func TestOfferVersusCloseIsSerialized(t *testing.T) {
	const (
		rounds  = 8
		senders = 16
		offers  = 200
	)

	for r := 0; r < rounds; r++ {
		addr := startProbeStub(t, &probeStub{})
		c, err := dialConn(addr)
		if err != nil {
			t.Fatalf("round %d: dialConn: %v", r, err)
		}

		drained := make(chan struct{})
		go func() {
			defer close(drained)
			for range c.Pushes() {
			}
		}()

		var wg sync.WaitGroup
		for s := 0; s < senders; s++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				for j := 0; j < offers; j++ {
					c.offer(Push{
						Type:      TypeConfigChangeNotifyRequest,
						RequestID: fmt.Sprint(s, j),
						Body:      []byte("{}"),
					})
				}
			}(s)
		}

		// Close lands in the middle of the storm: this is the interleaving that
		// used to overlap with an in-flight send.
		go func() { _ = c.Close() }()
		wg.Wait()
		<-drained

		// The contract after Close is a no-op, not a panic and not a send.
		for j := 0; j < 100; j++ {
			c.offer(Push{Type: TypeConfigChangeNotifyRequest, RequestID: "late", Body: []byte("{}")})
		}
		if err := c.Close(); err != nil {
			t.Fatalf("round %d: second Close: %v", r, err)
		}
	}
}
