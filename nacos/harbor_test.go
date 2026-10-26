package nacos

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// SWEET_HARBOR_ADDR opts into verifying against harbor, the Nacos-compatible
// registry implemented server-side in jaws. This is the "从对面打过来" leg:
// the same client code must work against a registry that is not Nacos.
const envHarbor = "SWEET_HARBOR_ADDR"

func harborAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv(envHarbor)
	if addr == "" {
		t.Skipf("set %s (e.g. 127.0.0.1:19848) with `./run-sample.sh harbor-standalone 19848`", envHarbor)
	}
	return addr
}

// TestHarborHandshakeWithAbilityTable is the baseline: a client that fills
// abilityTable must complete the handshake against harbor too.
func TestHarborHandshakeWithAbilityTable(t *testing.T) {
	c, err := Dial(harborAddr(t))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	// Harbor's own client never sends a unary HealthCheckRequest, so whether
	// harbor answers our probe is an open question; do not let it fail this
	// test until we have measured it separately.
	c.ProbeFatal = false

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Setup(ctx, "public", nil); err != nil {
		t.Fatalf("handshake with harbor: %v", err)
	}
	t.Logf("harbor handshake ok: connectionId=%q abilities=%v", c.ConnectionID(), c.Abilities())
}

// TestHarborSilentWithoutAbilityTable documents the trap we already suspect in
// harbor's own client: harbor (like Nacos) only answers SetupAckRequest when the
// setup carries a non-null abilityTable, and HarborConnection fills labels but
// not abilityTable. If this test *passes* (i.e. harbor stays silent), harbor's
// native client is relying on the 100ms sleep path rather than on a handshake
// acknowledgement.
func TestHarborSilentWithoutAbilityTable(t *testing.T) {
	c, err := Dial(harborAddr(t))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()
	c.ProbeFatal = false
	c.SetupTimeout = 3 * time.Second

	// Deliberately omit abilityTable by sending the frame ourselves.
	if err := c.openStream(); err != nil {
		t.Fatalf("openStream: %v", err)
	}
	p, err := marshalRequest(ConnectionSetupRequest{
		ClientVersion: ClientVersion,
		Tenant:        "public",
		Labels:        map[string]string{"source": "sdk", "module": "naming"},
		// AbilityTable intentionally nil — `json:"abilityTable"` with no omitempty
		// still emits `"abilityTable":null`, which the server may or may not read
		// as non-null; that difference is exactly what this test measures.
	}, c.localIP())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.send(p); err != nil {
		t.Fatalf("send setup: %v", err)
	}
	select {
	case <-c.awaitSetup():
		t.Log("harbor answered a setup without an ability table")
	case <-time.After(5 * time.Second):
		out := c.StreamErr()
		if out != nil && strings.Contains(out.Error(), "not open") {
			t.Fatalf("stream died instead of timing out: %v", out)
		}
		t.Log("harbor stayed silent for a null-abilityTable setup (same contract as Nacos)")
	}
}
