package nacos

import (
	"context"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/javahongxi/sweet/registry"
)

// TestIdleRegistrationSurvives is the acceptance check for "ephemeral liveness
// IS the connection" (§8.5): with nothing but the keepalive running, a
// registration must stay visible. The first version of this client lost the
// instance 20–40s after registering it, with the process still running — which
// is why this test exists and why it is long.
//
// Run it with SWEET_LIVE_MINUTES=5 (or any minute count) to make it bite.
func TestIdleRegistrationSurvives(t *testing.T) {
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s", envAddr)
	}
	minutes := 3
	if v := os.Getenv("SWEET_LIVE_MINUTES"); v != "" {
		if m, err := strconv.Atoi(v); err == nil && m > 0 {
			minutes = m
		}
	}
	reg, err := New(Options{ServerAddr: addr, Logger: log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Close()

	const service = "sweet.example.v1.IdleSurvival"
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(minutes+2)*time.Minute)
	defer cancel()

	port := freePort(t)
	if _, err := reg.Expose(ctx, service, registry.Instance{Host: "127.0.0.1", Port: port, Weight: 100}); err != nil {
		t.Fatalf("Expose: %v", err)
	}
	deadline := time.Now().Add(time.Duration(minutes) * time.Minute)
	probes := 0
	for time.Now().Before(deadline) {
		time.Sleep(15 * time.Second)
		probes++
		got, err := reg.query(ctx, service)
		if err != nil {
			t.Fatalf("probe %d: query failed: %v", probes, err)
		}
		if len(got) != 1 || got[0].Port != port {
			t.Fatalf("probe %d: instances = %v, want the one registered instance", probes, got)
		}
	}
	t.Logf("registration survived %d probes over %dm of idleness", probes, minutes)
}

// TestReconnectAfterServerRestart is the other half of §8.5: when the server
// takes the connection away, the client must come back and replay its intent.
// Manual by design — it needs someone to restart Nacos.
func TestReconnectAfterServerRestart(t *testing.T) {
	if os.Getenv("SWEET_RESTART_DRILL") == "" {
		t.Skip("set SWEET_RESTART_DRILL=1, then restart Nacos when prompted")
	}
	addr := os.Getenv(envAddr)
	if addr == "" {
		t.Skipf("set %s", envAddr)
	}
	reg, err := New(Options{ServerAddr: addr, Logger: log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer reg.Close()

	const service = "sweet.example.v1.RestartDrill"
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	port := freePort(t)
	if _, err := reg.Expose(ctx, service, registry.Instance{Host: "127.0.0.1", Port: port, Weight: 100}); err != nil {
		t.Fatalf("Expose: %v", err)
	}
	t.Log(">>> restart Nacos now (bin/shutdown.sh && bin/startup.sh -m standalone); waiting up to 5m")
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)
		if got, err := reg.query(ctx, service); err == nil && len(got) == 1 && got[0].Port == port {
			t.Logf("recovered: registration replayed automatically (instances=%v)", got)
			return
		}
	}
	t.Fatal("registration never came back after the restart")
}
