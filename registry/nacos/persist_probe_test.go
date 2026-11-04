package nacos

import (
	"context"
	"errors"
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
		got, err := reg.Query(ctx, service)
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
//
// It has two phases on purpose. The first version polled only for "the instance
// is listed", which passed 5s after Expose *whether or not anyone restarted
// anything* — an untouched registration still answers a query, so the test could
// not tell a successful replay from nothing happening. So the restart has to be
// *observed* first: the instance must go away (the server dropped the connection
// and with it the ephemeral instance, or queries fail while the client is
// reconnecting). Only then is "it came back" evidence of the replay.
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
	port := freePort(t)
	exposeCtx, cancelExpose := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelExpose()
	if _, err := reg.Expose(exposeCtx, service, registry.Instance{Host: "127.0.0.1", Port: port, Weight: 100}); err != nil {
		t.Fatalf("Expose: %v", err)
	}

	// Each probe gets its own short context: a deadline on one shared context would
	// look exactly like the disappearance this phase is trying to detect.
	absent := func() (bool, string) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		got, err := reg.Query(ctx, service)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return false, ""
			}
			return true, "query failed: " + err.Error()
		}
		if len(got) == 0 {
			return true, "the server lists no instance for the service"
		}
		return false, ""
	}

	t.Log(">>> restart Nacos now; waiting up to 3m to observe the registration go away")
	goneWhy := ""
	goneAt := time.Time{}
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		if no, why := absent(); no {
			goneAt, goneWhy = time.Now(), why
			t.Logf("loss observed at %s: %s", goneAt.Format("15:04:05.000"), why)
			break
		}
	}
	if goneAt.IsZero() {
		t.Fatal("never observed the registration disappearing — the server was not restarted, " +
			"so this run cannot tell a successful replay from nothing happening")
	}

	// Phase two: the client must re-establish and replay on its own. Nobody calls
	// Expose again, so a reappearance can only be the replay.
	deadline = time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		time.Sleep(2 * time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		got, err := reg.Query(ctx, service)
		cancel()
		if err == nil && len(got) == 1 && got[0].Port == port {
			t.Logf("recovered %s after the loss: registration replayed automatically (instances=%v)",
				time.Since(goneAt).Round(time.Second), got)
			return
		}
	}
	t.Fatalf("the registration never came back within 3m of disappearing (%s)", goneWhy)
}
