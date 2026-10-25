package nacos_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/example/helloworld/ping"
	"github.com/javahongxi/sweet/registry"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
)

// TestRollingRestartDrill measures the claim sweet's whole point rests on:
// when a provider is restarted, a client that only knows the service name
// converges to the new instance without an outage. Failures are classified,
// because "Unavailable: produced zero addresses" (discovery lag) and
// "Unavailable: connection error" (calling a dead address) are different bugs,
// and convergence is observed rather than assumed.
//
// Requires a running Nacos and a prebuilt server binary:
//
//	go build -o /tmp/sweet-server ./example/helloworld/cmd/server
//	NACOS_GRPC_ADDR=127.0.0.1:9848 SWEET_SERVER_BIN=/tmp/sweet-server \
//	  go test ./registry/nacos -run RollingRestart -v
func TestRollingRestartDrill(t *testing.T) {
	addr := os.Getenv("NACOS_GRPC_ADDR")
	bin := os.Getenv("SWEET_SERVER_BIN")
	if addr == "" || bin == "" {
		t.Skip("set NACOS_GRPC_ADDR and SWEET_SERVER_BIN (see doc comment)")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("SWEET_SERVER_BIN: %v", err)
	}
	const service = "sweet.example.v1.RollingDrill"

	var updates atomic.Int64
	var lastSnapshot atomic.Value // []registry.Instance
	note := func(_ string, insts []registry.Instance) {
		updates.Add(1)
		lastSnapshot.Store(insts)
	}
	reg, err := nacosregistry.New(nacosregistry.Options{ServerAddr: addr})
	if err != nil {
		t.Fatalf("registry New: %v", err)
	}
	defer reg.Close()

	// The client's own registry is the one that watches, so it is the one the
	// observation hook must be installed on.
	cliReg, err := nacosregistry.New(nacosregistry.Options{ServerAddr: addr, WatchFilter: note})
	if err != nil {
		t.Fatalf("client registry New: %v", err)
	}
	defer cliReg.Close()
	// Somebody must actually watch the service, otherwise nothing is subscribed
	// and the observation hook can never fire.
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()
	seen, err := cliReg.Watch(watchCtx, service)
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	go func() { // drain so a full buffer never drops an update
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-seen:
			}
		}
	}()

	conn, err := client.Dial(cliReg, service,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	startServer := func(port int) *exec.Cmd {
		cmd := exec.Command(bin,
			"-registry=nacos", "-server="+addr, "-service="+service,
			fmt.Sprintf("-listen=127.0.0.1:%d", port))
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start server on %d: %v", port, err)
		}
		return cmd
	}

	nextPort := 21001
	portA := nextPort
	nextPort++
	cmdA := startServer(portA)
	t.Cleanup(func() { _ = cmdA.Process.Kill() })
	waitForInstances(t, &lastSnapshot, 1, 30*time.Second)

	var calls, fails atomic.Int64
	byKind := struct {
		mu    sync.Mutex
		count map[string]int
	}{count: map[string]int{}}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := ping.NewClient(conn).Ping(ctx, "drill")
			cancel()
			calls.Add(1)
			if err != nil {
				fails.Add(1)
				key := err.Error()
				if st, ok := status.FromError(err); ok {
					key = fmt.Sprintf("%s/%s", st.Code(), classify(st.Message()))
				}
				byKind.mu.Lock()
				byKind.count[key]++
				byKind.mu.Unlock()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	// Rolling restart: bring up B, take down A gracefully, then repeat.
	rounds := 2
	detected := make([]time.Duration, 0, rounds)
	for i := 0; i < rounds; i++ {
		// A fresh port each round: reusing the previous round's port would race
		// its TIME_WAIT/late child and report "address already in use".
		cmdB := startServer(nextPort)
		thisPort := nextPort
		nextPort++
		waitForInstances(t, &lastSnapshot, 2, 30*time.Second)
		time.Sleep(500 * time.Millisecond) // both live, traffic flowing

		at := time.Now()
		signalTerm(t, cmdA)
		waitForInstances(t, &lastSnapshot, 1, 30*time.Second)
		detected = append(detected, time.Since(at))
		t.Logf("round %d: took down %d, converged to the survivor %d in %v",
			i, portA, thisPort, time.Since(at))
		cmdA = cmdB
		portA = thisPort
		time.Sleep(500 * time.Millisecond)
	}

	close(stop)
	wg.Wait()

	total, failed := calls.Load(), fails.Load()
	byKind.mu.Lock()
	kinds := fmt.Sprint(byKind.count)
	byKind.mu.Unlock()
	t.Logf("calls=%d failures=%d (%s) state-updates=%d convergence=%v",
		total, failed, kinds, updates.Load(), detected)
	if total == 0 {
		t.Fatal("no calls issued")
	}
	// Discovery must have converged repeatedly; a single update means the
	// push/watch path is dead and the client was flying blind.
	if updates.Load() < int64(rounds*2) {
		t.Fatalf("only %d state updates for %d rounds, want >= %d",
			updates.Load(), rounds, int64(rounds*2))
	}
	// The strict assertion: graceful offline means no calls failed because a
	// peer was unreachable. In-flight calls during the drain window may still
	// fail; that is reported above rather than hidden.
	for kind, n := range byKind.count {
		if n > 0 && kind != codes.Unavailable.String()+"/in-flight" {
			t.Errorf("unexpected failure kind %q: %d calls", kind, n)
		}
	}
}

func classify(msg string) string {
	switch {
	case contains(msg, "zero addresses"):
		return "discovery-empty"
	case contains(msg, "connection error"), contains(msg, "Connection refused"):
		return "dead-peer"
	default:
		return "in-flight"
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func waitForInstances(t *testing.T, snap *atomic.Value, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v, ok := snap.Load().([]registry.Instance); ok && len(v) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	v, _ := snap.Load().([]registry.Instance)
	t.Fatalf("instance set never reached size %d (last seen %d): %v", want, len(v), v)
}

func signalTerm(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, exec.ErrWaitDelay) {
			t.Logf("server exited with %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("server pid %d did not exit after SIGTERM", cmd.Process.Pid)
	}
}
