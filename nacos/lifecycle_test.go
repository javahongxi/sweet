package nacos

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// The skeleton is where the hard-won connection rules now live, so the rules are
// tested here once rather than re-proved per domain. The domain suites
// (config_client_test.go, naming_client_test.go) keep their own end-to-end coverage;
// what these pin is behaviour that must be identical for both.

// lcSpy counts what the lifecycle's hooks did, so assertions read the skeleton's
// contract instead of a domain's side effects.
type lcSpy struct {
	mu       sync.Mutex
	dials    []string
	replays  int
	ready    int
	failDial int // first N dial attempts fail
}

func (s *lcSpy) dial() func(ctx context.Context, a string) (*sdkConn, error) {
	return func(ctx context.Context, a string) (*sdkConn, error) {
		s.mu.Lock()
		s.dials = append(s.dials, a)
		fail := s.failDial > 0
		if fail {
			s.failDial--
		}
		s.mu.Unlock()
		if fail {
			return nil, errors.New("spy: dial refused")
		}
		return dialAndHandshake(ctx, a)
	}
}

// dialAndHandshake is what a domain's hook does for real: dial, install the loss
// hook, and perform the two-part handshake. Kept here so the tests exercise the
// same transport path production uses.
func dialAndHandshake(ctx context.Context, addr string) (*sdkConn, error) {
	c, err := dialConn(addr)
	if err != nil {
		return nil, err
	}
	c.ProbeFatal = true
	hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := c.Setup(hctx, "public", map[string]string{"source": "sdk", "module": "test"}); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

func (s *lcSpy) counts() (dials, replays, ready int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dials), s.replays, s.ready
}

// newSpyLifecycle builds a lifecycle over the handshake-only stub.
func newSpyLifecycle(t *testing.T, addrs []string, failFirst int) (*lifecycle, *lcSpy, func()) {
	t.Helper()
	t.Helper()
	spy := &lcSpy{failDial: failFirst}
	ctx, cancel := context.WithCancel(context.Background())
	l := &lifecycle{}
	l.setup(newServerList(addrs, 0), ctx, cancel, 3*time.Second, 20*time.Millisecond)
	l.newConn = spy.dial()
	l.onReady = func(conn *sdkConn, gen int64) {
		spy.mu.Lock()
		spy.ready++
		spy.mu.Unlock()
		conn.SetStreamLostHook(func(error) { l.streamLost(conn) })
	}
	l.replay = func() error {
		spy.mu.Lock()
		spy.replays++
		spy.mu.Unlock()
		return nil
	}
	l.logFn = func(string, ...any) {}
	return l, spy, func() { _ = l.closeConn() }
}

// TestLifecycleKeepsRetryingUntilTheServerAnswers pins the branch that used to
// sleep forever: when the very first handshake fails there is no stream whose
// loss could be signalled, so the supervisor must drive retries itself.
func TestLifecycleKeepsRetryingUntilTheServerAnswers(t *testing.T) {
	stub := &probeStub{}
	addr := startProbeStub(t, stub)

	l, spy, done := newSpyLifecycle(t, []string{addr}, 2) // two failures, then success
	defer done()
	l.start()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := l.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady after retries: %v", err)
	}
	dials, replays, ready := spy.counts()
	if dials < 3 {
		t.Errorf("dials = %d, want at least 3 (two refusals then success)", dials)
	}
	if ready == 0 {
		t.Error("onReady never ran: no push consumer would be reading the stream")
	}
	if replays == 0 {
		t.Error("replay never ran: intents would be lost after the retry")
	}
}

// TestLifecycleReplaysOncePerConnection: replay is what makes a reconnect
// invisible to the server's tables, but a replay per *event* rather than per
// connection is how a route table or a registration gets applied twice.
func TestLifecycleReplaysOncePerConnection(t *testing.T) {
	stub := &probeStub{}
	addr := startProbeStub(t, stub)

	l, spy, done := newSpyLifecycle(t, []string{addr}, 0)
	defer done()
	l.start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	_, replays, _ := spy.counts()
	if replays != 1 {
		t.Fatalf("replays = %d after one connection, want exactly 1", replays)
	}
	// A healthy connection must not keep replaying while the supervisor idles.
	time.Sleep(200 * time.Millisecond)
	_, replays2, _ := spy.counts()
	if replays2 != 1 {
		t.Errorf("replays = %d while connected and idle, want still 1", replays2)
	}
}

// TestLifecycleSwitchToRetiresOldAndCountsOneHandshake: the ConnectReset path
// must leave exactly one new connection behind. If the retired connection's loss
// callback were honoured, the supervisor would dial a second time and the domain
// would replay twice.
func TestLifecycleSwitchToRetiresOldAndCountsOneHandshake(t *testing.T) {
	a := &probeStub{}
	addrA := startProbeStub(t, a)
	b := &probeStub{}
	addrB := startProbeStub(t, b)

	l, spy, done := newSpyLifecycle(t, []string{addrA, addrB}, 0)
	defer done()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := l.reconnectOnce(ctx); err != nil {
		t.Fatalf("first connect: %v", err)
	}
	if got := l.CurrentAddr(); got != addrA {
		t.Fatalf("CurrentAddr = %q, want %q", got, addrA)
	}

	before := l.Handshakes()
	if err := l.switchTo(ctx, addrB); err != nil {
		t.Fatalf("switchTo: %v", err)
	}
	if got := l.CurrentAddr(); got != addrB {
		t.Fatalf("after switch CurrentAddr = %q, want %q", got, addrB)
	}
	if got := l.Handshakes() - before; got != 1 {
		t.Errorf("handshakes grew by %d, want exactly 1", got)
	}
	_, replays, _ := spy.counts()
	if replays < 2 {
		t.Errorf("replays = %d, want one per connection (initial + switch)", replays)
	}
	// Give a wrongly-honoured loss callback from the retired connection a chance
	// to fire and produce a third handshake.
	time.Sleep(250 * time.Millisecond)
	if got := l.Handshakes() - before; got != 1 {
		t.Errorf("handshakes grew by %d after settling, want still 1 — the retired "+
			"connection's loss must not trigger a second reconnect", got)
	}
}

// TestLifecycleWaitReadyNamesTheListAndCooldown: an operator reading a degraded
// start needs to know which addresses were tried and which are parked, not just
// "unavailable".
func TestLifecycleWaitReadyNamesTheListAndCooldown(t *testing.T) {
	// Reserve an address and release it: nothing is behind it, so every dial
	// fails the way a downed node fails.
	lis, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatalf("reserve: %v", e)
	}
	addrDead := lis.Addr().String()
	_ = lis.Close()

	l, _, done := newSpyLifecycle(t, []string{addrDead}, 100)
	defer done()
	l.start()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err := l.WaitReady(ctx)
	if err == nil {
		t.Fatal("WaitReady must fail with nothing reachable")
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("err = %v, want it to wrap ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), addrDead) {
		t.Errorf("error should name the address it tried: %v", err)
	}
}

// TestLifecycleBackoffGrowsCapsAndJitters replaces the per-domain copy of this
// assertion: the wait must grow, must stop growing, and must not be a fixed
// interval a fleet can synchronise on.
func TestLifecycleBackoffGrowsCapsAndJitters(t *testing.T) {
	l := &lifecycle{reconnect: 100 * time.Millisecond}
	seen := make([]time.Duration, 0, 8)
	for i := 0; i < 8; i++ {
		seen = append(seen, l.nextBackoff())
	}
	if seen[0] < 100*time.Millisecond || seen[0] > 125*time.Millisecond {
		t.Errorf("first backoff = %v, want ~100ms with jitter", seen[0])
	}
	if seen[3] <= seen[1] {
		t.Errorf("backoff did not grow: %v", seen)
	}
	if seen[7] > 30*time.Second+30*time.Second/4 {
		t.Errorf("backoff %v exceeded the 30s cap", seen[7])
	}
	same := true
	for i := 0; i < 40 && same; i++ {
		l.attempt = 2
		same = l.nextBackoff() == l.nextBackoff()
	}
	if same {
		t.Error("no jitter across 80 draws — a fleet would retry in lockstep")
	}
}

// --- what WaitReady is allowed to claim ------------------------------------

// TestWaitReadyWaitsForReplay tightens the contract to match the invariant
// publish documents: "a connection that is current has the intents applied to it".
// Before this, publish installed the connection and woke WaitReady *before*
// onReady and replay ran, so a caller that obeyed WaitReady could act on a node
// that had not been told anything yet.
//
// The replay hook blocks here, which pins the ordering deterministically instead
// of racing for a counter to increment: while replay has not returned, WaitReady
// must not say ready.
func TestWaitReadyWaitsForReplay(t *testing.T) {
	stub := &probeStub{}
	addr := startProbeStub(t, stub)
	l, spy, done := newSpyLifecycle(t, []string{addr}, 0)
	defer done()

	inReplay := make(chan struct{}, 1)
	release := make(chan struct{})
	l.replay = func() error {
		spy.mu.Lock()
		spy.replays++
		spy.mu.Unlock()
		select {
		case inReplay <- struct{}{}:
		default:
		}
		<-release // hold publish open while the intents are being applied
		return nil
	}

	go l.start()

	select {
	case <-inReplay:
	case <-time.After(8 * time.Second):
		t.Fatal("replay never started; nothing to hold the ready signal behind")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := l.WaitReady(ctx); err == nil {
		close(release)
		t.Fatal("WaitReady returned nil while replay was still in flight — the caller would believe the intents are on this node when they are not")
	}

	close(release)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	if err := l.WaitReady(ctx2); err != nil {
		t.Fatalf("WaitReady after the replay completed: %v", err)
	}
	if _, replays, ready := spy.counts(); replays == 0 || ready == 0 {
		t.Errorf("counts after WaitReady: ready=%d replays=%d, want both non-zero", ready, replays)
	}
}

// TestWaitReadyDoesNotReportReadyWhenReplayFails is the other half: a replay that
// fails retires the connection, so that generation must never be reported as
// ready, and the supervisor has to land a connection whose replay actually
// succeeded before WaitReady returns nil.
//
// The first attempt is held inside the hook until the test releases it, so the
// assertion is about ordering rather than about winning a race against the retry
// backoff.
func TestWaitReadyDoesNotReportReadyWhenReplayFails(t *testing.T) {
	stub := &probeStub{}
	addr := startProbeStub(t, stub)
	l, spy, done := newSpyLifecycle(t, []string{addr}, 0)
	defer done()

	firstEntered := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	l.replay = func() error {
		spy.mu.Lock()
		spy.replays++
		n := spy.replays
		spy.mu.Unlock()
		if n == 1 {
			select {
			case firstEntered <- struct{}{}:
			default:
			}
			<-releaseFirst
			return errors.New("spy: replay refused")
		}
		return nil
	}

	go l.start()

	select {
	case <-firstEntered:
	case <-time.After(8 * time.Second):
		t.Fatal("the first replay never started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := l.WaitReady(ctx); err == nil {
		close(releaseFirst)
		t.Fatal("WaitReady vouched for a connection whose replay is known to fail")
	}

	close(releaseFirst)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	if err := l.WaitReady(ctx2); err != nil {
		t.Fatalf("WaitReady should succeed once a replay lands: %v", err)
	}
	if _, replays, _ := spy.counts(); replays < 2 {
		t.Errorf("replays = %d, want at least 2 (one refusal then a success)", replays)
	}
	if c, _ := l.currentConn(); c == nil || !c.Ready() {
		t.Error("WaitReady returned ready for a connection that is not current")
	}
}
