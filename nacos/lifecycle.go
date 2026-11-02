package nacos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// log routes a skeleton message through the owner's sink.
func (l *lifecycle) log(msg string, kv ...any) {
	if l.logFn != nil {
		l.logFn(msg, kv...)
	}
}

// noopLogger lets a domain whose Options.Logger is nil still satisfy the
// skeleton's log hook without a nil check at every call site.
var noopLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// lifecycle is the connection-owning skeleton shared by NamingClient and
// ConfigClient: which node we are on, the current connection, the reconnect
// supervisor with exponential backoff, the ConnectReset switch, and the
// accessors that read that state.
//
// It is one type rather than two copies because the guarantees in it are not
// obvious and were each paid for: a lost stream is not a dead node (retry in
// place before rotating), a planned switch must not reconnect twice (the dead
// connection is compared against the current one), and a switch must replay
// because the supervisor will never fire for a healthy connection. Duplicating
// the skeleton meant duplicating those bugs — and the ConnectReset work proved
// it, since the replay-after-switch had to be added in two places.
//
// The three things that genuinely differ per domain are hooks: how to dial
// (each domain has its own Options), what to start once a connection is live
// (its push consumer), and what to re-establish (its intents).
type lifecycle struct {
	// newConn dials one address and performs the domain-specific handshake,
	// applying the domain's own knobs (credentials, timeouts, drop reporting).
	newConn func(ctx context.Context, addr string) (*sdkConn, error)
	// onReady starts the domain's push consumer for a freshly published
	// connection. The generation is passed so the consumer can detect that it
	// has been superseded.
	onReady func(conn *sdkConn, generation int64)
	// replay re-establishes everything the server must know about this client on
	// the current connection. Called after a reconnect and after a
	// ConnectReset-driven switch.
	replay func() error
	// logFn is the owner's logger sink; kept a hook so the message keeps the
	// domain's prefix and options.
	logFn func(msg string, kv ...any)
	// tenantFn and labelsFn are what the in-place retry needs to re-handshake on
	// an existing connection, without the skeleton knowing each domain's Options.
	tenantFn func() string
	labelsFn func() map[string]string

	// Fields keep the names the domain clients already used, so their remaining
	// code reads them through embedding without a rewrite.
	serversList *serverList
	budgetDur   time.Duration
	reconnect   time.Duration

	mu         sync.Mutex
	conn       *sdkConn
	addr       string
	generation int64
	handshakes int64
	signal     chan struct{}
	lost       chan struct{}
	lastErr    error
	attempt    int

	ctx    context.Context
	cancel context.CancelFunc
}

// setup wires the skeleton's own state. Each domain constructor calls it after
// filling the hooks.
func (l *lifecycle) setup(servers *serverList, ctx context.Context, cancel context.CancelFunc, budget, backoff time.Duration) {
	if backoff <= 0 {
		backoff = time.Second
	}
	l.serversList = servers
	l.ctx = ctx
	l.cancel = cancel
	l.budgetDur = budget
	l.reconnect = backoff
	l.signal = make(chan struct{})
	l.lost = make(chan struct{}, 1)
}

// start makes one connection attempt inline (so a WaitReady immediately after
// construction has a real answer) and launches the supervisor.
func (l *lifecycle) start() {
	if err := l.reconnectOnce(l.ctx); err != nil {
		l.noteErr(err)
	}
	go l.supervise()
}

// budget is the per-attempt handshake allowance.
func (l *lifecycle) budget() time.Duration {
	if l.budgetDur > 0 {
		return l.budgetDur
	}
	return 12 * time.Second
}

// dialFor performs the domain dial through the hook. Named as before so the
// domain clients' remaining call sites compile unchanged.
func (l *lifecycle) dialFor(ctx context.Context, addr string) (*sdkConn, error) {
	return l.newConn(ctx, addr)
}

// reconnectOnce tries once: the same address while it is still considered
// healthy, otherwise the next one from the list.
func (l *lifecycle) reconnectOnce(ctx context.Context) error {
	addr := l.serversList.Pick()
	if addr == "" {
		return errors.New("nacos: no server address to try")
	}
	l.mu.Lock()
	conn, curAddr := l.conn, l.addr
	l.mu.Unlock()

	// A lost stream is not evidence the node is gone: retry it in place first,
	// which keeps the warm channel.
	if conn != nil && curAddr == addr {
		conn.CloseStream()
		hctx, cancel := context.WithTimeout(ctx, l.budget())
		err := conn.Setup(hctx, l.tenant(), l.labels())
		cancel()
		if err == nil {
			l.publish(conn, addr)
			return nil
		}
		l.serversList.MarkFailed(addr)
		_ = conn.Close()
	}

	next, err := l.dialFor(ctx, addr)
	if err != nil {
		l.serversList.MarkFailed(addr)
		return err
	}
	l.publish(next, addr)
	return nil
}

// publish installs a live connection, retires the previous one, starts its
// consumer and re-establishes the domain's intents.
//
// The replay lives HERE rather than in each caller so the invariant is
// unconditional: a connection that is current has the intents applied to it.
// Callers that replay themselves are one refactor away from forgetting, and a
// missed replay is silent — the connection looks perfectly healthy.
//
// The generation tag stops a previous consumer from acking or delivering after it
// was superseded.
func (l *lifecycle) publish(conn *sdkConn, addr string) {
	l.mu.Lock()
	old := l.conn
	l.conn, l.addr = conn, addr
	l.generation++
	l.handshakes++
	gen := l.generation
	l.lastErr = nil
	l.attempt = 0
	l.broadcastLocked()
	l.mu.Unlock()

	l.serversList.MarkHealthy(addr)
	if old != nil && old != conn {
		_ = old.Close()
	}
	l.logFn("connected", "addr", addr, "connectionId", conn.ConnectionID())
	l.onReady(conn, gen)
	if l.replay == nil {
		return
	}
	if err := l.replay(); err != nil {
		// The intents are not on this node, so this connection cannot serve them:
		// retire it and let the supervisor try again rather than keep a
		// healthy-looking connection the server knows nothing about.
		l.logFn("replay failed; retiring the connection", "err", err)
		l.streamLost(conn)
	}
}

// switchTo moves to a specific address now, which is what ConnectResetRequest
// asks for. The new connection is published before the old one is closed, so
// there is never a window with no live connection (same order as
// RpcClient.java:522-544).
//
// The replay is not optional and it does not happen by itself: this path leaves
// the connection healthy, so the supervisor will never fire. Arriving on the new
// node without re-registering / re-listening means the state is simply gone.
func (l *lifecycle) switchTo(ctx context.Context, addr string) error {
	conn, err := l.dialFor(ctx, addr)
	if err != nil {
		return err
	}
	// publish replays, so a switch needs nothing extra here.
	l.publish(conn, addr)
	return nil
}

// streamLost retires `dead` and wakes the supervisor, but only when `dead` is
// still the current connection. A planned switch closes a connection it has
// already replaced; honouring that callback would reconnect twice for one event
// (the job GrpcClient's abandon flag does, GrpcClient.java:310-322).
//
// The address is not marked failed here — a stream the server ended is retried
// against the same node first, and only a failed handshake puts it on cooldown.
func (l *lifecycle) streamLost(dead *sdkConn) {
	l.mu.Lock()
	if l.conn != dead {
		l.mu.Unlock()
		return
	}
	l.conn, l.addr = nil, ""
	l.generation++
	l.broadcastLocked()
	l.mu.Unlock()

	if dead != nil {
		_ = dead.Close()
	}
	select {
	case l.lost <- struct{}{}:
	default:
	}
}

// supervise turns "the connection is gone" into "retry this node, then rotate",
// until Close. Without it a dropped stream leaves the client holding a
// registration the server deleted along with the old connectionId.
func (l *lifecycle) supervise() {
	defer func() {
		if v := recover(); v != nil {
			// A dead supervisor means no replay after the next disconnect, which
			// is worse than a restartable one.
			l.logFn("supervisor panicked, restarting it", "err", fmt.Sprint(v))
			go l.supervise()
		}
	}()
	for {
		if l.ctx.Err() != nil {
			return
		}
		if l.connected() {
			select {
			case <-l.ctx.Done():
				return
			case <-l.lost:
				continue
			}
		}
		// Not connected. This branch also covers "the very first handshake
		// failed", where no stream ever existed to be lost — waiting on the loss
		// signal there would sleep forever.
		select {
		case <-l.ctx.Done():
			return
		case <-time.After(l.nextBackoff()):
		}
		if err := l.reconnectOnce(l.ctx); err != nil {
			l.noteErr(err)
			l.logFn("handshake failed", "err", err)
			continue
		}
	}
}

// nextBackoff grows exponentially from the configured first delay, capped at
// 30s, with up to a quarter of the delay as jitter. A fixed interval makes a
// fleet retry in lockstep: after a config centre restarts, every client knocks
// at the same instant, which is how the recovery becomes the next outage.
func (l *lifecycle) nextBackoff() time.Duration {
	l.mu.Lock()
	l.attempt++
	a := l.attempt
	l.mu.Unlock()

	d := l.reconnect << min(a-1, 5)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	if d <= 0 {
		return l.reconnect
	}
	return d + time.Duration(rand.Int64N(int64(d/4)+1))
}

// --- state readers, shared by both domains ---

// WaitReady blocks until a handshaked connection exists, ctx ends, or Close runs.
// The error wraps ErrUnavailable and names the list plus the last failure, so a
// caller can log *why* it is degraded instead of "it didn't connect".
func (l *lifecycle) WaitReady(ctx context.Context) error {
	for {
		l.mu.Lock()
		conn, sig, msg := l.conn, l.signal, l.lastErr
		l.mu.Unlock()
		if conn != nil && conn.Ready() {
			return nil
		}
		select {
		case <-sig:
		case <-ctx.Done():
			return fmt.Errorf("%w: none of %s reachable (last error: %v)", ErrUnavailable, l.addrs(), msg)
		case <-l.ctx.Done():
			return l.ctx.Err()
		}
	}
}

// CurrentAddr is the SDK address in use, "" while a reconnect is in flight.
func (l *lifecycle) CurrentAddr() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.addr
}

// ConnectionID is the id the server gave the current connection, "" while
// reconnecting. Exposed because a replay is only meaningful against a fresh id —
// the server's tables are keyed by it.
func (l *lifecycle) ConnectionID() string {
	l.mu.Lock()
	conn := l.conn
	l.mu.Unlock()
	if conn == nil {
		return ""
	}
	return conn.ConnectionID()
}

// Handshakes counts completed handshakes. A test that expects a replay must first
// prove a new handshake happened, otherwise "nothing was replayed" and "nothing
// was needed" look identical.
func (l *lifecycle) Handshakes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.handshakes
}

// PushDropped counts pushes discarded because the consumer stalled, across
// whichever connection is current.
func (l *lifecycle) PushDropped() int64 {
	l.mu.Lock()
	conn := l.conn
	l.mu.Unlock()
	if conn == nil {
		return 0
	}
	return conn.PushDropped()
}

// LastErr is the most recent handshake, replay or refresh failure.
func (l *lifecycle) LastErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastErr
}

// liveConn returns the current connection or an ErrUnavailable that says which
// list was tried and why it last failed. Callers take the connection once and use
// it for the whole call: the supervisor may publish another one mid-flight.
func (l *lifecycle) liveConn() (*sdkConn, error) {
	l.mu.Lock()
	conn, msg := l.conn, l.lastErr
	l.mu.Unlock()
	if conn == nil || !conn.Ready() {
		return nil, fmt.Errorf("%w: no connection to %s (last error: %v)", ErrUnavailable, l.addrs(), msg)
	}
	return conn, nil
}

func (l *lifecycle) connected() bool {
	l.mu.Lock()
	conn := l.conn
	l.mu.Unlock()
	return conn != nil && conn.Ready()
}

// currentConn / backgroundCtx / servers are the resetTarget hooks; the embedding
// domain clients satisfy the interface through them.
func (l *lifecycle) currentConn() (*sdkConn, string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conn, l.addr
}

func (l *lifecycle) backgroundCtx() context.Context { return l.ctx }

func (l *lifecycle) servers() *serverList { return l.serversList }

func (l *lifecycle) noteErr(err error) {
	if err == nil {
		return
	}
	l.mu.Lock()
	l.lastErr = err
	l.mu.Unlock()
}

func (l *lifecycle) broadcastLocked() {
	close(l.signal)
	l.signal = make(chan struct{})
}

// addrs renders the list and who is cooling down, for error text and logs.
func (l *lifecycle) addrs() string {
	all, bad := l.serversList.Addrs()
	if len(bad) == 0 {
		return fmt.Sprint(all)
	}
	return fmt.Sprintf("%v (%v on cooldown)", all, bad)
}

// closeConn tears the connection down and marks the lifecycle closed. Each
// domain's Close calls it after its own cleanup.
func (l *lifecycle) closeConn() error {
	l.cancel()
	l.mu.Lock()
	conn := l.conn
	l.conn, l.addr = nil, ""
	l.broadcastLocked()
	l.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (l *lifecycle) tenant() string {
	if l.tenantFn == nil {
		return ""
	}
	return l.tenantFn()
}

func (l *lifecycle) labels() map[string]string {
	if l.labelsFn == nil {
		return nil
	}
	return l.labelsFn()
}
