package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/javahongxi/sweet/gen"
)

// gRPC method names, derived from nacos_grpc_service.proto (no proto package,
// services Request / BiRequestStream).
const (
	methodUnary = "/Request/request"
	methodBidi  = "/BiRequestStream/requestBiStream"
)

// Timeouts and intervals, per the nacos-client contract
// (jaws/doc/harbor-vs-nacos.md §8.3, re-verified against nacos @3.2.3):
//
//	DefaultServerCheckTimeout  3s   serverCheckTimeOut, unary before the stream
//	DefaultSetupTimeout        3s   wait for SetupAckRequest
//	DefaultKeepAlive           5s   connectionKeepAlive, app-level HealthCheckRequest
//	DefaultCallTimeout           5s   unary naming calls
const (
	DefaultServerCheckTimeout = 3 * time.Second
	DefaultSetupTimeout       = 3 * time.Second
	DefaultKeepAlive          = 5 * time.Second
	DefaultCallTimeout        = 5 * time.Second
)

// Push is a server-initiated request arriving on the bidi stream.
//
// RequestID is copied from the frame's own JSON body
// (api/.../remote/request/ServerRequest.java): the server's
// RpcAckCallbackSynchronizer waits for an ack carrying the same requestId per
// connection and re-pushes until it sees one, so echoing it back is mandatory.
// Measured on a live 3.2.3 server: an un-acked change was pushed three times.
type Push struct {
	Type      string
	RequestID string
	Body      []byte
}

// serverRequestMeta is the piece of a pushed ServerRequest we must understand:
// its requestId.
const typeHealthCheckResponse = "HealthCheckResponse"

type serverRequestMeta struct {
	RequestID string `json:"requestId,omitempty"`
}

// Conn is one logical connection to a Nacos server: the unary RPC and the
// long-lived bidi stream that carries the handshake and pushes. Both live on one
// grpc.ClientConn, which is itself long-lived — losing the stream does not lose
// the channel, so recovery means opening a new stream and re-handshaking.
type Conn struct {
	addr string
	ip   string

	cc     *grpc.ClientConn
	ctx    context.Context // owns the bidi stream; cancelled by Close
	cancel context.CancelFunc

	ServerCheckTimeout time.Duration
	SetupTimeout       time.Duration
	KeepAlive          time.Duration
	CallTimeout        time.Duration

	sendMu    sync.Mutex
	stream    grpc.ClientStream
	streamErr error

	setupMu    sync.Mutex
	setupDone  chan struct{}
	handshaked bool
	abilities  map[string]bool

	connectionID atomic.Value // string
	epoch        atomic.Int64
	lastActive   atomic.Int64

	pushMu sync.Mutex
	pushes chan Push

	closed   bool
	closedMu sync.Mutex

	// ProbeFatal decides whether an unanswered probe tears the connection down.
	// Verified against a live 3.2.3 server: the unary HealthCheckRequest is
	// answered, so an unanswered one means the connection is gone (and gRPC
	// gives no other signal for a stream the server ended on its own).
	ProbeFatal bool
	requestSeq atomic.Int64

	// onStreamLost, when set, is called once per lost stream so the owner can
	// re-establish and replay. It runs on a goroutine, never on the read loop's
	// critical path beyond signalling.
	onStreamLost func(error)
}

// Dial prepares the transport. No I/O happens here: grpc.NewClient connects
// lazily; Setup performs the handshake described in §8.3.
func Dial(addr string, opts ...grpc.DialOption) (*Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("nacos: server addr %q wants host:port", addr)
	}
	dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	cc, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("nacos: dial %s: %w", addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		addr:               addr,
		ip:                 host,
		cc:                 cc,
		ctx:                ctx,
		cancel:             cancel,
		pushes:             make(chan Push, 64),
		setupDone:          make(chan struct{}),
		ProbeFatal:         true,
		ServerCheckTimeout: DefaultServerCheckTimeout,
		SetupTimeout:       DefaultSetupTimeout,
		KeepAlive:          DefaultKeepAlive,
		CallTimeout:        DefaultCallTimeout,
	}
	c.connectionID.Store("")
	return c, nil
}

// currentStream exposes the live stream for tests that need to observe raw
// transport behaviour.
func (c *Conn) currentStream() grpc.ClientStream {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream
}

// SetStreamLostHook registers the recovery callback. Call it before Setup: the
// hook is what turns "the stream died" into "reconnect and replay", and a
// connection without it degrades to exactly the silent-death failure mode the
// first version had.
func (c *Conn) SetStreamLostHook(fn func(error)) { c.onStreamLost = fn }

// Setup performs the two-part handshake:
//
//  1. unary ServerCheckRequest  → ServerCheckResponse{connectionId}
//  2. bidi ConnectionSetupRequest → SetupAckRequest
//
// Step 1 is not optional bookkeeping: "拿到 connectionId 才算连上"
// (harbor-vs-nacos.md §8.3), and it is also what warms the channel so the
// stream is not the first thing on a cold connection. abilityTable must be
// non-null or the server answers nothing at all — see ConnectionSetupRequest.
func (c *Conn) Setup(ctx context.Context, tenant string, labels map[string]string) error {
	if labels == nil {
		// RemoteConstants.java:32,40 — the values Nacos itself uses.
		labels = map[string]string{"source": "sdk", "module": "naming"}
	}
	id, err := c.serverCheck(ctx)
	if err != nil {
		return err
	}
	c.connectionID.Store(id)

	if err := c.openStream(); err != nil {
		return err
	}
	setup := ConnectionSetupRequest{
		ClientVersion: ClientVersion,
		Tenant:        tenant,
		Labels:        labels,
		AbilityTable:  map[string]bool{}, // non-null: see the field comment
	}
	p, err := marshalRequest(setup, c.localIP())
	if err != nil {
		return err
	}
	if err := c.send(p); err != nil {
		return err
	}
	timeout := c.SetupTimeout
	if timeout <= 0 {
		timeout = DefaultSetupTimeout
	}
	select {
	case <-c.awaitSetup():
		c.epoch.Add(1) // epoch counts completed handshakes: 1, then +1 per recovery
		go c.keepAlive()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return fmt.Errorf("nacos: %s did not acknowledge the setup within %s", c.addr, timeout)
	}
}

// serverCheck issues the unary probe and returns the connectionId it carries.
func (c *Conn) serverCheck(ctx context.Context) (string, error) {
	timeout := c.ServerCheckTimeout
	if timeout <= 0 {
		timeout = DefaultServerCheckTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	p, err := marshalRequest(ServerCheckRequest{}, c.localIP())
	if err != nil {
		return "", err
	}
	var out gen.Payload
	if err := c.cc.Invoke(cctx, methodUnary, p, &out); err != nil {
		return "", fmt.Errorf("nacos: server check %s: %w", c.addr, err)
	}
	c.touch()
	var resp struct {
		ConnectionID string `json:"connectionId"`
		Result       string `json:"result"`
	}
	if _, err := unmarshal(&out, &resp); err != nil {
		return "", err
	}
	if resp.ConnectionID == "" {
		return "", errors.New("nacos: server check returned no connectionId")
	}
	return resp.ConnectionID, nil
}

func (c *Conn) openStream() error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.stream != nil {
		return nil
	}
	cs, err := c.cc.NewStream(c.ctx, &grpc.StreamDesc{
		StreamName:    "requestBiStream",
		ServerStreams: true,
		ClientStreams: true,
	}, methodBidi)
	if err != nil {
		return fmt.Errorf("nacos: open bidi stream: %w", err)
	}
	c.stream = cs
	c.streamErr = nil
	go c.readLoop(cs)
	return nil
}

// guard turns a panic in one of the connection's long-lived goroutines into a
// stream failure, i.e. into the same event as "the server walked away". A
// panic while decoding a frame is a poison on this connection, not a reason to
// take the whole process down — and leaving the goroutine dead while the
// connection still reports Ready() is precisely the silent-death failure mode
// this package exists to avoid.
func (c *Conn) guard(where string) {
	if v := recover(); v != nil {
		c.failStream(fmt.Errorf("nacos: panic in %s: %v", where, v))
	}
}

// readLoop owns receives on the bidi stream. Handshake frames are consumed here;
// everything else is offered to the consumer, which is responsible for acking.
func (c *Conn) readLoop(cs grpc.ClientStream) {
	defer c.guard("readLoop")
	for {
		var p gen.Payload
		if err := cs.RecvMsg(&p); err != nil {
			c.failStream(err)
			return
		}
		c.touch()
		t, err := unmarshal(&p, nil)
		if err != nil {
			continue
		}
		switch t {
		case typeSetupAckRequest:
			var ack SetupAckRequest
			_, _ = unmarshal(&p, &ack)
			c.setupMu.Lock()
			c.abilities = ack.AbilityTable
			c.setupMu.Unlock()
			// The server sends SetupAck async and does not wait for this reply
			// (§8.3); we answer anyway because Nacos' own client does, and a
			// server that tracks it would otherwise re-push.
			if r, err := marshalResponse(SetupAckResponse{}); err == nil {
				_ = c.send(r)
			}
			c.markSetupDone()
		default:
			var sr serverRequestMeta
			_, _ = unmarshal(&p, &sr)
			c.offer(Push{Type: t, RequestID: sr.RequestID, Body: append([]byte(nil), p.GetBody().GetValue()...)})
		}
	}
}

// failStream records why the stream died and hands recovery to the owner. A
// silent dead stream is the worst failure mode this package can have: the
// instance vanishes from the registry while the process looks perfectly alive.
// failStream is idempotent: it is called from the read loop and from senders,
// and a send error on an already-reaped stream must not fire recovery twice.
func (c *Conn) failStream(err error) {
	c.sendMu.Lock()
	if c.stream == nil {
		c.sendMu.Unlock()
		return
	}
	c.stream = nil
	if err == nil {
		err = errors.New("stream closed")
	}
	c.streamErr = err
	closed := c.closed
	c.sendMu.Unlock()

	c.connectionID.Store("")
	if closed {
		return
	}
	if c.onStreamLost != nil {
		go c.onStreamLost(err)
	}
}

// keepAlive issues the application-level HealthCheckRequest, and only when
// idle: §8.3 says "无活动超过 connectionKeepAlive 才发", not a fixed timer.
// gRPC's HTTP/2 keepalive (6min default) is a different layer and is not what
// makes a Nacos connection count as active.
//
// Two measurements decide this function's shape:
//   - Against a live 3.2.3 server the probe is answered on the *unary* channel
//     and goes unanswered on the bidi stream, which is why keepAlive uses
//     Unary — matching RpcClient.healthCheck(), which calls
//     currentConnection.request(...) and treats failure as UNHEALTHY → reconnect.
//   - gRPC surfaces a server-side end of a single bidi stream to neither a
//     blocked RecvMsg nor a SendMsg (measured against an in-process server).
//     So an unanswered probe is the only reliable way to notice a stream the
//     server already walked away from; without it a client keeps holding a
//     registration the server removed.
func (c *Conn) keepAlive() {
	interval := c.KeepAlive
	if interval <= 0 {
		interval = DefaultKeepAlive
	}
	tick := time.NewTicker(interval)
	tick.Reset(interval) // do not fire on a connection that is microseconds old
	defer tick.Stop()
	defer c.guard("keepAlive")
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-tick.C:
			if c.idleFor() < interval {
				continue
			}
			timeout := c.ServerCheckTimeout
			if timeout <= 0 {
				timeout = DefaultServerCheckTimeout
			}
			ctx, cancel := context.WithTimeout(c.ctx, timeout)
			_, _, err := c.Unary(ctx, HealthCheckRequest{RequestID: c.nextRequestID()})
			cancel()
			if err != nil {
				if c.ProbeFatal {
					c.failStream(fmt.Errorf("keepalive failed: %w", err))
				}
				return
			}
			c.touch()
		}
	}
}

// nextRequestID mirrors PushAckIdGenerator: a per-connection increasing id.
func (c *Conn) nextRequestID() string {
	return strconv.FormatInt(c.requestSeq.Add(1), 10)
}

// Ack answers a pushed request, which is what stops the server's re-push loop.
// The reply class is the request class with "Response" appended
// (NotifySubscriberRequest → NotifySubscriberResponse, §8.4).
func (c *Conn) Ack(p Push) error {
	if p.RequestID == "" {
		return nil // nothing measured to echo back
	}
	body, err := json.Marshal(ackBody{Headers: map[string]string{}, RequestID: p.RequestID})
	if err != nil {
		return err
	}
	frame := &gen.Payload{
		Metadata: &gen.Metadata{Type: p.Type + "Response"},
		Body:     &anypb.Any{Value: body},
	}
	if err := c.send(frame); err != nil {
		return err
	}
	c.touch()
	return nil
}

type ackBody struct {
	Headers   map[string]string `json:"headers"`
	RequestID string            `json:"requestId,omitempty"`
}

// Pushes exposes server-initiated frames.
func (c *Conn) Pushes() <-chan Push { return c.pushes }

func (c *Conn) offer(p Push) {
	c.pushMu.Lock()
	ready := !c.closed
	c.pushMu.Unlock()
	if !ready {
		return
	}
	select {
	case c.pushes <- p:
	default:
		// A full buffer means the consumer stalled. Dropping one push is
		// recoverable (the next change re-pushes); blocking would stall the
		// read loop and therefore keepalive too.
	}
}

func (c *Conn) send(p *gen.Payload) error {
	c.sendMu.Lock()
	cs := c.stream
	c.sendMu.Unlock()
	if cs == nil {
		return fmt.Errorf("nacos: stream not open")
	}
	if err := cs.SendMsg(p); err != nil {
		// Writing to a stream the server already ended is how we learn about
		// it: gRPC's client does not surface "peer closed this one stream" to a
		// blocked RecvMsg, so a sender-side failure is the detection path.
		c.failStream(fmt.Errorf("nacos: send %s: %w", p.GetMetadata().GetType(), err))
		return fmt.Errorf("nacos: send %s: %w", p.GetMetadata().GetType(), err)
	}
	return nil
}

// Call performs one unary request and decodes the reply into resp, asserting
// the response class name.
func (c *Conn) Call(ctx context.Context, req envelope, resp envelope) error {
	t, body, err := c.Unary(ctx, req)
	if err != nil {
		return err
	}
	if t != resp.typeName() {
		return fmt.Errorf("nacos: %s answered with type %q", req.typeName(), t)
	}
	if err := json.Unmarshal(body, resp); err != nil {
		return fmt.Errorf("nacos: decode %s: %w", t, err)
	}
	return nil
}

// CallNoResponse sends a unary request and discards the reply body; for probes
// where only transport-level success matters.
func (c *Conn) CallNoResponse(ctx context.Context, req envelope) error {
	_, _, err := c.Unary(ctx, req)
	return err
}

// Unary performs one request and returns the raw reply. Naming calls dispatch on
// the returned class name themselves because Nacos answers a subscribe with
// SubscribeServiceResponse and a query with QueryServiceResponse.
func (c *Conn) Unary(ctx context.Context, req envelope) (string, []byte, error) {
	p, err := marshalRequest(req, c.localIP())
	if err != nil {
		return "", nil, err
	}
	var out gen.Payload
	timeout := c.CallTimeout
	if timeout <= 0 {
		timeout = DefaultCallTimeout
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.cc.Invoke(cctx, methodUnary, p, &out); err != nil {
		return "", nil, fmt.Errorf("nacos: call %s: %w", req.typeName(), err)
	}
	c.touch()
	t, err := unmarshal(&out, nil)
	if err != nil {
		return t, nil, err
	}
	return t, append([]byte(nil), out.GetBody().GetValue()...), nil
}

// localIP is the address reported in Metadata.clientIp: the *peer* address of
// the channel when known, else the dial host. Nacos fills NetUtils.localIp().
func (c *Conn) localIP() string {
	return c.ip
}

// ConnectionID is the id the server assigned at serverCheck time. Empty means no
// live connection.
func (c *Conn) ConnectionID() string { v, _ := c.connectionID.Load().(string); return v }

// Addr is the server address this connection points at.
func (c *Conn) Addr() string { return c.addr }

// Abilities reports the table the server acknowledged during setup.
func (c *Conn) Abilities() map[string]bool {
	c.setupMu.Lock()
	defer c.setupMu.Unlock()
	return c.abilities
}

// Epoch counts successful handshakes. Owners compare it across calls to decide
// when to replay registrations and subscriptions after a reconnect.
func (c *Conn) Epoch() int64 { return c.epoch.Load() }

// Ready reports a live stream *and* a completed handshake. The earlier version
// returned true forever once the handshake latch was closed, which is how a
// silently dead connection looked healthy while its instances disappeared.
func (c *Conn) Ready() bool {
	c.setupMu.Lock()
	done := c.setupDone
	c.setupMu.Unlock()
	select {
	case <-done:
	default:
		return false
	}
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.stream != nil && !c.closed
}

// awaitSetup returns the latch for the *current* handshake attempt. Re-handshake
// replaces it, so callers must never hold on to a stale channel.
func (c *Conn) awaitSetup() chan struct{} {
	c.setupMu.Lock()
	defer c.setupMu.Unlock()
	return c.setupDone
}

func (c *Conn) markSetupDone() {
	c.setupMu.Lock()
	defer c.setupMu.Unlock()
	if c.handshaked {
		return
	}
	c.handshaked = true
	close(c.setupDone)
}

// StreamErr reports why the stream last died, if it has.
func (c *Conn) StreamErr() error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.streamErr
}

func (c *Conn) touch() { c.lastActive.Store(time.Now().UnixNano()) }

func (c *Conn) idleFor() time.Duration {
	last := c.lastActive.Load()
	if last == 0 {
		return time.Hour
	}
	return time.Since(time.Unix(0, last))
}

// CloseStream releases the current stream without closing the channel, so the
// owner can hand us a new one after recovery. Safe to call twice.
func (c *Conn) CloseStream() {
	c.sendMu.Lock()
	c.stream = nil
	c.sendMu.Unlock()
	c.setupMu.Lock()
	if !c.handshaked {
		c.setupMu.Unlock()
		return
	}
	c.setupDone = make(chan struct{})
	c.handshaked = false
	c.setupMu.Unlock()
}

// Close tears the connection down. Safe to call more than once.
func (c *Conn) Close() error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return nil
	}
	c.closed = true
	c.closedMu.Unlock()

	c.pushMu.Lock()
	close(c.pushes)
	c.pushMu.Unlock()
	c.sendMu.Lock()
	c.stream = nil
	c.sendMu.Unlock()
	c.cancel()
	return c.cc.Close()
}

// HostPort normalises host:port inputs.
func HostPort(host string, port int) string { return net.JoinHostPort(host, strconv.Itoa(port)) }
