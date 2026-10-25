package conn

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/gen"
)

// gRPC method names, derived from nacos_grpc_service.proto (no proto package,
// services Request / BiRequestStream).
const (
	methodUnary = "/Request/request"
	methodBidi  = "/BiRequestStream/requestBiStream"
)

// DefaultSetupTimeout mirrors harbor's config default and Nacos' own 3s
// behaviour: a setup that is not acknowledged within this window is a failed
// connection, not a slow one.
const DefaultSetupTimeout = 3 * time.Second

// Push is a server-initiated request arriving on the bidi stream. S4 consumes
// these (NotifySubscriberRequest); S2 only needs the channel to exist so the
// read loop never blocks on an unread push.
type Push struct {
	Type   string
	Body   []byte
	AckFor *gen.Payload // the frame to acknowledge, nil once acking is wired
}

// Conn is one connection to a Nacos server: a lazily opened bidi stream plus
// the unary RPC that shares the same grpc.ClientConn.
type Conn struct {
	addr string
	ip   string

	cc     *grpc.ClientConn
	ctx    context.Context // owns the long-lived bidi stream; cancelled by Close
	cancel context.CancelFunc

	sendMu    sync.Mutex // one Send per stream at a time
	stream    grpc.ClientStream
	streamErr error

	setupDone chan struct{}
	setupOnce sync.Once
	abilities map[string]bool

	pushMu sync.Mutex
	pushes chan Push

	closed   bool
	closedMu sync.Mutex

	SetupTimeout time.Duration
}

// Dial opens the transport. No I/O happens here: grpc.NewClient connects
// lazily, and the bidi stream (and therefore the handshake) starts in Setup.
// Extra opts are appended after the default insecure transport credentials,
// so a caller may override them (tests run against an in-process stub).
func Dial(addr string, opts ...grpc.DialOption) (*Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return nil, fmt.Errorf("nacos: server addr %q wants host:port", addr)
	}
	dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, opts...)
	cc, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("nacos: dial %s: %w", addr, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		addr:         net.JoinHostPort(host, port),
		ip:           host,
		cc:           cc,
		ctx:          ctx,
		cancel:       cancel,
		setupDone:    make(chan struct{}),
		pushes:       make(chan Push, 64),
		SetupTimeout: DefaultSetupTimeout,
	}
	return c, nil
}

// Setup opens the bidi stream and performs Nacos' connection handshake:
//
//	client → ConnectionSetupRequest (with a non-null abilityTable)
//	server → SetupAckRequest        (this is the only thing that makes the
//	                                 handshake complete; the server sends it
//	                                 async and does *not* wait for a reply)
//	client → SetupAckResponse       (what Nacos' own SetupRequestHandler answers
//	                                 with, GrpcClient.java:619-625; the server
//	                                 only uses it to refresh activity time, so
//	                                 it is courteous rather than required)
//
// Success is therefore defined as "SetupAckRequest received", not "three frames
// written". An earlier version of this file assumed the handshake needed the
// third frame before returning; the server never waits for it.
func (c *Conn) Setup(ctx context.Context, tenant string, labels map[string]string) error {
	if labels == nil {
		// RemoteConstants.java:32,40 — the values Nacos itself uses.
		labels = map[string]string{"source": "sdk", "module": "naming"}
	}
	// The stream lives on the connection's own context, not the caller's: a
	// short Setup timeout must not tear down a connection that is already up.
	if err := c.openStream(); err != nil {
		return err
	}
	setup := ConnectionSetupRequest{
		ClientVersion: ClientVersion,
		Tenant:        tenant,
		Labels:        labels,
		// Non-null even when empty: see the comment on the field.
		AbilityTable: map[string]bool{},
	}
	p, err := marshalRequest(setup, c.ip)
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
	case <-c.setupDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(timeout):
		return fmt.Errorf("nacos: %s did not acknowledge the setup within %s", c.addr, timeout)
	}
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
	go c.readLoop(cs)
	return nil
}

// readLoop owns receives on the bidi stream and dispatches: setup ack wakes
// Setup, everything else is offered to Pushes. It never blocks the stream.
func (c *Conn) readLoop(cs grpc.ClientStream) {
	for {
		var p gen.Payload
		if err := cs.RecvMsg(&p); err != nil {
			c.markStreamFailed(err)
			return
		}
		t, err := unmarshal(&p, nil)
		if err != nil {
			continue
		}
		switch t {
		case typeSetupAckRequest:
			var ack SetupAckRequest
			_, _ = unmarshal(&p, &ack)
			c.abilities = ack.AbilityTable
			if r, err := marshalResponse(SetupAckResponse{}); err == nil {
				_ = c.send(r)
			}
			c.setupOnce.Do(func() { close(c.setupDone) })
		default:
			c.offer(Push{Type: t, Body: append([]byte(nil), p.GetBody().GetValue()...), AckFor: &p})
		}
	}
}

// Pushes exposes server-initiated frames. S4 consumes it.
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
		// A full buffer means the consumer stalled; dropping a push is
		// recoverable (the next change re-pushes), blocking is not.
	}
}

func (c *Conn) markStreamFailed(err error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	if c.stream == nil {
		return
	}
	if err != nil && err != io.EOF {
		c.streamErr = err
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
		return fmt.Errorf("nacos: send %s: %w", p.GetMetadata().GetType(), err)
	}
	return nil
}

// Call performs one unary naming RPC. Correlation is gRPC's own — Nacos'
// naming protocol carries no requestId (verified: it appears only in
// DefaultRequestFuture for timeouts/logs, never in Metadata.headers).
func (c *Conn) Call(ctx context.Context, req envelope, resp envelope) error {
	if resp == nil {
		return c.CallNoResponse(ctx, req)
	}
	p, err := marshalRequest(req, c.ip)
	if err != nil {
		return err
	}
	var out gen.Payload
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.cc.Invoke(callCtx, methodUnary, p, &out); err != nil {
		return fmt.Errorf("nacos: call %s: %w", req.typeName(), err)
	}
	got, err := unmarshal(&out, resp)
	if err != nil {
		return err
	}
	if got != resp.typeName() {
		return fmt.Errorf("nacos: %s answered with type %q", req.typeName(), got)
	}
	return nil
}

// CallNoResponse sends a unary request and discards the reply body; useful for
// probes where only the transport-level success matters.
func (c *Conn) CallNoResponse(ctx context.Context, req envelope) error {
	p, err := marshalRequest(req, c.ip)
	if err != nil {
		return err
	}
	var out gen.Payload
	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := c.cc.Invoke(callCtx, methodUnary, p, &out); err != nil {
		return fmt.Errorf("nacos: call %s: %w", req.typeName(), err)
	}
	return nil
}

// Addr is the server address this connection points at.
func (c *Conn) Addr() string { return c.addr }

// Abilities reports the table the server acknowledged during setup.
func (c *Conn) Abilities() map[string]bool { return c.abilities }

// Ready reports whether the handshake completed.
func (c *Conn) Ready() bool {
	select {
	case <-c.setupDone:
		return true
	default:
		return false
	}
}

// Close tears the connection down. Safe to call more than once.
func (c *Conn) Close() error {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.pushMu.Lock()
	close(c.pushes)
	c.pushMu.Unlock()
	c.cancel()
	return c.cc.Close()
}

// HostPort normalises "host:port" inputs, rejecting anything without a port.
func HostPort(host string, port int) string { return net.JoinHostPort(host, strconv.Itoa(port)) }
