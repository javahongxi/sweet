package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

// NamingClient is the naming-domain connection owner: it keeps the SDK
// connection alive, rotates to another node when one fails, remembers what the
// application asked the server to believe (registrations and subscriptions), and
// re-establishes all of it after a reconnect.
//
// It is the naming counterpart of ConfigClient and shares that file's rules: one
// connection per domain (ConnectionBasedClientManager.java:60-64), credentials on
// every request (AccessGetter), connection-level failover only
// (RpcClient.java:717-723).
//
// The intents map is the whole point of the type. An ephemeral instance's
// liveness *is* the connection (jaws/doc/harbor-vs-nacos.md §8.5 — gRPC model has
// no beat), so a reconnect that does not re-register leaves the service looking
// empty to every consumer while this process believes it is published. registry/
// nacos earned that rule the hard way, which is why replay() mirrors it exactly
// rather than reinventing it:
//
//   - only live intents are replayed, and
//   - an intent is marked not-live *before* its deregistration is sent, so a
//     reconnect racing the caller cannot resurrect an instance that was asked to
//     go away (注销优先).
type NamingClient struct {
	servers *serverList
	opts    NamingOptions

	ctx    context.Context
	cancel context.CancelFunc
	lost   chan struct{}

	mu            sync.Mutex
	conn          *sdkConn
	addr          string
	generation    int64
	handshakes    int64
	signal        chan struct{}
	lastErr       error
	attempt       int
	reconnect     time.Duration
	registrations map[string]*regIntent
	subscriptions map[string]*subIntent
	closed        bool

	updates chan ServiceInfo
}

// regIntent is one instance this client wants published.
type regIntent struct {
	service string
	want    Instance
	live    bool
}

// subIntent is one service this client wants pushed.
type subIntent struct {
	service string
	live    bool
}

// NamingOptions configures a NamingClient.
type NamingOptions struct {
	// Addr is the Nacos gRPC SDK address (main port + 1000, i.e. 9848 for a
	// server on 8848). The offset is not applied here — pass the gRPC port.
	// Ignored when Servers is set.
	Addr string

	// Servers enables failover: a node whose handshake fails goes on cooldown and
	// the next attempt uses another address. Connection-level only; see serverList.
	Servers []string

	// Namespace and GroupName are the identity fields every naming request carries
	// (AbstractNamingRequest.java:30-34). Empty namespace means public.
	Namespace string
	GroupName string

	// HealthyOnly filters the instance list the server answers with.
	HealthyOnly bool

	// Labels go into ConnectionSetupRequest; module is forced to "naming" because
	// that label is what makes the naming module see this connection at all.
	Labels map[string]string

	Access        AccessGetter
	Logger        *slog.Logger
	OnPushDropped func(Push)
	OnError       func(error)

	// OnReplay runs after a successful reconnect replay with the services that
	// were re-subscribed. The owner uses it to nudge its watchers, because after a
	// reconnect the next push may never come if nothing changes.
	OnReplay func(services []string)

	ReconnectBackoff   time.Duration
	ServerCheckTimeout time.Duration
	SetupTimeout       time.Duration
	KeepAlive          time.Duration
	CallTimeout        time.Duration
}

// NewNamingClient builds the client and starts connecting in the background.
// Only structural problems (no address, module override) are returned as errors;
// unreachability is WaitReady's to report.
func NewNamingClient(opts NamingOptions) (*NamingClient, error) {
	addrs := opts.Servers
	if len(addrs) == 0 {
		if opts.Addr == "" {
			return nil, errors.New("nacos: NamingOptions.Addr or Servers required")
		}
		addrs = []string{opts.Addr}
	}
	if _, ok := opts.Labels["module"]; ok {
		return nil, errors.New("nacos: naming connection labels module=naming; do not set it")
	}
	if opts.GroupName == "" {
		opts.GroupName = "DEFAULT_GROUP"
	}

	labels := map[string]string{"source": "sdk", "module": "naming"}
	for k, v := range opts.Labels {
		labels[k] = v
	}
	opts.Labels = labels

	ctx, cancel := context.WithCancel(context.Background())
	n := &NamingClient{
		servers:       newServerList(addrs, 0),
		opts:          opts,
		ctx:           ctx,
		cancel:        cancel,
		lost:          make(chan struct{}, 1),
		signal:        make(chan struct{}),
		registrations: map[string]*regIntent{},
		subscriptions: map[string]*subIntent{},
		updates:       make(chan ServiceInfo, 16),
	}
	if opts.ReconnectBackoff > 0 {
		n.reconnect = opts.ReconnectBackoff
	} else {
		n.reconnect = time.Second
	}
	if err := n.reconnectOnce(ctx); err != nil {
		n.noteErr(err)
	}
	go n.supervise()
	return n, nil
}

// WaitReady blocks until a handshaked connection exists, ctx ends, or Close runs.
func (n *NamingClient) WaitReady(ctx context.Context) error {
	for {
		n.mu.Lock()
		conn, sig, msg := n.conn, n.signal, n.lastErr
		n.mu.Unlock()
		if conn != nil && conn.Ready() {
			return nil
		}
		select {
		case <-sig:
		case <-ctx.Done():
			return fmt.Errorf("%w: none of %s reachable (last error: %v)", ErrUnavailable, n.addrs(), msg)
		case <-n.ctx.Done():
			return n.ctx.Err()
		}
	}
}

// Register publishes one instance and records the intent, so a later reconnect
// puts it back.
func (n *NamingClient) Register(ctx context.Context, service string, in Instance) error {
	conn, err := n.liveConn()
	if err != nil {
		return err
	}
	if err := n.registerInstance(ctx, conn, service, TypeRegisterInstance, in); err != nil {
		return err
	}
	key := instanceKey(service, in)
	n.mu.Lock()
	n.registrations[key] = &regIntent{service: service, want: in, live: true}
	n.mu.Unlock()
	return nil
}

// Deregister withdraws one instance.
//
// The intent is marked not-live *before* the request goes out. A reconnect
// happening between the two would otherwise replay an instance its owner has
// already asked to remove — the ordering registry/nacos documents as 注销优先.
func (n *NamingClient) Deregister(ctx context.Context, service string, in Instance) error {
	key := instanceKey(service, in)
	n.mu.Lock()
	if rec := n.registrations[key]; rec != nil {
		rec.live = false
	}
	n.mu.Unlock()

	err := error(nil)
	if conn, cerr := n.liveConn(); cerr == nil {
		err = n.registerInstance(ctx, conn, service, TypeDeregisterInstance, in)
	}
	n.mu.Lock()
	delete(n.registrations, key)
	n.mu.Unlock()
	return err
}

// Subscribe turns pushes on for a service and returns the list the server
// answered with — the subscribe response already carries it (§8.4), so there is
// no separate seed query to race.
//
// The subscription intent is recorded whatever the wire call does, and the
// failure is returned alongside it. That is what lets a caller serve a cached
// view while the registry is unreachable and still get the live list the moment
// it heals: an intent recorded only on success would mean the reconnect has
// nothing to re-subscribe, and the stale view would never be replaced — exactly
// the guarantee registry/nacos' serveStaleOnFailure has been carrying.
func (n *NamingClient) Subscribe(ctx context.Context, service string) (ServiceInfo, error) {
	n.mu.Lock()
	n.subscriptions[service] = &subIntent{service: service, live: true}
	n.mu.Unlock()

	conn, err := n.liveConn()
	if err != nil {
		return ServiceInfo{}, err
	}
	info, err := n.callSubscribe(ctx, conn, service, true)
	if err != nil {
		return ServiceInfo{}, err
	}
	// Names come back qualified ("GROUP@@service"); normalising here means no
	// caller has to know the peeling rule, and the push path already does the
	// same thing in consumePushes.
	info.Name = trimGroup(info.Name, info.GroupName)
	return info, nil
}

// Unsubscribe stops pushes and forgets the intent.
func (n *NamingClient) Unsubscribe(ctx context.Context, service string) error {
	n.mu.Lock()
	if rec := n.subscriptions[service]; rec != nil {
		rec.live = false
	}
	delete(n.subscriptions, service)
	n.mu.Unlock()

	conn, err := n.liveConn()
	if err != nil {
		return nil // nothing left registered on a connection we do not have
	}
	_, err = n.callSubscribe(ctx, conn, service, false)
	return err
}

// Query asks what the server currently believes, without subscribing.
func (n *NamingClient) Query(ctx context.Context, service string) (ServiceInfo, error) {
	conn, err := n.liveConn()
	if err != nil {
		return ServiceInfo{}, err
	}
	return n.callQuery(ctx, conn, service)
}

// Updates carries every instance list the server pushed. It is drained by one
// consumer; if that consumer stalls the channel fills and drops are counted and
// reported, because a missed push here means a stale view until the next change.
func (n *NamingClient) Updates() <-chan ServiceInfo { return n.updates }

// Handshakes counts completed handshakes. A test that expects a replay must
// first prove a new handshake happened — otherwise "nothing was replayed" and
// "nothing was needed" look identical. Distinct from the generation counter,
// which also moves when a connection is merely retired.
func (n *NamingClient) Handshakes() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.handshakes
}

// Subscribed lists the services with a live subscription intent. It exists so
// the snapshot fallback's central claim — "a stale list was handed out *and* the
// subscription is still going to be re-established" — stays assertable from
// outside this package now that the intents live here.
func (n *NamingClient) Subscribed() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.subscriptions))
	for service, rec := range n.subscriptions {
		if rec.live {
			out = append(out, service)
		}
	}
	sort.Strings(out)
	return out
}

// Registered lists the instances this client still wants published, as
// "service|ip:port". Same reason as Subscribed: the 注销优先 guarantee has to be
// observable to be testable.
func (n *NamingClient) Registered() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.registrations))
	for key, rec := range n.registrations {
		if rec.live {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// CurrentAddr is the SDK address in use, "" while a reconnect is in flight.
func (n *NamingClient) CurrentAddr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.addr
}

// ConnectionID is the id the server gave the current connection, "" while
// reconnecting.
func (n *NamingClient) ConnectionID() string {
	n.mu.Lock()
	conn := n.conn
	n.mu.Unlock()
	if conn == nil {
		return ""
	}
	return conn.ConnectionID()
}

// PushDropped counts pushes discarded because Updates() was not drained.
func (n *NamingClient) PushDropped() int64 {
	n.mu.Lock()
	conn := n.conn
	n.mu.Unlock()
	if conn == nil {
		return 0
	}
	return conn.PushDropped()
}

// LastErr is the most recent handshake or replay failure.
func (n *NamingClient) LastErr() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastErr
}

// Close tears the connection down. It does not deregister — an owner that wants
// graceful withdrawal calls Deregister first, because a process that dies should
// not also erase the service it was serving.
func (n *NamingClient) Close() error {
	n.cancel()
	n.mu.Lock()
	conn := n.conn
	n.conn, n.addr = nil, ""
	n.closed = true
	n.broadcastLocked()
	n.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// --- lifecycle internals ---------------------------------------------------

// liveConn returns the current connection or an ErrUnavailable naming the list
// tried and why it last failed. Callers take the connection once and use it for
// the whole call: the supervisor may publish another one mid-flight, and writing
// through a retired connection is what the generation guard exists to prevent.
func (n *NamingClient) liveConn() (*sdkConn, error) {
	n.mu.Lock()
	c, msg := n.conn, n.lastErr
	n.mu.Unlock()
	if c == nil || !c.Ready() {
		return nil, fmt.Errorf("%w: no connection to %s (last error: %v)", ErrUnavailable, n.addrs(), msg)
	}
	return c, nil
}

// --- the three naming-domain RPCs ------------------------------------------
//
// They fill the identity fields every naming request carries — namespace and
// groupName, per AbstractNamingRequest.java:30-34 — and do nothing else: no
// intents, no retry, no lifecycle. That is the rest of NamingClient's job.

// registerInstance sends one InstanceRequest. action is TypeRegisterInstance or
// TypeDeregisterInstance; which to send is the caller's policy, not the wire
// format's.
func (n *NamingClient) registerInstance(ctx context.Context, c *sdkConn, service, action string, in Instance) error {
	req := InstanceRequest{
		Namespace: n.opts.Namespace, ServiceName: service, GroupName: n.opts.GroupName,
		Type: action, Instance: in,
	}
	t, body, err := c.Unary(ctx, req)
	if err != nil {
		return err
	}
	var resp Response
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("nacos: %s answered with type %q, undecodable: %w", req.Type, t, err)
	}
	return resp.classify("register "+req.Type, service)
}

// callSubscribe turns pushes on or off and returns the list the server answered
// with. Whether to remember that list is the caller's business.
func (n *NamingClient) callSubscribe(ctx context.Context, c *sdkConn, service string, subscribe bool) (ServiceInfo, error) {
	t, body, err := c.Unary(ctx, SubscribeServiceRequest{
		Namespace: n.opts.Namespace, ServiceName: service, GroupName: n.opts.GroupName, Subscribe: subscribe,
	})
	if err != nil {
		return ServiceInfo{}, err
	}
	if t != typeSubscribeServiceResponse {
		return ServiceInfo{}, fmt.Errorf("nacos: subscribe answered with type %q", t)
	}
	var resp SubscribeServiceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return ServiceInfo{}, err
	}
	if err := resp.classify("subscribe", service); err != nil {
		return ServiceInfo{}, err
	}
	return resp.ServiceInfo, nil
}

// callQuery asks what the server currently believes, independent of the push path.
func (n *NamingClient) callQuery(ctx context.Context, c *sdkConn, service string) (ServiceInfo, error) {
	t, body, err := c.Unary(ctx, ServiceQueryRequest{
		Namespace: n.opts.Namespace, ServiceName: service, GroupName: n.opts.GroupName,
		HealthyOnly: n.opts.HealthyOnly,
	})
	if err != nil {
		return ServiceInfo{}, err
	}
	if t != typeQueryServiceResponse {
		return ServiceInfo{}, fmt.Errorf("nacos: query answered with type %q", t)
	}
	var resp QueryServiceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return ServiceInfo{}, err
	}
	if err := resp.classify("query", service); err != nil {
		return ServiceInfo{}, err
	}
	return resp.ServiceInfo, nil
}

func (n *NamingClient) budget() time.Duration {
	if t := n.opts.SetupTimeout; t > 0 {
		return t * 4
	}
	return 12 * time.Second
}

func (n *NamingClient) addrs() string {
	all, bad := n.servers.Addrs()
	if len(bad) == 0 {
		return fmt.Sprint(all)
	}
	return fmt.Sprintf("%v (%v on cooldown)", all, bad)
}

// reconnectOnce tries once: the same address while it is still considered
// healthy, otherwise the next one from the list.
func (n *NamingClient) reconnectOnce(ctx context.Context) error {
	addr := n.servers.Pick()
	if addr == "" {
		return errors.New("nacos: no server address to try")
	}
	n.mu.Lock()
	conn, curAddr := n.conn, n.addr
	n.mu.Unlock()

	if conn != nil && curAddr == addr {
		conn.CloseStream()
		hctx, cancel := context.WithTimeout(ctx, n.budget())
		err := conn.Setup(hctx, n.opts.Namespace, n.opts.Labels)
		cancel()
		if err == nil {
			n.publish(conn, addr)
			return nil
		}
		n.servers.MarkFailed(addr)
		_ = conn.Close()
	}

	next, err := n.dialFor(ctx, addr)
	if err != nil {
		n.servers.MarkFailed(addr)
		return err
	}
	n.publish(next, addr)
	return nil
}

func (n *NamingClient) dialFor(ctx context.Context, addr string) (*sdkConn, error) {
	conn, err := dialConn(addr)
	if err != nil {
		return nil, err
	}
	opts := n.opts
	conn.ProbeFatal = true
	conn.Access = opts.Access
	conn.Logger = opts.Logger
	conn.OnPushDropped = func(p Push) {
		n.log("push dropped; a view may stay stale until the next change", "type", p.Type)
		if opts.OnPushDropped != nil {
			opts.OnPushDropped(p)
		}
	}
	if opts.ServerCheckTimeout > 0 {
		conn.ServerCheckTimeout = opts.ServerCheckTimeout
	}
	if opts.SetupTimeout > 0 {
		conn.SetupTimeout = opts.SetupTimeout
	}
	if opts.KeepAlive > 0 {
		conn.KeepAlive = opts.KeepAlive
	}
	if opts.CallTimeout > 0 {
		conn.CallTimeout = opts.CallTimeout
	}
	conn.SetStreamLostHook(func(error) { n.signalLost() })

	hctx, cancel := context.WithTimeout(ctx, n.budget())
	defer cancel()
	if err := conn.Setup(hctx, opts.Namespace, opts.Labels); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// publish installs a live connection and starts its consumer. The generation tag
// is what stops a previous connection's consumer from acking or delivering after
// it was superseded — registry/nacos learned that an untagged consumer logs
// "ack …: stream not open" forever and can overwrite newer state.
func (n *NamingClient) publish(conn *sdkConn, addr string) {
	n.mu.Lock()
	old := n.conn
	n.conn, n.addr = conn, addr
	n.generation++
	gen := n.generation
	n.handshakes++
	n.lastErr = nil
	n.attempt = 0
	n.broadcastLocked()
	n.mu.Unlock()

	n.servers.MarkHealthy(addr)
	if old != nil && old != conn {
		_ = old.Close()
	}
	n.log("connected", "addr", addr, "connectionId", conn.ConnectionID())
	go n.consumePushes(conn, gen)
}

// replay re-establishes every live intent on the current connection.
func (n *NamingClient) replay() error {
	n.mu.Lock()
	conn := n.conn
	regs := make([]*regIntent, 0, len(n.registrations))
	for _, rec := range n.registrations {
		if rec.live {
			regs = append(regs, rec)
		}
	}
	subs := make([]string, 0, len(n.subscriptions))
	for _, rec := range n.subscriptions {
		if rec.live {
			subs = append(subs, rec.service)
		}
	}
	n.mu.Unlock()
	if conn == nil {
		return errors.New("nacos: no connection to replay onto")
	}

	ctx, cancel := context.WithTimeout(n.ctx, 20*time.Second)
	defer cancel()
	// Deterministic order: map iteration would replay registrations in a random
	// sequence, which makes a failure impossible to reproduce.
	sort.Slice(regs, func(i, j int) bool {
		if regs[i].service != regs[j].service {
			return regs[i].service < regs[j].service
		}
		return instanceKey(regs[i].service, regs[i].want) < instanceKey(regs[j].service, regs[j].want)
	})
	sort.Strings(subs)

	for _, rec := range regs {
		if err := n.registerInstance(ctx, conn, rec.service, TypeRegisterInstance, rec.want); err != nil {
			return fmt.Errorf("replay register %s: %w", rec.service, err)
		}
	}
	for _, service := range subs {
		if _, err := n.callSubscribe(ctx, conn, service, true); err != nil {
			return fmt.Errorf("replay subscribe %s: %w", service, err)
		}
	}
	n.log("replayed", "registrations", len(regs), "subscriptions", len(subs))
	if n.opts.OnReplay != nil && len(subs) > 0 {
		n.opts.OnReplay(subs)
	}
	return nil
}

// consumePushes serves one connection until it is superseded.
func (n *NamingClient) consumePushes(conn *sdkConn, gen int64) {
	defer func() {
		if v := recover(); v != nil {
			n.log("push consumer panicked, dropping the connection", "err", fmt.Sprint(v))
			n.signalLost()
		}
	}()
	for push := range conn.Pushes() {
		n.mu.Lock()
		current := n.generation == gen && n.conn == conn && !n.closed
		n.mu.Unlock()
		if !current {
			return
		}
		if push.Type != TypeNotifySubscriberRequest {
			_ = conn.Ack(push) // ClientDetectionRequest & friends still need the echo
			continue
		}
		note, err := DecodeNotify(push)
		if err != nil {
			_ = conn.Ack(push)
			continue
		}
		if err := conn.Ack(push); err != nil {
			n.signalLost()
			continue
		}
		info := note.ServiceInfo
		info.Name = note.ServiceName()
		select {
		case n.updates <- info:
		default:
			// A stalled consumer must not block the ack path, but it must not be
			// invisible either: the drop is counted on the sdkConn and logged here.
			n.log("update channel full; dropping a push", "service", info.Name)
		}
	}
}

// signalLost retires the connection and wakes the supervisor. The address is not
// marked failed here — a stream the server ended is retried in place first.
func (n *NamingClient) signalLost() {
	n.mu.Lock()
	conn := n.conn
	n.conn, n.addr = nil, ""
	n.generation++
	n.broadcastLocked()
	n.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	select {
	case n.lost <- struct{}{}:
	default:
	}
}

func (n *NamingClient) supervise() {
	defer func() {
		if v := recover(); v != nil {
			n.log("supervisor panicked, restarting it", "err", fmt.Sprint(v))
			go n.supervise()
		}
	}()
	for {
		if n.ctx.Err() != nil {
			return
		}
		if n.connected() {
			select {
			case <-n.ctx.Done():
				return
			case <-n.lost:
				continue
			}
		}
		select {
		case <-n.ctx.Done():
			return
		case <-time.After(n.nextBackoff()):
		}
		if err := n.reconnectOnce(n.ctx); err != nil {
			n.noteErr(err)
			n.log("handshake failed", "err", err)
			continue
		}
		// The whole reason this type exists: without the replay a reconnect leaves
		// the service empty on the server while this process still believes it is
		// published.
		if err := n.replay(); err != nil {
			n.noteErr(err)
			n.log("replay failed", "err", err)
		}
	}
}

func (n *NamingClient) connected() bool {
	n.mu.Lock()
	conn := n.conn
	n.mu.Unlock()
	return conn != nil && conn.Ready()
}

func (n *NamingClient) nextBackoff() time.Duration {
	n.mu.Lock()
	n.attempt++
	a := n.attempt
	n.mu.Unlock()

	d := n.reconnect << min(a-1, 5)
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	if d <= 0 {
		return n.reconnect
	}
	return d + time.Duration(rand.Int64N(int64(d/4)+1))
}

func (n *NamingClient) noteErr(err error) {
	if err == nil {
		return
	}
	n.mu.Lock()
	n.lastErr = err
	n.mu.Unlock()
}

func (n *NamingClient) broadcastLocked() {
	close(n.signal)
	n.signal = make(chan struct{})
}

func (n *NamingClient) log(msg string, kv ...any) {
	if n.opts.Logger == nil {
		return
	}
	n.opts.Logger.Log(n.ctx, slog.LevelDebug, "nacos: naming "+msg, kv...)
}

// instanceKey identifies one published instance. It mirrors registry/nacos'
// regKey so the two agree on what "the same registration" means.
func instanceKey(service string, in Instance) string {
	return service + "|" + in.IP + ":" + fmt.Sprint(in.Port)
}
