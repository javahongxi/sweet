package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
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
	// lifecycle owns the connection, the reconnect supervisor and the accessors;
	// embedding it keeps WaitReady / CurrentAddr / ConnectionID / Handshakes /
	// PushDropped / LastErr as part of this type's API, and shares the mutex so
	// connection state and intents are never guarded by two different locks.
	lifecycle

	opts          NamingOptions
	registrations map[string]*regIntent
	subscriptions map[string]*subIntent
	updates       chan ServiceInfo
	closed        bool
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
		opts:          opts,
		registrations: map[string]*regIntent{},
		subscriptions: map[string]*subIntent{},
		updates:       make(chan ServiceInfo, 16),
	}
	n.lifecycle = lifecycle{
		serversList: newServerList(addrs, 0),
		newConn:     n.dial,
		onReady:     func(conn *sdkConn, gen int64) { go n.consumePushes(conn, gen) },
		replay:      n.replay,
		logFn:       func(msg string, kv ...any) { n.logger().Log(n.ctx, slog.LevelDebug, "nacos: naming "+msg, kv...) },
		tenantFn:    func() string { return n.opts.Namespace },
		labelsFn:    func() map[string]string { return n.opts.Labels },
	}
	n.lifecycle.setup(n.serversList, ctx, cancel, 0, opts.ReconnectBackoff)
	n.lifecycle.start()
	return n, nil
}

// dial performs the naming-domain handshake on a fresh address. Every knob comes
// from this domain's Options, which is why the skeleton cannot own it.
func (n *NamingClient) dial(ctx context.Context, addr string) (*sdkConn, error) {
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
	conn.SetStreamLostHook(func(error) { n.streamLost(conn) })

	hctx, cancel := context.WithTimeout(ctx, n.budget())
	defer cancel()
	if err := conn.Setup(hctx, opts.Namespace, opts.Labels); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (n *NamingClient) logger() *slog.Logger {
	if n.opts.Logger == nil {
		return noopLogger
	}
	return n.opts.Logger
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

// Updates is the one channel every subscribed service's pushed instance lists
// arrive on — one per client, not one per service. A caller that subscribes more
// than one service demuxes by ServiceInfo.Name; registry/nacos' watcher fan-out is
// the in-repo example. Buffered 16, and only one goroutine may drain it.
//
// When that consumer stalls the buffer fills and the excess push is dropped with a
// Debug log line ("update channel full; dropping a push"). Note which counter that
// does NOT move: PushDropped and Options.OnPushDropped watch the connection's own
// push buffer, which overflows for a different reason (the consumer blocked, e.g. on
// an ack), so PushDropped()==0 is not proof nothing was lost. The consumer here never
// blocks on this buffer, so the ack path keeps running; the cost lands on freshness —
// a missed list means a stale view until the next change.
func (n *NamingClient) Updates() <-chan ServiceInfo { return n.updates }

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

// Close tears the connection down. It does not deregister — an owner that wants
// graceful withdrawal calls Deregister first, because a process that dies should
// not also erase the service it was serving.
func (n *NamingClient) Close() error {
	n.mu.Lock()
	n.closed = true
	n.mu.Unlock()
	return n.closeConn()
}

// --- lifecycle internals ---------------------------------------------------

// liveConn returns the current connection or an ErrUnavailable naming the list
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
			n.streamLost(conn)
		}
	}()
	for push := range conn.Pushes() {
		n.mu.Lock()
		current := n.generation == gen && n.conn == conn && !n.closed
		n.mu.Unlock()
		if !current {
			return
		}
		if push.Type == TypeConnectResetRequest {
			// Acked inside, on this stream, before the switch.
			handleConnectReset(n, push)
			continue
		}
		if push.Type != TypeNotifySubscriberRequest {
			// ClientDetectionRequest and friends: acked only when we know the
			// response class. An unknown type is left un-acked, matching the
			// official "no handler → no response" (GrpcClient.java:286-290).
			_ = conn.Ack(push)
			continue
		}
		note, err := DecodeNotify(push)
		if err != nil {
			_ = conn.Ack(push)
			continue
		}
		if err := conn.Ack(push); err != nil {
			n.streamLost(conn)
			continue
		}
		info := note.ServiceInfo
		info.Name = note.ServiceName()
		select {
		case n.updates <- info:
		default:
			// A stalled consumer must not block the ack path. This drop is NOT the
			// one PushDropped()/OnPushDropped report — those count overflows of the
			// connection's own push buffer (see Updates) — so the Debug log is the
			// only signal that a service list was lost here.
			n.log("update channel full; dropping a push", "service", info.Name)
		}
	}
}

// --- resetTarget: the hooks handleConnectReset needs ---

// instanceKey identifies one published instance. It mirrors registry/nacos'
// regKey so the two agree on what "the same registration" means.
func instanceKey(service string, in Instance) string {
	return service + "|" + in.IP + ":" + fmt.Sprint(in.Port)
}
