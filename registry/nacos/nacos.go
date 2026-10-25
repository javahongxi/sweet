// Package nacos implements registry.Registry over Nacos' naming protocol with
// the client protocol written by hand (see nacos/conn) — no nacos-sdk-go.
//
// Scope is the ephemeral-instance path: register, deregister, subscribe, and
// pushes, all over gRPC. Nacos 2.x/3.x carries all of that on the gRPC surface,
// so this package uses no HTTP at all.
//
// Two things this package treats as load-bearing, both learned from the live
// server rather than from reading the request classes:
//
//   - Ephemeral liveness IS the connection (harbor-vs-nacos.md §8.5): the
//     server drops every instance of a connection the moment its stream goes
//     away, and there is no beat to keep it alive. So reconnect + replay is not
//     an optimisation, it is what keeps a registration existing.
//   - "注销优先" (§8.6): a deregistration must survive a reconnect. Redo data
//     therefore records an intent, not just a last-known value.
package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/javahongxi/sweet/nacos/conn"
	"github.com/javahongxi/sweet/registry"
)

// Options configures a Registry.
type Options struct {
	// ServerAddr is the SDK gRPC port, i.e. mainPort + 1000 (9848 for 8848).
	ServerAddr string
	// Namespace is the tenant. Empty means "public".
	Namespace string
	// GroupName defaults to DEFAULT_GROUP.
	GroupName string
	// HealthyOnly filters queried instances.
	HealthyOnly bool
	// WatchFilter, when set, receives every instance list this registry
	// publishes to watchers (initial snapshots and pushes). It exists for the
	// restart drill: convergence must be observed, not assumed.
	WatchFilter func(serviceName string, insts []registry.Instance)
	// ReconnectDelay is the gap between recovery attempts.
	ReconnectDelay time.Duration
	// Logger, if set, receives connection lifecycle events. Recovery that is
	// invisible is indistinguishable from recovery that never happens.
	Logger *log.Logger
}

// intent is the redo record: what the client *wants* to be true, so a
// reconnect can rebuild server state from intent rather than from whatever the
// server last acknowledged.
type intent struct {
	service string
	want    registry.Instance
	// live is false once deregister/unsubscribe ran, so replay skips it and a
	// finished lifecycle can never resurrect itself.
	live bool
}

// Registry is a Nacos-backed registry.
type Registry struct {
	opts Options

	mu         sync.Mutex
	c          *conn.Conn
	recovering bool
	closing    bool

	registrations map[string]*intent // "<service>|<host:port>" -> register intent
	subscriptions map[string]*intent // service -> subscribe intent
	watchers      map[string][]chan []registry.Instance
}

// New validates opts. No connection is opened yet.
func New(opts Options) (*Registry, error) {
	if opts.ServerAddr == "" {
		return nil, errors.New("nacos: Options.ServerAddr required")
	}
	if opts.Namespace == "" {
		opts.Namespace = "public"
	}
	if opts.GroupName == "" {
		opts.GroupName = "DEFAULT_GROUP"
	}
	if opts.ReconnectDelay <= 0 {
		opts.ReconnectDelay = time.Second
	}
	return &Registry{
		opts:          opts,
		registrations: map[string]*intent{},
		subscriptions: map[string]*intent{},
		watchers:      map[string][]chan []registry.Instance{},
	}, nil
}

// Name makes this registry answer the "nacos:///<service>" scheme.
func (r *Registry) Name() string { return "nacos" }

// Expose registers one instance and returns its deregistration.
func (r *Registry) Expose(ctx context.Context, serviceName string, in registry.Instance) (func(), error) {
	c, err := r.ensureConn(ctx)
	if err != nil {
		return nil, err
	}
	key := regKey(serviceName, in)
	if err := r.register(ctx, c, serviceName, in); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.registrations[key] = &intent{service: serviceName, want: in, live: true}
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			rec := r.registrations[key]
			if rec != nil {
				rec.live = false // 注销优先: replay must not bring it back
			}
			r.mu.Unlock()

			dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if dc, err := r.currentConn(); err == nil {
				if err := r.register(dctx, dc, serviceName, deregType(in)); err != nil {
					r.logf("deregister %s: %v", serviceName, err)
				}
			}

			r.mu.Lock()
			delete(r.registrations, key)
			r.mu.Unlock()
		})
	}, nil
}

// Watch subscribes to a service and returns its current instances, then every
// change pushed by the server.
func (r *Registry) Watch(ctx context.Context, serviceName string) (<-chan []registry.Instance, error) {
	c, err := r.ensureConn(ctx)
	if err != nil {
		return nil, err
	}
	ch := make(chan []registry.Instance, 4)
	r.mu.Lock()
	first := len(r.watchers[serviceName]) == 0
	r.watchers[serviceName] = append(r.watchers[serviceName], ch)
	r.mu.Unlock()

	if first {
		snapshot, err := r.subscribe(ctx, c, serviceName, true)
		if err != nil {
			r.dropWatcher(serviceName, ch)
			return nil, fmt.Errorf("nacos: subscribe %s: %w", serviceName, err)
		}
		r.mu.Lock()
		r.subscriptions[serviceName] = &intent{service: serviceName, live: true}
		r.mu.Unlock()
		// A subscribe response already carries the current list (§8.4), so no
		// separate query is needed to seed the view.
		select {
		case ch <- snapshot:
		default:
		}
	}
	return ch, nil
}

// Unsubscribe stops pushes for a service and forgets its watchers.
func (r *Registry) Unsubscribe(ctx context.Context, serviceName string) error {
	r.mu.Lock()
	chans := r.watchers[serviceName]
	delete(r.watchers, serviceName)
	sub := r.subscriptions[serviceName]
	if sub != nil {
		sub.live = false
	}
	delete(r.subscriptions, serviceName)
	r.mu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
	c, err := r.currentConn()
	if err != nil {
		return nil // nothing subscribed server-side that we can still reach
	}
	_, err = r.subscribe(ctx, c, serviceName, false)
	return err
}

// Close stops recovery and drops the connection. Registrations are left to the
// server's connection-scoped cleanup: losing the stream removes them, which is
// the same mechanism that made them exist.
func (r *Registry) Close() error {
	r.mu.Lock()
	r.closing = true
	c := r.c
	r.c = nil
	for name, chans := range r.watchers {
		for _, ch := range chans {
			close(ch)
		}
		delete(r.watchers, name)
	}
	r.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}

func (r *Registry) ensureConn(ctx context.Context) (*conn.Conn, error) {
	r.mu.Lock()
	c := r.c
	r.mu.Unlock()
	if c != nil && c.Ready() {
		return c, nil
	}
	fresh, err := r.dialAndHandshake(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.c != nil && r.c.Ready() {
		old := r.c
		r.mu.Unlock()
		_ = fresh.Close()
		_ = old.Close()
		return old, nil
	}
	if r.closing {
		r.mu.Unlock()
		_ = fresh.Close()
		return nil, errors.New("nacos: registry closed")
	}
	r.c = fresh
	r.mu.Unlock()
	go r.consumePushes(fresh)
	return fresh, nil
}

// dialAndHandshake opens a Conn, wires recovery, and performs the handshake.
// The hook must be installed before Setup so a stream that dies immediately is
// still caught.
func (r *Registry) dialAndHandshake(ctx context.Context) (*conn.Conn, error) {
	c, err := conn.Dial(r.opts.ServerAddr)
	if err != nil {
		return nil, err
	}
	c.SetStreamLostHook(func(cause error) { r.onStreamLost(c, cause) })
	if err := c.Setup(ctx, r.opts.Namespace, nil); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("nacos: handshake with %s: %w", r.opts.ServerAddr, err)
	}
	return c, nil
}

// onStreamLost is the redo trigger. Per §8.6 the disconnect itself sends
// nothing; a scheduled pass replays every intent still marked live.
func (r *Registry) onStreamLost(dead *conn.Conn, cause error) {
	r.logf("connection to %s lost (%v); scheduling reconnect", dead.Addr(), cause)
	dead.CloseStream()

	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return
	}
	if r.recovering {
		r.mu.Unlock()
		return
	}
	if r.c == dead {
		r.c = nil
	}
	r.recovering = true
	r.mu.Unlock()

	go r.recover()
}

func (r *Registry) recover() {
	for attempt := 0; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		c, err := r.dialAndHandshake(ctx)
		status := r.connStatus()
		cancel()
		if status == connStatusClosed {
			return
		}
		if err == nil {
			r.mu.Lock()
			r.c = c
			r.recovering = false
			r.mu.Unlock()
			go r.consumePushes(c)
			if err := r.replay(); err != nil {
				r.logf("replay after reconnect: %v", err)
			}
			return
		}
		r.logf("reconnect attempt %d failed: %v", attempt+1, err)
		r.mu.Lock()
		closing := r.closing
		r.mu.Unlock()
		if closing {
			return
		}
		time.Sleep(r.opts.ReconnectDelay * time.Duration(attempt+1))
		if attempt > 30 {
			r.logf("giving up reconnecting to %s after %d attempts", r.opts.ServerAddr, attempt+1)
			return
		}
	}
}

// replay re-sends every still-live intent. Instances first, then subscriptions:
// a subscriber of a service we ourselves provide should not observe a moment
// where we are subscribed to nothing we publish.
func (r *Registry) replay() error {
	r.mu.Lock()
	regs := make([]*intent, 0, len(r.registrations))
	for _, rec := range r.registrations {
		if rec.live {
			regs = append(regs, rec)
		}
	}
	subs := make([]*intent, 0, len(r.subscriptions))
	for _, rec := range r.subscriptions {
		if rec.live {
			subs = append(subs, rec)
		}
	}
	c := r.c
	r.mu.Unlock()
	if c == nil {
		return errors.New("nacos: no connection to replay onto")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, rec := range regs {
		if err := r.register(ctx, c, rec.service, rec.want); err != nil {
			return err
		}
	}
	for _, rec := range subs {
		if _, err := r.subscribe(ctx, c, rec.service, true); err != nil {
			return err
		}
		r.deliver(rec.service, nil, true) // nudge watchers with whatever the server sends next
	}
	r.logf("replayed %d registration(s) and %d subscription(s)", len(regs), len(subs))
	return nil
}

// consumePushes is the single reader of one connection's pushes. The
// generation check makes a goroutine belonging to a dead connection stop as soon
// as its channel closes, instead of racing a newer connection's deliveries.
func (r *Registry) consumePushes(c *conn.Conn) {
	for push := range c.Pushes() {
		// A goroutine belonging to a superseded connection must stop touching
		// anything: acking on a stream that is already gone was producing
		// "ack ClientDetectionRequest: nacos: stream not open" in the logs,
		// and delivering from a stale connection would overwrite newer state.
		if !r.stillCurrent(c) {
			return
		}
		if push.Type != conn.TypeNotifySubscriberRequest {
			// Anything else (ConnectResetRequest, ClientDetectionRequest, …) is
			// a server request that expects an answer: ack it so the server does
			// not hold the slot and re-push.
			if err := c.Ack(push); err != nil {
				r.logf("ack %s: %v", push.Type, err)
			}
			continue
		}
		var note notifyPush
		if err := json.Unmarshal(push.Body, &note); err != nil {
			r.logf("decode %s: %v", push.Type, err)
			continue
		}
		name := note.serviceName()
		if err := c.Ack(push); err != nil { // stop the re-push loop (§8.4)
			r.logf("ack %s: %v", push.Type, err)
		}
		r.deliver(name, toInstances(note.ServiceInfo.Hosts), false)
	}
}

func (r *Registry) register(ctx context.Context, c *conn.Conn, serviceName string, in registry.Instance) error {
	req := conn.InstanceRequest{
		Namespace:   r.opts.Namespace,
		ServiceName: serviceName,
		GroupName:   r.opts.GroupName,
		Type:        actionFor(in),
		Instance:    toWireInstance(in),
	}
	t, body, err := c.Unary(ctx, req)
	if err != nil {
		return err
	}
	var resp conn.Response
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("nacos: %s answered with type %q, undecodable: %w", req.Type, t, err)
	}
	if !resp.Success {
		return fmt.Errorf("nacos: %s refused (%d): %s", req.Type, resp.ErrorCode, resp.ErrorMsg)
	}
	return nil
}

func (r *Registry) subscribe(ctx context.Context, c *conn.Conn, serviceName string, subscribe bool) ([]registry.Instance, error) {
	t, body, err := c.Unary(ctx, conn.SubscribeServiceRequest{
		Namespace:   r.opts.Namespace,
		ServiceName: serviceName,
		GroupName:   r.opts.GroupName,
		Subscribe:   subscribe,
	})
	if err != nil {
		return nil, err
	}
	if t != conn.TypeSubscribeServiceResponse {
		return nil, fmt.Errorf("nacos: subscribe answered with type %q", t)
	}
	var resp conn.SubscribeServiceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("nacos: subscribe refused (%d): %s", resp.ErrorCode, resp.ErrorMsg)
	}
	return toInstances(resp.ServiceInfo.Hosts), nil
}

// query is a plain ServiceQueryRequest; tests use it to assert what the server
// believes is registered, independently of the push path.
func (r *Registry) query(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	c, err := r.currentConn()
	if err != nil {
		return nil, err
	}
	t, body, err := c.Unary(ctx, conn.ServiceQueryRequest{
		Namespace:   r.opts.Namespace,
		ServiceName: serviceName,
		GroupName:   r.opts.GroupName,
		HealthyOnly: r.opts.HealthyOnly,
	})
	if err != nil {
		return nil, err
	}
	if t != "QueryServiceResponse" {
		return nil, fmt.Errorf("nacos: query answered with type %q", t)
	}
	var resp conn.QueryServiceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, fmt.Errorf("nacos: query refused (%d): %s", resp.ErrorCode, resp.ErrorMsg)
	}
	return toInstances(resp.ServiceInfo.Hosts), nil
}

// stillCurrent reports whether c is the connection this registry is now using.
func (r *Registry) stillCurrent(c *conn.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.c == c
}

func (r *Registry) currentConn() (*conn.Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.c == nil || !r.c.Ready() {
		return nil, errors.New("nacos: not connected")
	}
	return r.c, nil
}

const (
	connStatusLive = iota
	connStatusClosed
)

func (r *Registry) connStatus() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		return connStatusClosed
	}
	return connStatusLive
}

func (r *Registry) deliver(serviceName string, insts []registry.Instance, nudge bool) {
	if r.opts.WatchFilter != nil && !nudge {
		r.opts.WatchFilter(serviceName, insts)
	}
	r.mu.Lock()
	chans := append([]chan []registry.Instance(nil), r.watchers[serviceName]...)
	r.mu.Unlock()
	for _, ch := range chans {
		if nudge {
			select {
			case ch <- nil:
			default:
			}
			continue
		}
		select {
		case ch <- insts:
		default:
			// Dropping one update is recoverable (the next push replaces it);
			// blocking the dispatcher would stall every other service.
		}
	}
}

func (r *Registry) dropWatcher(serviceName string, ch chan []registry.Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.watchers[serviceName][:0]
	for _, w := range r.watchers[serviceName] {
		if w != ch {
			kept = append(kept, w)
		}
	}
	if len(kept) == 0 {
		delete(r.watchers, serviceName)
		return
	}
	r.watchers[serviceName] = kept
}

func (r *Registry) logf(format string, args ...any) {
	if r.opts.Logger != nil {
		r.opts.Logger.Printf("sweet/registry/nacos: "+format, args...)
	}
}

// notifyPush mirrors NotifySubscriberRequest. Measured on a live 3.2.3 server
// its JSON is {"headers":{},"requestId":"5","serviceInfo":{...}} — there is no
// top-level serviceName; identity lives in serviceInfo.name and is qualified
// ("GROUP@@service" whenever the group is not DEFAULT_GROUP).
type notifyPush struct {
	Headers     map[string]string `json:"headers,omitempty"`
	RequestID   string            `json:"requestId,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	ServiceInfo conn.ServiceInfo  `json:"serviceInfo"`
}

func (n notifyPush) serviceName() string {
	return trimGroup(n.ServiceInfo.Name, n.ServiceInfo.GroupName)
}

func trimGroup(qualified, group string) string {
	if qualified == "" {
		return qualified
	}
	if group != "" && group != "DEFAULT_GROUP" {
		return strings.TrimPrefix(qualified, group+"@@")
	}
	return strings.TrimPrefix(strings.TrimPrefix(qualified, "DEFAULT_GROUP@@"), "default@@")
}

func toInstances(hosts []conn.Instance) []registry.Instance {
	out := make([]registry.Instance, 0, len(hosts))
	for _, h := range hosts {
		if !h.Enabled {
			continue
		}
		out = append(out, registry.Instance{
			Host: h.IP, Port: h.Port, Weight: int(h.Weight), Metadata: h.Metadata,
		})
	}
	return out
}

func toWireInstance(in registry.Instance) conn.Instance {
	meta := map[string]string{}
	for k, v := range in.Metadata {
		meta[k] = v
	}
	if len(meta) == 0 {
		meta = nil
	}
	weight := float64(in.Weight)
	if weight == 0 {
		weight = 100
	}
	return conn.Instance{
		IP: in.Host, Port: in.Port, Weight: weight,
		Healthy: true, Enabled: true, Ephemeral: true,
		ClusterName: "DEFAULT", Metadata: meta,
	}
}

const deregMarker = "\x00deregister"

// deregType tags an instance for deregistration by carrying the action in the
// instance metadata until the request is built; keeps Expose's closure and
// replay on one code path.
func deregType(in registry.Instance) registry.Instance {
	out := registry.Instance{Host: in.Host, Port: in.Port, Weight: in.Weight}
	out.Metadata = map[string]string{deregMarker: "1"}
	return out
}

func actionFor(in registry.Instance) string {
	if in.Metadata[deregMarker] == "1" {
		return conn.TypeDeregisterInstance
	}
	return conn.TypeRegisterInstance
}

func regKey(serviceName string, in registry.Instance) string {
	return serviceName + "|" + in.Host + ":" + fmt.Sprint(in.Port)
}
