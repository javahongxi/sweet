// Package nacos implements registry.Registry over Nacos' naming protocol with
// the client protocol written by hand (see the nacos package) — no nacos-sdk-go.
//
// This package is now only the adapter: registry.Registry's shape (Expose returns
// a closure, Watch returns a channel, instances are host/port/metadata), the
// watcher fan-out, and the disk snapshot fallback. The connection itself —
// handshake, keepalive, reconnect, failover across a server list, and the
// intents a reconnect must rebuild — belongs to nacos.NamingClient.
//
// What that means for the two load-bearing rules this package learned from the
// live server rather than from reading request classes:
//
//   - Ephemeral liveness IS the connection (harbor-vs-nacos.md §8.5): the server
//     drops every instance of a connection whose stream goes away, and there is
//     no beat to keep it alive. Reconnect + replay is not an optimisation, it is
//     what keeps a registration existing.
//   - "注销优先" (§8.6): a deregistration must survive a reconnect. The intent is
//     marked not-live before the request goes out, inside NamingClient.Deregister.
//
// Both are now asserted one layer down (nacos/naming_client_test.go); the
// registry-side tests that used to be their only proof still pass unchanged.
package nacos

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	naming "github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

// Compile-time proof that this adapter satisfies the abstraction, without any
// call site having to say so. registry/direct asserts the same for its backend;
// resolver does it for grpc's Builder and Resolver interfaces.
var _ registry.Registry = (*Registry)(nil)

// Options configures a Registry.
type Options struct {
	// ServerAddr is the SDK gRPC port, i.e. mainPort + 1000 (9848 for 8848).
	// Ignored when Servers is set.
	ServerAddr string
	// Servers enables failover across a cluster; forwarded to NamingClient.
	Servers []string
	// Namespace is the tenant. Empty means "public".
	Namespace string
	// GroupName defaults to DEFAULT_GROUP.
	GroupName string
	// HealthyOnly filters queried instances.
	HealthyOnly bool
	// Access supplies credentials for every request. Nil means the server does
	// not require auth.
	Access naming.AccessGetter
	// WatchFilter, when set, receives every instance list this registry
	// publishes to watchers (initial snapshots and pushes). It exists for the
	// restart drill: convergence must be observed, not assumed.
	WatchFilter func(serviceName string, insts []registry.Instance)
	// ReconnectDelay is the first gap between reconnection attempts; the client
	// backs off exponentially from it, capped at 30s.
	ReconnectDelay time.Duration
	// Logger, if set, receives registry-side events. Recovery that is invisible
	// is indistinguishable from recovery that never happens.
	Logger *log.Logger
	// SnapshotDir, when set, enables the disk snapshot of service lists: every
	// list the server actually gave us is written there, and a failed subscribe
	// may fall back to the last one instead of returning nothing. Empty — the
	// default — means no file is ever read or written: serving a list whose age
	// nobody asked for is a liability, not a convenience. See snapshot.go for the
	// rules that keep a fallback honest.
	SnapshotDir string
	// OnSnapshotFallback, when set, is called with every list handed out from
	// disk, together with how old it is. This is the hook a caller uses to
	// alarm or refuse; staleness must not be visible only in the log.
	OnSnapshotFallback func(serviceName string, insts []registry.Instance, age time.Duration)
}

type namingClient interface {
	Register(context.Context, string, naming.Instance) error
	Deregister(context.Context, string, naming.Instance) error
	Subscribe(context.Context, string) (naming.ServiceInfo, error)
	Unsubscribe(context.Context, string) error
	Query(context.Context, string) (naming.ServiceInfo, error)
	Updates() <-chan naming.ServiceInfo
	Subscribed() []string
	Close() error
}

type watcher struct {
	ch   chan []registry.Instance
	done chan struct{}
}

type subscribeAttempt struct {
	done chan struct{}
}

type serviceSubscription struct {
	// mu orders Subscribe/Unsubscribe for one service. It is acquired before
	// Registry.mu when both are needed; no network call runs with Registry.mu held.
	mu sync.Mutex
	// attempt is guarded by Registry.mu and closed after its initial view is sent.
	attempt *subscribeAttempt
}

// Registry is a Nacos-backed registry: an adapter over nacos.NamingClient.
type Registry struct {
	opts   Options
	client namingClient

	// mu guards watchers, views and closing. A watcher channel is sent to or
	// closed only while mu is held; every send is non-blocking, so this rule
	// prevents send/close races without letting a slow consumer stall the pump.
	mu       sync.Mutex
	watchers map[string][]*watcher
	views    map[string][]registry.Instance
	closing  bool
	pumpDone chan struct{}

	// subscriptions keep per-service ordering and initial-subscription state.
	// Entries stay after the last watcher leaves so concurrent cleanup and a new
	// Watch always refer to the same gate.
	subscriptions map[string]*serviceSubscription
}

// New validates opts and starts the underlying client, which connects in the
// background: no call here blocks on the server.
//
// One deliberate change from the implementation this replaces: the reconnect loop
// no longer gives up after 31 attempts. A capped exponential backoff is quiet
// enough to run forever, and a registry that stops trying after half an hour of
// Nacos outage is worse than one that keeps knocking.
func New(opts Options) (*Registry, error) {
	if opts.ServerAddr == "" && len(opts.Servers) == 0 {
		return nil, errors.New("nacos: Options.ServerAddr or Servers required")
	}
	// Namespace is not filled here: NewNamingClient normalises an empty one to
	// "public" the way the official client does, and this used to be the adapter
	// quietly doing the library's job.
	if opts.GroupName == "" {
		opts.GroupName = "DEFAULT_GROUP"
	}
	if opts.ReconnectDelay <= 0 {
		opts.ReconnectDelay = time.Second
	}

	r := &Registry{
		opts:          opts,
		watchers:      map[string][]*watcher{},
		views:         map[string][]registry.Instance{},
		subscriptions: map[string]*serviceSubscription{},
		pumpDone:      make(chan struct{}),
	}
	client, err := naming.NewNamingClient(naming.NamingOptions{
		Addr:             opts.ServerAddr,
		Servers:          opts.Servers,
		Namespace:        opts.Namespace,
		GroupName:        opts.GroupName,
		HealthyOnly:      opts.HealthyOnly,
		Access:           opts.Access,
		ReconnectBackoff: opts.ReconnectDelay,
		// After a reconnect the next push may never arrive if nothing changes, so
		// nudge watchers: their next resolver refresh re-queries.
		OnReplay: r.nudgeAfterReplay,
	})
	if err != nil {
		return nil, err
	}
	r.client = client
	go r.pump()
	return r, nil
}

// Name makes this registry answer the "nacos:///<service>" scheme.
func (r *Registry) Name() string { return "nacos" }

// Expose registers one instance and returns its deregistration.
func (r *Registry) Expose(ctx context.Context, serviceName string, in registry.Instance) (func(), error) {
	wire := toWireInstance(in)
	if err := r.client.Register(ctx, serviceName, wire); err != nil {
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.client.Deregister(dctx, serviceName, wire); err != nil {
				r.logf("deregister %s: %v", serviceName, err)
			}
		})
	}, nil
}

// Watch subscribes to a service and returns its current instances, then every
// change pushed by the server.
//
// When the subscribe cannot reach the server, a configured snapshot may be handed
// out instead — see snapshot.go and Options.SnapshotDir. That path keeps the
// subscription intent (NamingClient.Subscribe records it even when the call
// fails), so the reconnect re-subscribes and the fresh list replaces the stale
// one. It is not a way to stop paying attention to Nacos.
func (r *Registry) Watch(ctx context.Context, serviceName string) (<-chan []registry.Instance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := &watcher{ch: make(chan []registry.Instance, 4), done: make(chan struct{})}
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return nil, errors.New("nacos: registry closed")
	}
	sub := r.subscriptionLocked(serviceName)
	r.watchers[serviceName] = append(r.watchers[serviceName], w)
	insts, seeded := r.views[serviceName]
	if seeded {
		// The new channel is empty, so its initial snapshot cannot block or be
		// displaced by a push. An empty list is a valid snapshot too.
		w.ch <- insts
	}
	r.mu.Unlock()

	go r.watchContext(ctx, serviceName, w, sub)
	if seeded {
		return w.ch, nil
	}

	for {
		r.mu.Lock()
		active := watcherRegisteredLocked(r.watchers[serviceName], w)
		_, seeded = r.views[serviceName]
		closing := r.closing
		attempt := sub.attempt
		ownsAttempt := false
		if active && !seeded && attempt == nil {
			attempt = &subscribeAttempt{done: make(chan struct{})}
			sub.attempt = attempt
			ownsAttempt = true
		}
		r.mu.Unlock()

		if !active {
			return nil, watchStoppedError(ctx, serviceName, closing)
		}
		if seeded {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return w.ch, nil
		}
		if ownsAttempt {
			// This goroutine owns the one wire subscription shared by every
			// watcher currently waiting for the first view.
			return r.subscribeInitial(ctx, serviceName, w, sub, attempt)
		}

		// Another Watch is subscribing. Waiting on its completion rather than
		// its network lock lets this watcher leave promptly when its own context
		// ends, and lets it retry if the first caller's context fails.
		select {
		case <-attempt.done:
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.done:
			r.mu.Lock()
			closing = r.closing
			r.mu.Unlock()
			return nil, watchStoppedError(ctx, serviceName, closing)
		}
	}
}

func (r *Registry) subscriptionLocked(serviceName string) *serviceSubscription {
	sub := r.subscriptions[serviceName]
	if sub == nil {
		sub = &serviceSubscription{}
		if r.subscriptions == nil {
			r.subscriptions = map[string]*serviceSubscription{}
		}
		r.subscriptions[serviceName] = sub
	}
	return sub
}

func watchStoppedError(ctx context.Context, serviceName string, closing bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if closing {
		return errors.New("nacos: registry closed")
	}
	return fmt.Errorf("nacos: watch %s stopped before its initial subscription", serviceName)
}

func (r *Registry) subscribeInitial(ctx context.Context, serviceName string, w *watcher,
	sub *serviceSubscription, attempt *subscribeAttempt) (<-chan []registry.Instance, error) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	defer func() {
		r.mu.Lock()
		sub.attempt = nil
		close(attempt.done)
		r.mu.Unlock()
	}()

	// Unsubscribe or last-watcher cleanup may have removed this watcher while
	// it waited for the per-service wire-operation gate.
	r.mu.Lock()
	active := watcherRegisteredLocked(r.watchers[serviceName], w)
	_, seeded := r.views[serviceName]
	closing := r.closing
	r.mu.Unlock()
	if !active {
		return nil, watchStoppedError(ctx, serviceName, closing)
	}
	if seeded {
		return w.ch, nil
	}

	info, err := r.client.Subscribe(ctx, serviceName)
	if err != nil {
		if r.snapshotEnabled() {
			if ferr := r.serveStaleOnFailure(ctx, serviceName, w, err); ferr != nil {
				return nil, ferr
			}
			return w.ch, nil
		}
		r.dropWatcher(serviceName, w)
		return nil, fmt.Errorf("nacos: subscribe %s: %w", serviceName, err)
	}

	// A subscribe response already carries the current list (§8.4), so no
	// separate query is needed. Deliver through the common path both to cache
	// the view and to seed watchers that joined while Subscribe was in flight.
	r.deliver(serviceName, toInstances(info.Hosts), false)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return w.ch, nil
}

func watcherRegisteredLocked(watchers []*watcher, w *watcher) bool {
	for _, candidate := range watchers {
		if candidate == w {
			return true
		}
	}
	return false
}

// watchContext owns the local half of cancellation. The server subscription is
// shared, so only removing the last watcher also unsubscribes at Nacos.
func (r *Registry) watchContext(ctx context.Context, serviceName string, w *watcher, sub *serviceSubscription) {
	select {
	case <-ctx.Done():
		// Serialize with a new first Subscribe for this service. A late
		// Unsubscribe must not erase that new watcher's subscription intent.
		sub.mu.Lock()
		defer sub.mu.Unlock()
		if r.dropWatcher(serviceName, w) {
			uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.client.Unsubscribe(uctx, serviceName); err != nil {
				r.logf("unsubscribe %s after the last watcher stopped: %v", serviceName, err)
			}
		}
	case <-w.done:
	}
}

// Unsubscribe stops pushes for a service and forgets its watchers.
func (r *Registry) Unsubscribe(ctx context.Context, serviceName string) error {
	r.mu.Lock()
	sub := r.subscriptionLocked(serviceName)
	r.mu.Unlock()
	sub.mu.Lock()
	defer sub.mu.Unlock()
	r.mu.Lock()
	r.closeServiceWatchersLocked(serviceName)
	r.mu.Unlock()
	// Unsubscribe clears the intent before sending, so a reconnect racing this
	// call will not re-subscribe. A client we cannot reach has nothing left to
	// unsubscribe anyway, which Unsubscribe reports as success.
	return r.client.Unsubscribe(ctx, serviceName)
}

// Query is a plain ServiceQueryRequest: what the server believes right now,
// independent of the push path. Tests assert against it.
func (r *Registry) Query(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	info, err := r.client.Query(ctx, serviceName)
	if err != nil {
		return nil, err
	}
	return toInstances(info.Hosts), nil
}

// Close drops the connection and every watcher. Registrations are left to the
// server's connection-scoped cleanup: losing the stream removes them, which is
// the same mechanism that made them exist.
func (r *Registry) Close() error {
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return nil
	}
	r.closing = true
	close(r.pumpDone)
	for serviceName := range r.watchers {
		r.closeServiceWatchersLocked(serviceName)
	}
	r.mu.Unlock()

	return r.client.Close()
}

// --- the adapter's own concerns: push fan-out, logging, instance conversion ---

// pump turns the client's push stream into watcher fan-out and snapshots. It is
// the only place the registry learns about changes.
func (r *Registry) pump() {
	for {
		select {
		case <-r.pumpDone:
			return
		case info, ok := <-r.client.Updates():
			if !ok {
				return
			}
			// NamingClient already peels the "GROUP@@" qualification off both
			// the subscribe response and the push, so the name here is the one
			// the watcher asked for.
			r.deliver(info.Name, toInstances(info.Hosts), false)
			r.remember(info.Name, info.Hosts)
		}
	}
}

// nudgeAfterReplay wakes watchers of the services that were re-subscribed after a
// reconnect. Without it a watcher that missed pushes during the outage keeps its
// stale list until something unrelated changes.
func (r *Registry) nudgeAfterReplay(services []string) {
	for _, s := range services {
		r.deliver(s, nil, true)
	}
}

func (r *Registry) deliver(serviceName string, insts []registry.Instance, nudge bool) {
	if r.opts.WatchFilter != nil && !nudge {
		r.opts.WatchFilter(serviceName, insts)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	watchers := r.watchers[serviceName]
	if len(watchers) == 0 {
		return
	}
	if !nudge {
		r.views[serviceName] = insts
	}
	for _, w := range watchers {
		if nudge {
			select {
			case w.ch <- nil:
			default:
			}
			continue
		}
		select {
		case w.ch <- insts:
		default:
			// Dropping one update is recoverable (the next push replaces it);
			// blocking the dispatcher would stall every other service.
		}
	}
}

// dropWatcher removes and closes w if it is still registered. Its return value
// tells the cancellation path whether it removed the last watcher and should
// also clear the shared server-side subscription.
func (r *Registry) dropWatcher(serviceName string, w *watcher) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	watchers := r.watchers[serviceName]
	for i, candidate := range watchers {
		if candidate != w {
			continue
		}
		copy(watchers[i:], watchers[i+1:])
		watchers[len(watchers)-1] = nil
		watchers = watchers[:len(watchers)-1]
		close(w.done)
		close(w.ch)
		if len(watchers) == 0 {
			delete(r.watchers, serviceName)
			delete(r.views, serviceName)
			return !r.closing
		}
		r.watchers[serviceName] = watchers
		return false
	}
	return false
}

// closeServiceWatchersLocked closes every channel while holding the same lock
// deliver uses for sending. Removing the entry first makes cleanup idempotent:
// a concurrent cancellation cannot find and close any of these watchers again.
func (r *Registry) closeServiceWatchersLocked(serviceName string) {
	watchers := r.watchers[serviceName]
	delete(r.watchers, serviceName)
	delete(r.views, serviceName)
	for _, w := range watchers {
		close(w.done)
		close(w.ch)
	}
}

func (r *Registry) logf(format string, args ...any) {
	if r.opts.Logger == nil {
		return
	}
	r.opts.Logger.Printf("nacos: "+format, args...)
}

func toInstances(hosts []naming.Instance) []registry.Instance {
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

func toWireInstance(in registry.Instance) naming.Instance {
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
	return naming.Instance{
		IP: in.Host, Port: in.Port, Weight: weight,
		Healthy: true, Enabled: true, Ephemeral: true,
		ClusterName: "DEFAULT", Metadata: meta,
	}
}
