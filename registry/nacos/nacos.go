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

// Registry is a Nacos-backed registry: an adapter over nacos.NamingClient.
type Registry struct {
	opts   Options
	client *naming.NamingClient

	mu       sync.Mutex
	watchers map[string][]chan []registry.Instance
	closing  bool
	pumpDone chan struct{}
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
	if opts.Namespace == "" {
		opts.Namespace = "public"
	}
	if opts.GroupName == "" {
		opts.GroupName = "DEFAULT_GROUP"
	}
	if opts.ReconnectDelay <= 0 {
		opts.ReconnectDelay = time.Second
	}

	r := &Registry{
		opts:     opts,
		watchers: map[string][]chan []registry.Instance{},
		pumpDone: make(chan struct{}),
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
	ch := make(chan []registry.Instance, 4)
	r.mu.Lock()
	first := len(r.watchers[serviceName]) == 0
	r.watchers[serviceName] = append(r.watchers[serviceName], ch)
	r.mu.Unlock()

	if !first {
		// Already subscribed server-side; this watcher just joins the fan-out.
		return ch, nil
	}

	info, err := r.client.Subscribe(ctx, serviceName)
	if err != nil {
		if r.snapshotEnabled() {
			if ferr := r.serveStaleOnFailure(ctx, serviceName, ch, err); ferr != nil {
				return nil, ferr
			}
			return ch, nil
		}
		r.dropWatcher(serviceName, ch)
		return nil, fmt.Errorf("nacos: subscribe %s: %w", serviceName, err)
	}

	// A subscribe response already carries the current list (§8.4), so no
	// separate query is needed to seed the view.
	select {
	case ch <- toInstances(info.Hosts):
	default:
	}
	return ch, nil
}

// Unsubscribe stops pushes for a service and forgets its watchers.
func (r *Registry) Unsubscribe(ctx context.Context, serviceName string) error {
	r.mu.Lock()
	chans := r.watchers[serviceName]
	delete(r.watchers, serviceName)
	r.mu.Unlock()
	for _, ch := range chans {
		close(ch)
	}
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
	r.closing = true
	for name, chans := range r.watchers {
		for _, ch := range chans {
			close(ch)
		}
		delete(r.watchers, name)
	}
	r.mu.Unlock()

	close(r.pumpDone)
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
