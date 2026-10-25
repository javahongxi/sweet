// Package nacos implements registry.Registry over Nacos' naming protocol with
// the client protocol written by hand (see nacos/conn) — no nacos-sdk-go.
//
// Scope is deliberately the ephemeral-instance path only: registration,
// deregistration, and subscription over the gRPC naming surface. Nacos 2.x/3.x
// carries all of that on gRPC (the HTTP surface remains only for persistent
// instances and the maintenance API), so this registry needs no HTTP at all.
package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	// Namespace is the tenant. Empty means "public", which Nacos' own client
	// normalises the same way.
	Namespace string
	// GroupName defaults to DEFAULT_GROUP.
	GroupName string
	// HealthyOnly filters queried instances.
	HealthyOnly bool
}

// Registry is a Nacos-backed registry.
type Registry struct {
	opts     Options
	serverIP string

	mu       sync.Mutex
	c        *conn.Conn
	subs     map[string]struct{} // "group@@service" -> subscribed
	watchers map[string][]chan []registry.Instance
}

// New validates opts. No connection is opened yet: the first call or watch
// establishes it, so constructing a registry cannot block.
func New(opts Options) (*Registry, error) {
	if opts.ServerAddr == "" {
		return nil, errors.New("nacos: Options.ServerAddr required")
	}
	host, _, err := splitHostPort(opts.ServerAddr)
	if err != nil {
		return nil, fmt.Errorf("nacos: ServerAddr: %w", err)
	}
	if opts.Namespace == "" {
		opts.Namespace = "public"
	}
	if opts.GroupName == "" {
		opts.GroupName = "DEFAULT_GROUP"
	}
	return &Registry{
		opts:     opts,
		serverIP: host,
		subs:     map[string]struct{}{},
		watchers: map[string][]chan []registry.Instance{},
	}, nil
}

// Name makes this registry answer the "nacos:///<service>" scheme.
func (r *Registry) Name() string { return "nacos" }

// Expose registers the instance and returns its deregistration.
func (r *Registry) Expose(ctx context.Context, serviceName string, in registry.Instance) (func(), error) {
	c, err := r.ensureConn(ctx)
	if err != nil {
		return nil, err
	}
	req := r.instanceRequest(serviceName, conn.TypeRegisterInstance, in)
	if err := r.call(c, req); err != nil {
		return nil, fmt.Errorf("nacos: register %s: %w", serviceName, err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if c, err := r.ensureConn(dctx); err == nil {
				_ = r.call(c, r.instanceRequest(serviceName, conn.TypeDeregisterInstance, in))
			}
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
	key := r.key(serviceName)
	r.mu.Lock()
	first := len(r.watchers[key]) == 0
	r.watchers[key] = append(r.watchers[key], ch)
	r.mu.Unlock()

	if first {
		resp, err := r.subscribe(ctx, c, serviceName, true)
		if err != nil {
			r.dropWatcher(key, ch)
			return nil, fmt.Errorf("nacos: subscribe %s: %w", serviceName, err)
		}
		// Nacos answers a subscribe with the current snapshot, so no separate
		// query is needed to seed the view.
		select {
		case ch <- toInstances(resp.ServiceInfo.Hosts):
		default:
		}
	}
	return ch, nil
}

// Close deregisters nothing; it only tears the connection down. Callers that
// need graceful offline must run the closures returned by Expose first.
func (r *Registry) Close() error {
	r.mu.Lock()
	c := r.c
	r.c = nil
	for _, chans := range r.watchers {
		for _, ch := range chans {
			close(ch)
		}
	}
	r.watchers = map[string][]chan []registry.Instance{}
	r.subs = map[string]struct{}{}
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
	fresh, err := conn.Dial(r.opts.ServerAddr)
	if err != nil {
		return nil, err
	}
	if err := fresh.Setup(ctx, r.opts.Namespace, nil); err != nil {
		_ = fresh.Close()
		return nil, fmt.Errorf("nacos: handshake with %s: %w", r.opts.ServerAddr, err)
	}
	r.mu.Lock()
	if r.c != nil && r.c.Ready() {
		old := r.c
		r.mu.Unlock()
		_ = old.Close()
		_ = fresh.Close()
		return old, nil
	}
	r.c = fresh
	r.mu.Unlock()
	go r.readPushes(fresh)
	return fresh, nil
}

// readPushes is the single consumer of server pushes: it decodes
// NotifySubscriberRequest and fans the instance list out to local watchers.
func (r *Registry) readPushes(c *conn.Conn) {
	for push := range c.Pushes() {
		if push.Type != conn.TypeNotifySubscriberRequest {
			continue
		}
		var note notifyPush
		if err := json.Unmarshal(push.Body, &note); err != nil {
			continue
		}
		name := note.serviceName()
		// Ack first: the server re-pushes until it gets the answer, and a
		// stalled re-push looks exactly like "subscription works, updates slow".
		if push.RequestID != "" {
			_ = c.Ack(conn.Push{Type: push.Type, RequestID: push.RequestID}, "ok")
		}
		r.deliver(name, toInstances(note.ServiceInfo.Hosts))
	}
}

// notifyPush mirrors NotifySubscriberRequest. Measured on a live 3.2.3 server,
// its JSON is {"headers":{},"requestId":"5","serviceInfo":{...}} — there is no
// top-level serviceName: the service identity lives inside serviceInfo, whose
// `name` is qualified ("GROUP@@service" whenever the group is not DEFAULT_GROUP).
// Keying a push off a field that is not on the wire is how the first version of
// this code silently dropped every notification.
type notifyPush struct {
	Headers     map[string]string `json:"headers,omitempty"`
	RequestID   string            `json:"requestId,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	ServiceInfo conn.ServiceInfo  `json:"serviceInfo"`
}

// serviceName recovers the plain service name from a qualified one.
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

func (r *Registry) deliver(serviceName string, insts []registry.Instance) {
	key := r.key(serviceName)
	r.mu.Lock()
	chans := append([]chan []registry.Instance(nil), r.watchers[key]...)
	r.mu.Unlock()
	for _, ch := range chans {
		select {
		case ch <- insts:
		default:
			// A slow consumer misses one update and is corrected by the next
			// push; blocking the dispatcher would stall every other service.
		}
	}
}

func (r *Registry) subscribe(ctx context.Context, c *conn.Conn, serviceName string, subscribe bool) (*conn.SubscribeServiceResponse, error) {
	req := conn.SubscribeServiceRequest{
		Namespace:   r.opts.Namespace,
		ServiceName: serviceName,
		GroupName:   r.opts.GroupName,
		Subscribe:   subscribe,
	}
	t, body, err := c.Unary(ctx, req)
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
	r.mu.Lock()
	if subscribe {
		r.subs[r.key(serviceName)] = struct{}{}
	} else {
		delete(r.subs, r.key(serviceName))
	}
	r.mu.Unlock()
	return &resp, nil
}

// Unsubscribe stops pushes for a service once no watcher remains.
func (r *Registry) Unsubscribe(ctx context.Context, serviceName string) error {
	r.mu.Lock()
	key := r.key(serviceName)
	n := len(r.watchers[key])
	if n == 0 {
		r.mu.Unlock()
		return nil
	}
	chans := r.watchers[key]
	delete(r.watchers, key)
	for _, ch := range chans {
		close(ch)
	}
	r.mu.Unlock()
	c, err := r.currentConn()
	if err != nil {
		return err
	}
	_, err = r.subscribe(ctx, c, serviceName, false)
	return err
}

func (r *Registry) currentConn() (*conn.Conn, error) {
	r.mu.Lock()
	c := r.c
	r.mu.Unlock()
	if c == nil || !c.Ready() {
		return nil, errors.New("nacos: not connected")
	}
	return c, nil
}

func (r *Registry) dropWatcher(key string, ch chan []registry.Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()
	kept := r.watchers[key][:0]
	for _, w := range r.watchers[key] {
		if w != ch {
			kept = append(kept, w)
		}
	}
	if len(kept) == 0 {
		delete(r.watchers, key)
		return
	}
	r.watchers[key] = kept
}

func (r *Registry) instanceRequest(serviceName, action string, in registry.Instance) conn.InstanceRequest {
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
	return conn.InstanceRequest{
		Namespace:   r.opts.Namespace,
		ServiceName: serviceName,
		GroupName:   r.opts.GroupName,
		Type:        action,
		Instance: conn.Instance{
			IP: in.Host, Port: in.Port, Weight: weight,
			Healthy: true, Enabled: true, Ephemeral: true,
			ClusterName: "DEFAULT", Metadata: meta,
		},
	}
}

func (r *Registry) call(c *conn.Conn, req conn.InstanceRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
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

// query is a plain ServiceQueryRequest; it exists so tests can assert what
// Nacos believes is registered without depending on the push path.
func (r *Registry) query(ctx context.Context, serviceName string) ([]registry.Instance, error) {
	c, err := r.currentConn()
	if err != nil {
		c, err = r.ensureConn(ctx)
		if err != nil {
			return nil, err
		}
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

func (r *Registry) key(serviceName string) string {
	return strings.TrimSpace(r.opts.GroupName) + "@@" + strings.TrimSpace(serviceName)
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

func splitHostPort(addr string) (string, string, error) {
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			return addr[:i], addr[i+1:], nil
		}
	}
	return "", "", errors.New("want host:port")
}
