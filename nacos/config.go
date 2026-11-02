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

// ConfigClient speaks Nacos' config domain over its own SDK connection.
//
// Own connection, not a share of the naming one, and that is the server's rule
// rather than a convenience: the connection is labelled by module
// (NamingGrpcClientProxy.java:116 sends "naming", ClientWorker.java:752-755
// sends "config"), RpcClientFactory.java:126 keeps one GrpcSdkClient per
// clientName, and ConnectionBasedClientManager.java:60-64 makes the naming
// module ignore any connection whose module label is not "naming". A single
// connection cannot serve both domains, so neither can this client.
//
// Within the config domain, however, query and listen do share this connection —
// the unary RPC and the bidi stream live on one grpc.ClientConn, which is what
// jaws/doc/nacos-config.md:31 means by "与 listen 复用同一条 SDK 面连接".
//
// The change protocol is pull-after-notify: the server pushes keys only
// (ConfigChangeNotifyRequest carries dataId/group/tenant), and the client must
// query for content (jaws/doc/nacos-config.md:79). After each pull the client
// re-registers its listen with the *new* md5, because the server stores the
// md5 per (groupKey, connectionId) when listening
// (ConfigChangeBatchListenRequestHandler.java:80).
type ConfigClient struct {
	servers *ServerList
	opts    ConfigOptions

	conn *Conn // current connection; nil while a reconnect is in flight
	addr string
	// generation counts connection swaps; a push consumer stops when the number it
	// was started with no longer matches.
	generation int64
	tenant     string
	labels     map[string]string

	ctx    context.Context
	cancel context.CancelFunc

	// lost receives one signal per dead stream; the supervisor turns it into a
	// re-handshake plus a replay. Non-blocking send, so the read loop never
	// waits on recovery.
	lost chan struct{}

	mu        sync.Mutex
	watchers  map[configKey]*watcher
	onError   func(configKey, error)
	lastErr   error
	reconnect time.Duration
	attempt   int

	// signal is closed and replaced on every connection state change, so WaitReady
	// can block without polling and without the polarity trap of a latch that has
	// to be "un-closed" when a connection drops.
	signal chan struct{}
}

type watcher struct {
	key configKey
	md5 string
	fn  func(Config)
}

// ConfigOptions configures a ConfigClient.
type ConfigOptions struct {
	// Addr is the Nacos gRPC SDK address — main port + 1000 (Constants.java:102,
	// GrpcSdkServer.java:49-51), i.e. 127.0.0.1:9848 for a server on 8848. The
	// offset is not applied here: pass the gRPC port, same convention as naming.
	// Ignored when Servers is set.
	Addr string

	// Servers is a cluster's worth of SDK addresses. When set it takes precedence
	// over Addr and enables failover: a node whose handshake fails goes on
	// cooldown and the next attempt uses another address.
	//
	// Failover is connection-level only, matching the official client — an
	// in-flight request still returns its error to the caller instead of being
	// replayed elsewhere (RpcClient.java:717-723). See ServerList.
	Servers []string

	// Tenant is the namespace id, and it is the field config calls "tenant"
	// (naming calls the same thing "namespace"). Empty means public.
	Tenant string

	// Labels are sent in ConnectionSetupRequest. Defaults to
	// {source: sdk, module: config, appName: sweet} — the module label is what
	// makes this a config-domain connection, so overriding it is not supported;
	// add keys, do not replace module.
	Labels map[string]string

	// OnError receives refresh failures that were *not* reported to a Watch
	// caller, e.g. a config deleted while being watched. Optional; nil drops
	// them. Silently keeping the last good value is the right default for a
	// route table, so the alternative to a callback is not "fail loudly" but
	// "fail invisibly" — this hook is how a caller keeps the former.
	OnError func(configKey, error)

	ServerCheckTimeout time.Duration
	SetupTimeout       time.Duration
	KeepAlive          time.Duration
	CallTimeout        time.Duration

	// Access supplies credentials for every request. Nil means the server does not
	// require auth. See AccessGetter for why this cannot be a one-time handshake
	// detail.
	Access AccessGetter

	// Logger receives connection lifecycle events; nil means silent.
	Logger *slog.Logger

	// OnPushDropped is called when a server push is discarded because the consumer
	// stalled. For a config watcher this is not a cosmetic event: a dropped notify
	// leaves the cached value stale until something else changes.
	OnPushDropped func(Push)

	// ReconnectBackoff is the *first* wait between re-handshake attempts; retries
	// then grow exponentially with jitter up to 30s.
	ReconnectBackoff time.Duration
}

// NewConfigClient dials and handshakes as a config-domain connection, then
// starts the push loop.
//
// An unreachable server is NOT an error here. The gateway's cold start is the
// whole point of having a route cache, and failing construction would turn
// "config centre down" into "edge refuses to boot" — the outcome the cache
// exists to prevent. registry/nacos made the same call for the same reason
// (see scheduleRecover in registry/nacos/snapshot.go: a cold start against an
// unreachable server must still heal). Errors from the first handshake and the
// first Watch are reported to the caller through those calls, and the
// supervisor keeps retrying in the background.
func NewConfigClient(opts ConfigOptions) (*ConfigClient, error) {
	addrs := opts.Servers
	if len(addrs) == 0 {
		if opts.Addr == "" {
			return nil, errors.New("nacos: ConfigOptions.Addr or Servers required")
		}
		addrs = []string{opts.Addr}
	}
	labels := map[string]string{"source": "sdk", "module": "config", "appName": "sweet"}
	for k, v := range opts.Labels {
		if k == "module" {
			return nil, errors.New("nacos: config connection must keep label module=config")
		}
		labels[k] = v
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &ConfigClient{
		servers:   NewServerList(addrs, 0),
		opts:      opts,
		tenant:    opts.Tenant,
		labels:    labels,
		ctx:       ctx,
		cancel:    cancel,
		lost:      make(chan struct{}, 1),
		watchers:  map[configKey]*watcher{},
		onError:   opts.OnError,
		reconnect: opts.ReconnectBackoff,
		signal:    make(chan struct{}),
	}
	if c.reconnect <= 0 {
		c.reconnect = time.Second
	}
	// One attempt inline so a WaitReady immediately after construction has a real
	// answer. Its failure is recorded, not returned — see the type comment.
	if err := c.reconnectOnce(ctx); err != nil {
		c.noteErr(err)
	}
	go c.supervise()
	return c, nil
}

// budget is the per-attempt handshake allowance.
func (c *ConfigClient) budget() time.Duration {
	if t := c.opts.SetupTimeout; t > 0 {
		return t * 4
	}
	return 12 * time.Second
}

// dialFor builds and handshakes a connection to one address. Every knob is applied
// here rather than at construction, because the address — and with it the Conn
// object — can change on any retry.
func (c *ConfigClient) dialFor(ctx context.Context, addr string) (*Conn, error) {
	conn, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	opts := c.opts
	conn.ProbeFatal = true
	conn.Access = opts.Access
	conn.Logger = opts.Logger
	conn.OnPushDropped = func(p Push) {
		c.log("push dropped; the value may stay stale until the next change", "type", p.Type)
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
	// Installed before Setup so even the first stream loss is caught.
	conn.SetStreamLostHook(func(error) { c.signalLost() })

	hctx, cancel := context.WithTimeout(ctx, c.budget())
	defer cancel()
	if err := conn.Setup(hctx, c.tenant, c.labels); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// reconnectOnce tries once: the same address while it is still considered healthy,
// otherwise the next one from the list.
func (c *ConfigClient) reconnectOnce(ctx context.Context) error {
	addr := c.servers.Pick()
	if addr == "" {
		return errors.New("nacos: no server address to try")
	}
	c.mu.Lock()
	conn, curAddr := c.conn, c.addr
	c.mu.Unlock()

	// A lost stream is not evidence the node is gone: retry it in place first,
	// which keeps the warm channel.
	if conn != nil && curAddr == addr {
		conn.CloseStream()
		hctx, cancel := context.WithTimeout(ctx, c.budget())
		err := conn.Setup(hctx, c.tenant, c.labels)
		cancel()
		if err == nil {
			c.publish(conn, addr)
			return nil
		}
		c.servers.MarkFailed(addr)
		_ = conn.Close()
	}

	next, err := c.dialFor(ctx, addr)
	if err != nil {
		c.servers.MarkFailed(addr)
		return err
	}
	c.publish(next, addr)
	return nil
}

// publish installs a live connection, retires the previous one and starts its push
// consumer. Two consumers would double-deliver every notify, and a route table
// applied twice is a duplicate-registration error — hence the generation tag.
func (c *ConfigClient) publish(conn *Conn, addr string) {
	c.mu.Lock()
	old := c.conn
	c.conn, c.addr = conn, addr
	c.generation++
	gen := c.generation
	c.lastErr = nil
	c.attempt = 0
	c.broadcastLocked()
	c.mu.Unlock()

	c.servers.MarkHealthy(addr)
	if old != nil && old != conn {
		_ = old.Close()
	}
	c.log("connected", "addr", addr, "connectionId", conn.ConnectionID())
	go c.consumePushes(conn, gen)
}

// WaitReady blocks until a handshaked connection exists, ctx ends, or Close is
// called. The error wraps ErrUnavailable and names the list plus the last failure,
// so a caller can log *why* it is degraded rather than "it didn't connect".
func (c *ConfigClient) WaitReady(ctx context.Context) error {
	for {
		c.mu.Lock()
		conn, sig, msg := c.conn, c.signal, c.lastErr
		c.mu.Unlock()
		if conn != nil && conn.Ready() {
			return nil
		}
		select {
		case <-sig:
		case <-ctx.Done():
			return fmt.Errorf("%w: none of %s reachable (last error: %v)", ErrUnavailable, c.addrs(), msg)
		case <-c.ctx.Done():
			return c.ctx.Err()
		}
	}
}

// PushDropped counts server pushes discarded because the consumer stalled, across
// whichever connection is current. Read through the lock on purpose: the field it
// reports lives on a Conn that the supervisor may replace at any moment.
func (c *ConfigClient) PushDropped() int64 {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return 0
	}
	return conn.PushDropped()
}

// CurrentAddr is the SDK address in use, "" while a reconnect is in flight.
func (c *ConfigClient) CurrentAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addr
}

// addrs renders the list and who is on cooldown, for error text and logs.
func (c *ConfigClient) addrs() string {
	all, bad := c.servers.Addrs()
	if len(bad) == 0 {
		return fmt.Sprint(all)
	}
	return fmt.Sprintf("%v (%v on cooldown)", all, bad)
}

// current returns the live connection or an ErrUnavailable that says so.
func (c *ConfigClient) current() (*Conn, error) {
	c.mu.Lock()
	conn, msg := c.conn, c.lastErr
	c.mu.Unlock()
	if conn == nil || !conn.Ready() {
		return nil, fmt.Errorf("%w: no connection to %s (last error: %v)", ErrUnavailable, c.addrs(), msg)
	}
	return conn, nil
}

// broadcastLocked wakes everyone waiting on a connection state change.
func (c *ConfigClient) broadcastLocked() {
	close(c.signal)
	c.signal = make(chan struct{})
}

// signalLost retires the current connection and nudges the supervisor. The address
// is not marked failed here: a stream the server ended is retried against the same
// node first, and only a failed handshake puts it on cooldown.
func (c *ConfigClient) signalLost() {
	c.mu.Lock()
	conn := c.conn
	c.conn, c.addr = nil, ""
	c.generation++
	c.broadcastLocked()
	c.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	select {
	case c.lost <- struct{}{}:
	default:
	}
}

// LastErr is the most recent handshake or refresh failure, for a caller that
// wants to report a degraded start without owning the retry loop.
func (c *ConfigClient) LastErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

func (c *ConfigClient) setupBudget() time.Duration {
	if t := c.conn.SetupTimeout; t > 0 {
		return t * 4 // allow for a couple of transport retries on a cold server
	}
	return DefaultSetupTimeout * 4
}

// ConnectionID is the id the server gave this config connection; empty while a
// reconnect is in flight. Exposed because a replay is only meaningful against a
// fresh connectionId — the server's listen table is keyed by it.
// ConnectionID is the id the server gave the current connection, "" while a
// reconnect is in flight. Exposed because a replay is only meaningful against a
// fresh id — the server's listen table is keyed by it.
func (c *ConfigClient) ConnectionID() string {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return ""
	}
	return conn.ConnectionID()
}

// Config is one config's content plus the md5 to echo back on the next listen.
type Config struct {
	ConfigKey
	Content      string
	MD5          string
	ContentType  string
	LastModified int64
}

// Query pulls one config. A missing config yields an ErrNotFound-wrapped error, not an
// empty Config: "absent" and "empty" are different facts and a caller that
// conflates them will publish an empty table over a working one.
func (c *ConfigClient) Query(ctx context.Context, key ConfigKey) (Config, error) {
	if key.Tenant == "" {
		key.Tenant = c.tenant
	}
	conn, err := c.current()
	if err != nil {
		return Config{}, err
	}
	req := ConfigQueryRequest{DataID: key.DataID, Group: key.Group, Tenant: key.Tenant}
	var resp ConfigQueryResponse
	if err := conn.Call(ctx, req, &resp); err != nil {
		return Config{}, err
	}
	if err := resp.check("query", key); err != nil {
		return Config{}, err
	}
	return Config{
		ConfigKey:    key,
		Content:      resp.Content,
		MD5:          resp.MD5,
		ContentType:  resp.ContentType,
		LastModified: resp.LastModified,
	}, nil
}

// Watch registers interest in one config. It first pulls the current value and
// calls fn with it, then keeps calling fn after every server push.
//
// The registration is kept whatever the seed pull does, because a config centre
// that is unreachable right now is the exact case the caller needs to survive:
// the supervisor's replay re-listens with an empty md5, the server answers
// "stale", and the pull that follows delivers the content. Returning early
// without registering would mean a cold start against a down server never
// recovers, only restarts do.
//
// So the returned error is informational: nil when the seed landed (or the
// config simply does not exist yet), and the pull failure otherwise — with the
// watcher still installed.
func (c *ConfigClient) Watch(ctx context.Context, key ConfigKey, fn func(Config)) error {
	if key.Tenant == "" {
		key.Tenant = c.tenant
	}
	if fn == nil {
		return errors.New("nacos: Watch needs a callback")
	}
	seed, seedErr := c.Query(ctx, key)
	notFound := errors.Is(seedErr, ErrNotFound)

	c.mu.Lock()
	w, existed := c.watchers[key]
	if !existed {
		w = &watcher{key: key, fn: fn}
		c.watchers[key] = w
	} else {
		w.fn = fn // re-watching replaces the callback, it does not stack another
	}
	if !notFound && seedErr == nil {
		w.md5 = seed.MD5
	}
	c.mu.Unlock()

	if notFound {
		// Nothing to report yet, but the listen is in: the first publish reaches
		// this watcher without anyone having to call Watch again.
		if err := c.listen([]configKey{key}); err != nil {
			return fmt.Errorf("nacos: watch %s/%s registered but the listen failed: %w", key.Group, key.DataID, err)
		}
		return nil
	}
	if seedErr != nil {
		// Registration kept, error surfaced: the supervisor's replay will pull the
		// content once the connection exists.
		c.noteErr(seedErr)
		return seedErr
	}
	if err := c.listen([]configKey{key}); err != nil {
		// The supervisor replays every registration once the stream is back, so
		// a failed listen here is a degraded-but-recoverable state, not a
		// reason to refuse the Watch.
		c.noteErr(err)
		return fmt.Errorf("nacos: watch %s/%s registered but the listen failed: %w", key.Group, key.DataID, err)
	}
	fn(seed)
	return nil
}

// nextBackoff grows exponentially from ReconnectBackoff, capped at 30s, with up
// to a quarter of the delay as jitter. A fixed interval makes a fleet retry in
// lockstep: after a config centre restarts, every client knocks at the same
// instant, which is how the recovery becomes the next outage.
func (c *ConfigClient) nextBackoff() time.Duration {
	c.mu.Lock()
	c.attempt++
	n := c.attempt
	c.mu.Unlock()

	d := c.reconnect << min(n-1, 5) // reconnect · 2^(n-1), capped at 32×
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	if d <= 0 {
		return c.reconnect
	}
	// jitter in [d, d+d/4)
	return d + time.Duration(rand.Int64N(int64(d/4)+1))
}

func (c *ConfigClient) noteErr(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	c.lastErr = err
	c.mu.Unlock()
}

// Unwatch drops interest in one config and tells the server so with listen=false
// (ConfigChangeBatchListenRequestHandler.java:91 removeListen).
func (c *ConfigClient) Unwatch(ctx context.Context, key ConfigKey) error {
	if key.Tenant == "" {
		key.Tenant = c.tenant
	}
	c.mu.Lock()
	w, ok := c.watchers[key]
	if ok {
		delete(c.watchers, key)
	}
	c.mu.Unlock()
	if !ok {
		return nil
	}
	conn, err := c.current()
	if err != nil {
		return nil // nothing registered on a connection that does not exist
	}
	req := ConfigBatchListenRequest{Listen: false, ConfigListenContexts: []ConfigListenContext{
		{Group: key.Group, DataID: key.DataID, Tenant: key.Tenant, MD5: w.md5},
	}}
	var resp ConfigChangeBatchListenResponse
	return conn.Call(ctx, req, &resp)
}

// listen sends a batch listen for the given keys with the md5s we currently
// hold, and refreshes any key the server reports as stale.
func (c *ConfigClient) listen(keys []configKey) error {
	if len(keys) == 0 {
		return nil
	}
	c.mu.Lock()
	contexts := make([]ConfigListenContext, 0, len(keys))
	for _, k := range keys {
		w := c.watchers[k]
		if w == nil {
			continue
		}
		contexts = append(contexts, ConfigListenContext{
			Group: k.Group, DataID: k.DataID, Tenant: k.Tenant, MD5: w.md5,
		})
	}
	c.mu.Unlock()
	if len(contexts) == 0 {
		return nil
	}

	conn, err := c.current()
	if err != nil {
		return err
	}
	req := ConfigBatchListenRequest{Listen: true, ConfigListenContexts: contexts}
	var resp ConfigChangeBatchListenResponse
	if err := conn.Call(c.ctx, req, &resp); err != nil {
		return err
	}
	if err := resp.check("listen", configKey{}); err != nil {
		return err
	}
	// The immediate answer is the stale subset: a change that landed between our
	// query and our listen would otherwise be missed until the next one.
	for _, changed := range resp.ChangedConfigs {
		go c.refresh(changed.key())
	}
	return nil
}

// watchedKeys snapshots the registration set in a stable order, so a replay
// sends one batch rather than one call per key.
func (c *ConfigClient) watchedKeys() []configKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]configKey, 0, len(c.watchers))
	for k := range c.watchers {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Group != keys[j].Group {
			return keys[i].Group < keys[j].Group
		}
		return keys[i].DataID < keys[j].DataID
	})
	return keys
}

// refresh pulls one key and, when its md5 actually moved, hands the new content
// to the watcher and re-registers the listen with the new md5.
//
// The md5 guard is not an optimisation: the server re-pushes a notify until it
// sees an ack, and a duplicate notify for a config we already applied must not
// call fn twice — a route table applied twice would be rejected as a duplicate
// registration by the gateway.
func (c *ConfigClient) refresh(key configKey) {
	cfg, err := c.Query(c.ctx, key)
	if err != nil {
		c.noteErr(err)
		if c.onError != nil {
			c.onError(key, err)
		}
		return
	}
	c.mu.Lock()
	w := c.watchers[key]
	if w == nil || w.md5 == cfg.MD5 {
		c.mu.Unlock()
		return
	}
	w.md5 = cfg.MD5
	fn := w.fn
	c.mu.Unlock()

	fn(cfg)
	// Re-register with the fresh md5 so the server's per-connection record stops
	// treating this client as stale (ConfigChangeBatchListenRequestHandler.java:80).
	if err := c.listen([]configKey{key}); err != nil && c.onError != nil {
		c.onError(key, err)
	}
}

// supervise turns "the connection is gone" into "retry this node, then rotate",
// until Close. Without it a dropped stream leaves the client holding a
// registration the server deleted along with the old connectionId.
func (c *ConfigClient) supervise() {
	defer func() {
		if v := recover(); v != nil {
			// A dead supervisor means no replay after the next disconnect, which is
			// worse than a restartable one.
			c.log("supervisor panicked, restarting it", "err", fmt.Sprint(v))
			go c.supervise()
		}
	}()
	for {
		if c.ctx.Err() != nil {
			return
		}
		if c.connected() {
			// Idle until the stream goes; the loss signal is the only work to do.
			select {
			case <-c.ctx.Done():
				return
			case <-c.lost:
				continue
			}
		}

		// Not connected. This branch also covers "the very first handshake failed",
		// where no stream ever existed to be lost — waiting on the loss signal there
		// would sleep forever and the client would never heal.
		select {
		case <-c.ctx.Done():
			return
		case <-time.After(c.nextBackoff()):
		}
		if err := c.reconnectOnce(c.ctx); err != nil {
			c.noteErr(err)
			c.log("handshake failed", "err", err)
			continue
		}
		// Replay with the md5s we actually hold: the server keyed the previous
		// connectionId's subscriptions, and those died with it.
		if err := c.listen(c.watchedKeys()); err != nil {
			c.log("replay failed", "err", err)
		}
	}
}

// connected reports a live, handshaked connection.
func (c *ConfigClient) connected() bool {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	return conn != nil && conn.Ready()
}

// log writes to Options.Logger if one was set.
func (c *ConfigClient) log(msg string, kv ...any) {
	if c.opts.Logger == nil {
		return
	}
	c.opts.Logger.Log(c.ctx, slog.LevelDebug, "nacos: config "+msg, kv...)
}

// consumePushes serves one connection until it is superseded or closed. Every
// pushed ServerRequest is acked — an un-acked push is re-sent until the server
// unregisters the connection.
func (c *ConfigClient) consumePushes(conn *Conn, gen int64) {
	defer func() {
		if v := recover(); v != nil {
			// A poison frame must not kill the only consumer of pushes; that would
			// look like "config stopped updating" on a live connection. Turning it
			// into a reconnect is the honest response.
			c.log("push consumer panicked", "err", fmt.Sprint(v))
			c.signalLost()
		}
	}()
	for push := range conn.Pushes() {
		c.mu.Lock()
		current := c.generation == gen && c.conn == conn
		c.mu.Unlock()
		if !current {
			return
		}
		switch push.Type {
		case TypeConfigChangeNotifyRequest:
			var note ConfigChangeNotifyRequest
			if err := decodeBody(push, &note); err != nil {
				_ = conn.Ack(push) // still ack: a malformed push will not fix itself
				continue
			}
			if err := conn.Ack(push); err != nil {
				c.signalLost()
				continue
			}
			c.refresh(ConfigKey{DataID: note.DataID, Group: note.Group, Tenant: note.Tenant})
		default:
			// ClientDetectionRequest and any other ServerRequest expects a Response
			// carrying its requestId back; naming pushes cannot arrive on this
			// connection.
			_ = conn.Ack(push)
		}
	}
}

// Close shuts the connection down. Watch callbacks stop with it, and it is safe
// to call on a client that never managed to connect — which is the common case
// for a gateway that booted against a dead config centre and gave up.
func (c *ConfigClient) Close() error {
	c.cancel()
	c.mu.Lock()
	conn := c.conn
	c.conn, c.addr = nil, ""
	c.mu.Unlock()
	if conn == nil {
		return nil
	}
	return conn.Close()
}

// decodeBody reads a pushed frame's JSON body, which Conn already unwrapped
// from the Payload envelope.
func decodeBody(p Push, out any) error {
	return json.Unmarshal(p.Body, out)
}
