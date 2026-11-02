package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	conn   *Conn
	tenant string
	labels map[string]string

	ctx    context.Context
	cancel context.CancelFunc

	// lost receives one signal per dead stream; the supervisor turns it into a
	// re-handshake plus a replay. Non-blocking send, so the read loop never
	// waits on recovery.
	lost chan struct{}

	mu        sync.Mutex
	watchers  map[configKey]*watcher
	onError   func(configKey, error)
	reconnect time.Duration
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
	Addr string

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

	// ReconnectBackoff is the wait between re-handshake attempts.
	ReconnectBackoff time.Duration
}

// NewConfigClient dials, handshakes as a config-domain connection and starts
// the push loop. Close it when done.
func NewConfigClient(opts ConfigOptions) (*ConfigClient, error) {
	if opts.Addr == "" {
		return nil, errors.New("nacos: ConfigOptions.Addr required")
	}
	conn, err := Dial(opts.Addr)
	if err != nil {
		return nil, err
	}
	conn.ProbeFatal = true
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

	labels := map[string]string{"source": "sdk", "module": "config", "appName": "sweet"}
	for k, v := range opts.Labels {
		if k == "module" {
			return nil, errors.New("nacos: config connection must keep label module=config")
		}
		labels[k] = v
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &ConfigClient{
		conn:      conn,
		tenant:    opts.Tenant,
		labels:    labels,
		ctx:       ctx,
		cancel:    cancel,
		lost:      make(chan struct{}, 1),
		watchers:  map[configKey]*watcher{},
		reconnect: opts.ReconnectBackoff,
	}
	if c.reconnect <= 0 {
		c.reconnect = time.Second
	}
	conn.SetStreamLostHook(func(error) {
		select {
		case c.lost <- struct{}{}:
		default:
		}
	})
	setupCtx, cancelSetup := context.WithTimeout(ctx, c.setupBudget())
	err = conn.Setup(setupCtx, opts.Tenant, labels)
	cancelSetup()
	if err != nil {
		conn.Close()
		cancel()
		return nil, err
	}
	go c.servePushes()
	go c.supervise(opts.OnError)
	return c, nil
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
func (c *ConfigClient) ConnectionID() string { return c.conn.ConnectionID() }

// Config is one config's content plus the md5 to echo back on the next listen.
type Config struct {
	ConfigKey
	Content      string
	MD5          string
	ContentType  string
	LastModified int64
}

// Query pulls one config. A missing config yields ErrConfigNotFound, not an
// empty Config: "absent" and "empty" are different facts and a caller that
// conflates them will publish an empty table over a working one.
func (c *ConfigClient) Query(ctx context.Context, key ConfigKey) (Config, error) {
	if key.Tenant == "" {
		key.Tenant = c.tenant
	}
	req := ConfigQueryRequest{DataID: key.DataID, Group: key.Group, Tenant: key.Tenant}
	var resp ConfigQueryResponse
	if err := c.conn.Call(ctx, req, &resp); err != nil {
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
// If the config does not exist yet, fn is not called — there is nothing to
// report — but the listen is still registered, so the first publish reaches
// this watcher. A pull that fails for any other reason is returned and no
// listen is registered, because silently watching a config we could not read
// would look like "the config is broken" later.
func (c *ConfigClient) Watch(ctx context.Context, key ConfigKey, fn func(Config)) error {
	if key.Tenant == "" {
		key.Tenant = c.tenant
	}
	if fn == nil {
		return errors.New("nacos: Watch needs a callback")
	}
	seed, err := c.Query(ctx, key)
	notFound := errors.As(err, &ErrConfigNotFound{})
	if err != nil && !notFound {
		return err
	}

	c.mu.Lock()
	w, existed := c.watchers[key]
	if !existed {
		w = &watcher{key: key, fn: fn}
		c.watchers[key] = w
	} else {
		w.fn = fn // re-watching replaces the callback, it does not stack another
	}
	if !notFound {
		w.md5 = seed.MD5
	}
	c.mu.Unlock()

	if err := c.listen([]configKey{key}); err != nil {
		// The supervisor replays every registration once the stream is back, so
		// a failed listen here is a degraded-but-recoverable state, not a
		// reason to refuse the Watch.
		return fmt.Errorf("nacos: watch %s/%s registered but the listen failed: %w", key.Group, key.DataID, err)
	}
	if !notFound {
		fn(seed)
	}
	return nil
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
	req := ConfigBatchListenRequest{Listen: false, ConfigListenContexts: []ConfigListenContext{
		{Group: key.Group, DataID: key.DataID, Tenant: key.Tenant, MD5: w.md5},
	}}
	var resp ConfigChangeBatchListenResponse
	return c.conn.Call(ctx, req, &resp)
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

	req := ConfigBatchListenRequest{Listen: true, ConfigListenContexts: contexts}
	var resp ConfigChangeBatchListenResponse
	if err := c.conn.Call(c.ctx, req, &resp); err != nil {
		return err
	}
	if err := resp.check("listen", configKey{}); err != nil {
		return err
	}
	// The immediate answer is the stale subset: a change that landed between our
	// query and our listen would otherwise be missed until the next one.
	for _, changed := range resp.ChangedConfigs {
		go c.refresh(changed.key(), nil)
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
func (c *ConfigClient) refresh(key configKey, onError func(configKey, error)) {
	cfg, err := c.Query(c.ctx, key)
	if err != nil {
		if onError != nil {
			onError(key, err)
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
	if err := c.listen([]configKey{key}); err != nil && onError != nil {
		onError(key, err)
	}
}

// servePushes consumes the stream's pushes for the lifetime of the connection.
// Every pushed ServerRequest is acked — an un-acked push is re-sent forever and
// eventually gets the connection unregistered.
func (c *ConfigClient) servePushes() {
	defer func() {
		if v := recover(); v != nil && c.conn != nil {
			// A poison frame must not kill the only consumer of pushes; that
			// would look like "config stopped updating" with a live connection.
			c.conn.failStream(fmt.Errorf("nacos: config push loop panicked: %v", v))
		}
	}()
	for push := range c.conn.Pushes() {
		switch push.Type {
		case TypeConfigChangeNotifyRequest:
			var note ConfigChangeNotifyRequest
			if err := decodeBody(push, &note); err != nil {
				_ = c.conn.Ack(push) // still ack: a malformed push will not fix itself
				continue
			}
			if err := c.conn.Ack(push); err != nil {
				continue // stream is going away; supervisor handles it
			}
			key := ConfigKey{DataID: note.DataID, Group: note.Group, Tenant: note.Tenant}
			c.refreshWithErrors(key)
		default:
			_ = c.conn.Ack(push) // naming pushes cannot arrive on this connection;
			// anything else (ClientDetectionRequest, …) still needs the 200 echo.
		}
	}
}

func (c *ConfigClient) refreshWithErrors(key configKey) {
	c.mu.Lock()
	onError := c.onError
	c.mu.Unlock()
	c.refresh(key, onError)
}

// supervise turns "the stream died" into "re-handshake and replay". Without the
// replay a reconnect would leave the client holding a registration the server
// dropped with the old connectionId — exactly the silent-death failure mode the
// naming side already guards against.
func (c *ConfigClient) supervise(onError func(configKey, error)) {
	c.mu.Lock()
	c.onError = onError
	c.mu.Unlock()
	defer func() {
		if v := recover(); v != nil {
			// Same reasoning as the push loop: a dead supervisor means no replay
			// after the next disconnect, which is worse than a loud crash.
			go c.supervise(onError)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.lost:
		}
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(c.reconnect):
			}
			c.conn.CloseStream() // resets the setup latch; Setup would otherwise
			// return against the *previous* handshake
			ctx, cancel := context.WithTimeout(c.ctx, c.setupBudget())
			err := c.conn.Setup(ctx, c.tenant, c.labels)
			cancel()
			if err == nil {
				break
			}
		}
		_ = c.listen(c.watchedKeys())
	}
}

// Close shuts the connection down. Watch callbacks stop with it.
func (c *ConfigClient) Close() error {
	c.cancel()
	return c.conn.Close()
}

// decodeBody reads a pushed frame's JSON body, which Conn already unwrapped
// from the Payload envelope.
func decodeBody(p Push, out any) error {
	return json.Unmarshal(p.Body, out)
}
