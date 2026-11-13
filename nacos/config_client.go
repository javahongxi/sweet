package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
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
	// lifecycle owns the connection, the reconnect supervisor and the accessors;
	// embedding it keeps WaitReady / CurrentAddr / ConnectionID / Handshakes /
	// PushDropped / LastErr as part of this type's API.
	lifecycle

	opts   ConfigOptions
	tenant string
	labels map[string]string

	watchers map[configKey]*watcher
	onError  func(configKey, error)
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
	// replayed elsewhere (RpcClient.java:717-723). See serverList.
	Servers []string

	// Tenant is the namespace id, and it is the field config calls "tenant"
	// (naming calls the same thing "namespace"). An empty Tenant is normalised to
	// "public" by NewConfigClient, as the official client does.
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
	// Same normalisation as the naming domain: the official client turns an unset
	// namespace into "public" before it becomes the config request's tenant
	// (ClientBasicParamUtil.java:148-149), so the field goes on the wire as a value
	// instead of being dropped by `omitempty`. Unlike naming, a live 3.2.3 tolerates
	// the absent field — this is about matching every other client's shape.
	if opts.Tenant == "" {
		opts.Tenant = "public"
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
		opts:     opts,
		tenant:   opts.Tenant,
		labels:   labels,
		watchers: map[configKey]*watcher{},
		onError:  opts.OnError,
	}
	c.lifecycle = lifecycle{
		serversList: newServerList(addrs, 0),
		newConn:     c.dial,
		onReady:     func(conn *sdkConn, gen int64) { go c.consumePushes(conn, gen) },
		replay:      func() error { return c.listen(c.watchedKeys()) },
		logFn:       func(msg string, kv ...any) { c.logger().Log(c.ctx, slog.LevelDebug, "nacos: config "+msg, kv...) },
		tenantFn:    func() string { return c.tenant },
		labelsFn:    func() map[string]string { return c.labels },
	}
	c.lifecycle.setup(c.serversList, ctx, cancel, 0, opts.ReconnectBackoff)
	c.lifecycle.start()
	return c, nil
}

// dial performs the config-domain handshake on a fresh address: every knob comes
// from this domain's Options, which is why it is the hook the skeleton cannot
// own.
func (c *ConfigClient) dial(ctx context.Context, addr string) (*sdkConn, error) {
	conn, err := dialConn(addr)
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
	conn.SetStreamLostHook(func(error) { c.streamLost(conn) })

	hctx, cancel := context.WithTimeout(ctx, c.budget())
	defer cancel()
	if err := conn.Setup(hctx, c.tenant, c.labels); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (c *ConfigClient) logger() *slog.Logger {
	if c.opts.Logger == nil {
		return noopLogger
	}
	return c.opts.Logger
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
	conn, err := c.liveConn()
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

// PublishInput is one config write.
type PublishInput struct {
	// Content is the new value. It must not be blank: the server checks that
	// before it looks at anything else (ParamUtils.java:98-101 rejects a blank
	// content with errorCode 400), so "take this config away" is a Remove, never a
	// Publish of "". Checked here too, so the caller learns it without a round trip.
	Content string

	// Type is Nacos' own config type — json, yaml, properties, xml, text, html.
	// It travels in additionMap["type"] (ConfigPublishRequestHandler.java:86) and
	// only affects the console's rendering and validation; an unrecognised value is
	// quietly replaced by the server default (:109-111), so it is not verified here.
	Type string

	// AppName lands in additionMap["appName"]; like the server's own srcUser it is
	// bookkeeping for the config's history row. Optional.
	AppName string

	// ExpectMD5 turns the write into a compare-and-swap: pass the MD5 that came
	// back from Query and the server only applies the change while that is still
	// its current value, otherwise Publish returns an ErrConflict-wrapped error.
	// Empty means write-unconditional, which is the official client's default too
	// (ClientWorker.java:1438 passes whatever the caller had, usually null).
	//
	// One detail worth knowing: CAS does not require the config to exist. When it
	// does not, the server creates it and ignores ExpectMD5
	// (ConfigOperationService.java:1436-1439 → insertOrUpdateCas →
	// EmbeddedConfigInfoPersistServiceImpl.java:316-322), so a first publish cannot
	// fail with a conflict.
	ExpectMD5 string
}

// Publish writes one config.
//
// The write shares this client's connection — same module=config handshake, same
// per-request credential injection — and a publish for a key this client is
// watching arrives back as an ordinary server notify, so the Watch callback sees
// it like any other writer's change. Nothing is applied locally on the way out.
//
// Returning nil means the value is durable, not that Query can read it back yet.
// A publish commits to the database and then raises a ConfigDataChangeEvent that a
// separate task manager dumps into the in-memory cache
// (ConfigOperationService.java:172 → DumpService.java:133-156), while the read side
// answers from that cache and nowhere else — a cache miss is a not-found, with no
// database fallback (ConfigChainEntryHandler.java:52-76). Measured against a 3.2.3
// standalone on 9848: a new dataId took 58-118 ms to become readable, a delete took
// 85-110 ms to stop serving the old value, and a winning CAS still returned the old
// content and old md5 one millisecond after Publish said success.
//
// That is the server's contract, not a defect to hide here, and it rules out one
// tempting pattern: do not Query to confirm a Publish. Write conditionally with
// ExpectMD5, or Watch the key to learn when it moved.
func (c *ConfigClient) Publish(ctx context.Context, key ConfigKey, in PublishInput) error {
	if key.Tenant == "" {
		key.Tenant = c.tenant
	}
	if strings.TrimSpace(in.Content) == "" {
		return fmt.Errorf("nacos: publish %s/%s: content is required; Nacos rejects a blank one, use Remove to take a config away", key.Group, key.DataID)
	}
	conn, err := c.liveConn()
	if err != nil {
		return err
	}
	req := ConfigPublishRequest{
		DataID:  key.DataID,
		Group:   key.Group,
		Tenant:  key.Tenant,
		Content: in.Content,
		CasMD5:  in.ExpectMD5,
	}
	// Only send the keys that are set: the server reads them with
	// getAdditionParam, which treats a missing entry and an empty one the same
	// (ConfigPublishRequest.java:50-52), but an absent map is what the official
	// client sends when it has nothing to add.
	for k, v := range map[string]string{"type": in.Type, "appName": in.AppName} {
		if v != "" {
			if req.Addition == nil {
				req.Addition = map[string]string{}
			}
			req.Addition[k] = v
		}
	}
	var resp ConfigPublishResponse
	if err := conn.Call(ctx, req, &resp); err != nil {
		return err
	}
	return resp.checkWrite("publish", key)
}

// Remove deletes one config. It is idempotent in the server's own words: deleting
// a key that is not there is not an error, because the persist layer looks the row
// up first and does nothing when it is absent
// (EmbeddedConfigInfoPersistServiceImpl.java:494-519), and deleteConfig still
// reports success afterwards (ConfigOperationService.java:333). It does still fire
// a change notify, so watchers of a never-existing key get one pull that comes
// back not-found.
//
// The same visibility window as Publish applies in reverse: 85-110 ms measured here
// before a deleted dataId stopped answering, because the cache entry is dropped by
// the dump task rather than by the delete itself.
//
// ConfigRemoveRequest's tag field is deliberately not exposed — see the note on
// the wire type about it being passed as grayName.
func (c *ConfigClient) Remove(ctx context.Context, key ConfigKey) error {
	if key.Tenant == "" {
		key.Tenant = c.tenant
	}
	conn, err := c.liveConn()
	if err != nil {
		return err
	}
	req := ConfigRemoveRequest{DataID: key.DataID, Group: key.Group, Tenant: key.Tenant}
	var resp ConfigRemoveResponse
	if err := conn.Call(ctx, req, &resp); err != nil {
		return err
	}
	return resp.check("remove", key)
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
	conn, err := c.liveConn()
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

	conn, err := c.liveConn()
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

// consumePushes serves one connection until it is superseded or closed. Every
// pushed ServerRequest is acked — an un-acked push is re-sent until the server
// unregisters the connection.
func (c *ConfigClient) consumePushes(conn *sdkConn, gen int64) {
	defer func() {
		if v := recover(); v != nil {
			// A poison frame must not kill the only consumer of pushes; that would
			// look like "config stopped updating" on a live connection. Turning it
			// into a reconnect is the honest response.
			c.log("push consumer panicked", "err", fmt.Sprint(v))
			c.streamLost(conn)
		}
	}()
	for push := range conn.Pushes() {
		c.mu.Lock()
		current := c.generation == gen && c.conn == conn
		c.mu.Unlock()
		if !current {
			return
		}
		if push.Type == TypeConnectResetRequest {
			// Acked inside, on this stream, before the switch.
			handleConnectReset(c, push)
			continue
		}
		switch push.Type {
		case TypeConfigChangeNotifyRequest:
			var note ConfigChangeNotifyRequest
			if err := decodeBody(push, &note); err != nil {
				_ = conn.Ack(push) // still ack: a malformed push will not fix itself
				continue
			}
			if err := conn.Ack(push); err != nil {
				c.streamLost(conn)
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
	return c.closeConn()
}

// decodeBody reads a pushed frame's JSON body, which sdkConn already unwrapped
// from the Payload envelope.
func decodeBody(p Push, out any) error {
	return json.Unmarshal(p.Body, out)
}
