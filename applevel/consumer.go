package applevel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"sort"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	// Namespace prefixes the application name subscribed from the inner
	// registry: "" (default) watches the bare name sweet providers register,
	// "jaws" matches jaws' default registryNamespace. The mapping lookup and
	// the service-path match always use the bare interface name passed to
	// Watch, never the prefixed one.
	Namespace string
	// Logger receives discovery events; nil means silent.
	Logger *log.Logger
	// FetchTimeout bounds one point-to-point metadata pull; default 5s.
	FetchTimeout time.Duration
	// RetryInterval is the gap between retries after a revision failed to
	// resolve; default 10s, mirroring jaws' AddressRefreshRetryTask.
	RetryInterval time.Duration
}

// Consumer is the client-side registry decorator of application-level
// discovery. Watch checks the config-center mapping once — the same read
// jaws' ConsumerCoordinator does at refer time — and either passes through to
// interface-level discovery (no mapping) or subscribes to the application,
// resolves each revision through the shared cache or a point-to-point pull,
// and expands the metadata back into interface-level instances. Everything
// above it — resolver, client.Dial, the generated stub — stays untouched.
type Consumer struct {
	inner registry.Registry
	cfg   ConfigClient
	opts  ConsumerOptions
	cache *revisionCache

	// fetch pulls one revision from candidate instances, tried in the order
	// given. A field so offline tests inject metadata without a transport.
	fetch func(ctx context.Context, revision string, insts []registry.Instance) (*MetadataInfo, error)
}

var _ registry.Registry = (*Consumer)(nil)

// NewConsumer wraps inner. cfg reads the mapping; pass the same
// *nacos.ConfigClient the rest of the process uses.
func NewConsumer(inner registry.Registry, cfg ConfigClient, opts ConsumerOptions) *Consumer {
	if opts.FetchTimeout <= 0 {
		opts.FetchTimeout = 5 * time.Second
	}
	if opts.RetryInterval <= 0 {
		opts.RetryInterval = 10 * time.Second
	}
	c := &Consumer{
		inner: inner,
		cfg:   cfg,
		opts:  opts,
		cache: newRevisionCache(cacheCapacity),
	}
	c.fetch = c.fetchMetadata
	return c
}

// Name answers the inner registry's scheme so client.Dial targets stay valid.
func (c *Consumer) Name() string { return c.inner.Name() }

// Expose is watch-only; registration belongs to Provider.
func (c *Consumer) Expose(context.Context, string, registry.Instance) (func(), error) {
	return nil, errors.New("applevel: Consumer is watch-only; use Provider to expose")
}

// Watch resolves serviceName through the mapping when one exists. The mapping
// is read once per Watch, not watched: jaws consumers make the same choice at
// refer time, and a reference established one way keeps that way.
func (c *Consumer) Watch(ctx context.Context, serviceName string) (<-chan []registry.Instance, error) {
	cfg, err := c.cfg.Query(ctx, mappingKey(serviceName))
	if err != nil && !errors.Is(err, nacos.ErrNotFound) {
		return nil, fmt.Errorf("applevel: query mapping for %s: %w", serviceName, err)
	}
	application := ""
	if err == nil {
		application = cfg.Content
	}
	if application == "" {
		// No mapping: the classic interface-level path, verbatim.
		return c.inner.Watch(ctx, serviceName)
	}

	appName := application
	if c.opts.Namespace != "" {
		appName = c.opts.Namespace + "/" + application
	}
	src, err := c.inner.Watch(ctx, appName)
	if err != nil {
		return nil, fmt.Errorf("applevel: watch application %s: %w", appName, err)
	}
	c.logf("mapping %s -> application %s, watching %s", serviceName, application, appName)

	out := make(chan []registry.Instance, 1)
	w := &consumerWatch{c: c, name: serviceName, out: out}
	go w.run(ctx, src)
	return out, nil
}

// consumerWatch drives one subscription: expand every snapshot, keep the last
// good list when a revision cannot be resolved, retry until it can.
type consumerWatch struct {
	c    *Consumer
	name string
	out  chan []registry.Instance
}

func (w *consumerWatch) run(ctx context.Context, src <-chan []registry.Instance) {
	defer close(w.out)
	retryCh := make(chan struct{}, 1)
	var retry *time.Timer
	stopRetry := func() {
		if retry != nil {
			retry.Stop()
			retry = nil
		}
	}
	defer stopRetry()
	scheduleRetry := func() {
		stopRetry()
		retry = time.AfterFunc(w.c.opts.RetryInterval, func() {
			select {
			case retryCh <- struct{}{}:
			default:
			}
		})
	}

	var latest []registry.Instance
	for {
		select {
		case <-ctx.Done():
			return
		case insts, ok := <-src:
			if !ok {
				return
			}
			latest = insts
			stopRetry()
			if !w.resolve(ctx, latest) {
				scheduleRetry()
			}
		case <-retryCh:
			if !w.resolve(ctx, latest) {
				scheduleRetry()
			}
		}
	}
}

// resolve expands one snapshot and emits it; false means a revision failed
// and the notification is voided — the previous list stays with the resolver.
func (w *consumerWatch) resolve(ctx context.Context, insts []registry.Instance) bool {
	expanded, err := w.c.expand(ctx, w.name, insts)
	if err != nil {
		w.c.logf("metadata fetch failed for %s, keep current addresses and retry in %s: %v",
			w.name, w.c.opts.RetryInterval, err)
		return false
	}
	// Latest-wins into a depth-1 buffer: a slow resolver must never stall the
	// pump, and an obsolete snapshot is never worth delivering.
	select {
	case w.out <- expanded:
	default:
		select {
		case <-w.out:
		default:
		}
		select {
		case w.out <- expanded:
		default:
		}
	}
	return true
}

// expand groups instances by revision, resolves each group (cache first, then
// a point-to-point pull against a randomly ordered candidate), and collects
// the service URLs whose path is the interface this Watch asked for.
func (c *Consumer) expand(ctx context.Context, name string, insts []registry.Instance) ([]registry.Instance, error) {
	if len(insts) == 0 {
		return []registry.Instance{}, nil
	}
	byRevision := make(map[string][]registry.Instance)
	for _, in := range insts {
		revision := in.Metadata["revision"]
		if revision == "" {
			c.logf("application instance without revision parameter, skipped: %s", in.Addr())
			continue
		}
		byRevision[revision] = append(byRevision[revision], in)
	}
	revisions := make([]string, 0, len(byRevision))
	for revision := range byRevision {
		revisions = append(revisions, revision)
	}
	sort.Strings(revisions)

	out := make([]registry.Instance, 0, len(insts))
	seen := make(map[string]bool)
	for _, revision := range revisions {
		info := c.cache.get(revision)
		if info == nil {
			candidates := append([]registry.Instance(nil), byRevision[revision]...)
			// Random order avoids hotspotting one provider, like jaws.
			rand.Shuffle(len(candidates), func(i, j int) {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			})
			var err error
			info, err = c.fetch(ctx, revision, candidates)
			if err != nil {
				return nil, fmt.Errorf("revision %s: %w", revision, err)
			}
			if info == nil || info.Revision != revision || CalRevision(info.Services) != revision {
				return nil, fmt.Errorf("revision %s: metadata missing or content hash mismatch", revision)
			}
			c.cache.put(info)
			c.logf("fetched metadata revision=%s services=%d", revision, len(info.Services))
		}
		for _, raw := range info.Services {
			u, err := parseServiceURL(raw)
			if err != nil {
				c.logf("skip malformed service URL in revision %s: %v", revision, err)
				continue
			}
			if u.Path != name {
				continue
			}
			// During a rolling update two revisions may both describe this
			// interface; one address is enough.
			if seen[u.Host+":"+strconv.Itoa(u.Port)] {
				continue
			}
			seen[u.Host+":"+strconv.Itoa(u.Port)] = true
			weight := 100
			if w, err := strconv.Atoi(u.Params["weight"]); err == nil && w > 0 {
				weight = w
			}
			out = append(out, registry.Instance{
				Host: u.Host, Port: u.Port, Weight: weight, Metadata: u.Params,
			})
		}
	}
	return out, nil
}

// fetchMetadata tries the candidates in order until one answers with the
// revision; each attempt is a throw-away connection, like jaws' throw-away
// MetadataService reference.
func (c *Consumer) fetchMetadata(ctx context.Context, revision string, insts []registry.Instance) (*MetadataInfo, error) {
	var lastErr error
	for _, in := range insts {
		info, err := c.fetchFrom(ctx, revision, in)
		if err == nil {
			return info, nil
		}
		lastErr = err
		c.logf("metadata pull from %s failed: %v", in.Addr(), err)
	}
	if lastErr == nil {
		lastErr = errors.New("no candidate instance")
	}
	return nil, lastErr
}

// fetchFrom performs one StringValue-envelope pull. The group/version headers
// are copied from the instance metadata because jaws dispatches the internal
// MetadataService by the serviceKey they build — a custom-coordinate export
// is unreachable with default headers.
func (c *Consumer) fetchFrom(ctx context.Context, revision string, in registry.Instance) (*MetadataInfo, error) {
	conn, err := grpc.NewClient("passthrough:///"+in.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	fctx, cancel := context.WithTimeout(ctx, c.opts.FetchTimeout)
	defer cancel()
	group, version := in.Metadata["group"], in.Metadata["version"]
	if group == "" {
		group = DefaultURLGroup
	}
	if version == "" {
		version = DefaultURLVersion
	}
	fctx = metadata.AppendToOutgoingContext(fctx, "group", group, "version", version)

	out := new(wrapperspb.StringValue)
	path := "/" + MetadataServiceName + "/" + MetadataMethodName
	if err := conn.Invoke(fctx, path, wrapperspb.String(revision), out); err != nil {
		return nil, err
	}
	if out.GetValue() == "" {
		return nil, fmt.Errorf("revision %s not published by %s", revision, in.Addr())
	}
	var info MetadataInfo
	if err := json.Unmarshal([]byte(out.GetValue()), &info); err != nil {
		return nil, fmt.Errorf("decode metadata from %s: %w", in.Addr(), err)
	}
	return &info, nil
}

func (c *Consumer) logf(format string, args ...any) {
	if c.opts.Logger != nil {
		c.opts.Logger.Printf(format, args...)
	}
}
