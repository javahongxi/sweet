package applevel

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

// ProviderOptions configures a Provider.
type ProviderOptions struct {
	// Application is the instance identity peers subscribe to. Required.
	Application string
	// Namespace prefixes the registered application name, picking the side of
	// the interop naming convention: "" (default) registers the bare name the
	// way sweet always has, "jaws" matches a jaws consumer's default
	// registryNamespace. The URL "path" sentinel stays the bare application
	// either way — the prefix is a registry-name concern, not a URL concern.
	Namespace string
	// Group is the URL group parameter written into every service URL and the
	// application instance; a jaws consumer reference must set the same group.
	// Defaults to default_rpc, shared with jaws — the frameworks stay apart by
	// name prefix (jaws/{name} vs bare), not by group.
	Group string
	// Version is the URL version parameter; defaults to jaws' "1.0".
	Version string
	// Logger receives registration events; nil means silent.
	Logger *log.Logger
}

// Provider is the server-side registry decorator of application-level
// discovery. Expose does not register the service name peers used to look up:
// it aggregates the service URL into the application's MetadataInfo, registers
// this process's ONE application instance carrying the revision pointer (peers
// running the same application register their own, one per host:port), and publishes the
// interface → application mapping to the config center. The metadata itself
// never touches the registry — peers pull it over gRPC from this process.
//
// All services of one provider must share one address (one server, one port),
// the same constraint jaws' single-protocol instance mode enforces per process.
type Provider struct {
	inner   registry.Registry
	cfg     ConfigClient
	opts    ProviderOptions
	appName string // the name registered with the inner registry

	mu        sync.Mutex
	services  map[string]string // serviceKey → full URL string
	current   *MetadataInfo
	history   *revisionCache
	address   string // host:port of the application instance, first Expose wins
	timestamp string // startup millis, stamped once like jaws' warm-up timestamp
	unreg     func()
}

var _ registry.Registry = (*Provider)(nil)

// NewProvider validates opts and wraps inner. cfg carries the mapping writes;
// pass the same *nacos.ConfigClient the rest of the process uses.
func NewProvider(inner registry.Registry, cfg ConfigClient, opts ProviderOptions) (*Provider, error) {
	if inner == nil {
		return nil, errors.New("applevel: Provider requires a registry")
	}
	if cfg == nil {
		return nil, errors.New("applevel: Provider requires a config client")
	}
	if opts.Application == "" {
		return nil, errors.New("applevel: ProviderOptions.Application required")
	}
	if opts.Group == "" {
		opts.Group = "default_rpc"
	}
	if opts.Version == "" {
		opts.Version = DefaultURLVersion
	}
	appName := opts.Application
	if opts.Namespace != "" {
		appName = opts.Namespace + "/" + opts.Application
	}
	return &Provider{
		inner:    inner,
		cfg:      cfg,
		opts:     opts,
		appName:  appName,
		services: make(map[string]string),
		history:  newRevisionCache(cacheCapacity),
	}, nil
}

// Name answers the inner registry's scheme so client.Dial targets stay valid.
func (p *Provider) Name() string { return p.inner.Name() }

// Watch passes through: a provider may consume peers too, and discovery mode
// is the consumer's choice, not the registry's.
func (p *Provider) Watch(ctx context.Context, serviceName string) (<-chan []registry.Instance, error) {
	return p.inner.Watch(ctx, serviceName)
}

// Expose aggregates one service into the application instance. Repeated calls
// for the same service name overwrite its URL (idempotent, like the inner
// registry's contract); the returned closure removes the service again,
// re-registering the instance with the recomputed revision while other
// services remain, or deregistering it when the last one leaves. The mapping
// is left behind on purpose — the same boundary jaws and Dubbo keep.
func (p *Provider) Expose(ctx context.Context, serviceName string, in registry.Instance) (func(), error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.address != "" && p.address != in.Addr() {
		return nil, fmt.Errorf("applevel: application %s already exposed at %s; one application instance address per provider",
			p.opts.Application, p.address)
	}
	if p.timestamp == "" {
		p.timestamp = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}

	params := maps.Clone(in.Metadata)
	if params == nil {
		params = make(map[string]string)
	}
	// Identity keys are the framework's; user metadata cannot shadow them.
	params["application"] = p.opts.Application
	params["group"] = p.opts.Group
	params["version"] = p.opts.Version
	params["endpointType"] = "service"
	params["timestamp"] = p.timestamp
	if in.Weight > 0 {
		params["weight"] = strconv.Itoa(in.Weight)
	}
	key := serviceKey(p.opts.Group, serviceName, p.opts.Version)
	url := buildServiceURL("wire", in.Host, in.Port, serviceName, params)

	// Publish before committing state: a failed mapping write must not leave
	// the service table describing an instance that was never registered.
	if err := p.cfg.Publish(ctx, mappingKey(serviceName), nacos.PublishInput{
		Content: p.opts.Application,
		Type:    "text",
	}); err != nil {
		return nil, fmt.Errorf("applevel: publish mapping for %s: %w", serviceName, err)
	}
	p.services[key] = url
	info := p.rebuild()
	unreg, err := p.inner.Expose(ctx, p.appName, p.appInstance(in, info.Revision))
	if err != nil {
		return nil, fmt.Errorf("applevel: expose application %s: %w", p.appName, err)
	}
	p.address = in.Addr()
	// Nacos keys instances by ip:port, so the re-Expose above updated the one
	// instance in place; the previous closure would deregister the NEW one and
	// is dropped without running. Only the latest closure is ever called.
	p.unreg = unreg
	p.logf("exposed application %s revision=%s services=%d", p.appName, info.Revision, len(p.services))

	var once sync.Once
	return func() {
		once.Do(func() { p.remove(ctx, key, in) })
	}, nil
}

// remove is the unexport counterpart: drop one service, then land the
// recomputed revision or take the instance down entirely.
func (p *Provider) remove(ctx context.Context, key string, in registry.Instance) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.services, key)
	if len(p.services) == 0 {
		if p.unreg != nil {
			p.unreg()
			p.unreg = nil
		}
		p.current = nil
		p.address = ""
		p.logf("application %s deregistered, last service removed", p.appName)
		return
	}
	info := p.rebuild()
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := p.inner.Expose(dctx, p.appName, p.appInstance(in, info.Revision)); err != nil {
		p.logf("update application %s to revision %s: %v", p.appName, info.Revision, err)
		return
	}
	p.logf("application %s re-exposed revision=%s services=%d", p.appName, info.Revision, len(p.services))
}

// RemoveMapping deletes the interface → application mapping from the config
// center, restoring peers' interface-level fallback. The unexpose closure
// never calls this — the mapping outlives unexpose on purpose (the jaws/Dubbo
// boundary); callers that own the lifecycle (sample providers on shutdown)
// remove it explicitly, while the config client is still alive. A stale
// mapping silently reroutes every later interface-level lookup of this
// service name to the dead application, on both sides of the interop: sweet's
// own consumers and jaws references read the same key.
func (p *Provider) RemoveMapping(ctx context.Context, serviceName string) error {
	return p.cfg.Remove(ctx, mappingKey(serviceName))
}

// rebuild snapshots the service table into a fresh revision and archives the
// previous one, so a consumer holding an older revision pointer can still pull
// it during a rolling update. Caller holds p.mu.
func (p *Provider) rebuild() *MetadataInfo {
	info := &MetadataInfo{
		Application: p.opts.Application,
		Services:    maps.Clone(p.services),
	}
	info.Revision = CalRevision(info.Services)
	if p.current != nil && p.current.Revision != info.Revision {
		p.history.put(p.current)
	}
	p.current = info
	return info
}

// appInstance is the lightweight registry payload: address plus the sentinel
// keys jaws' NacosRegistry reads back (protocol, path) and the revision
// pointer. No service metadata travels here — that is the whole point.
func (p *Provider) appInstance(in registry.Instance, revision string) registry.Instance {
	return registry.Instance{
		Host:   in.Host,
		Port:   in.Port,
		Weight: in.Weight,
		Metadata: map[string]string{
			"protocol":     "wire",
			"path":         p.opts.Application,
			"application":  p.opts.Application,
			"group":        p.opts.Group,
			"version":      p.opts.Version,
			"endpointType": "service",
			"revision":     revision,
			"timestamp":    p.timestamp,
		},
	}
}

func (p *Provider) logf(format string, args ...any) {
	if p.opts.Logger != nil {
		p.opts.Logger.Printf(format, args...)
	}
}

// MetadataServiceServer is the handler contract of the internal metadata pull
// channel — the StringValue envelope jaws-wire speaks.
type MetadataServiceServer interface {
	GetMetadataInfo(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
}

// GetMetadataInfo answers a revision lookup purely from memory: the current
// revision or an archived one, JSON in a StringValue; the empty string means
// this process has never published that revision.
func (p *Provider) GetMetadataInfo(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	revision := req.GetValue()
	p.mu.Lock()
	defer p.mu.Unlock()
	var info *MetadataInfo
	if p.current != nil && p.current.Revision == revision {
		info = p.current
	} else {
		info = p.history.get(revision)
	}
	if info == nil {
		return &wrapperspb.StringValue{}, nil
	}
	payload, err := jsonMetadata(info)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return wrapperspb.String(payload), nil
}

// MetadataServiceDesc is the gRPC service descriptor to register beside the
// business descriptors on the SAME server, so peers pull metadata from the
// instance address they discovered — no second port, no registry entry:
//
//	srv.Register(provider.MetadataServiceDesc(), provider)
//
// Both the camelCase name jaws sends and the PascalCase alias are served.
func (p *Provider) MetadataServiceDesc() *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: MetadataServiceName,
		HandlerType: (*MetadataServiceServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: MetadataMethodName, Handler: metadataMethodHandler},
			{MethodName: "GetMetadataInfo", Handler: metadataMethodHandler},
		},
		Metadata: MetadataServiceName + ".proto",
	}
}

func metadataMethodHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(wrapperspb.StringValue)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(MetadataServiceServer).GetMetadataInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + MetadataServiceName + "/" + MetadataMethodName,
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(MetadataServiceServer).GetMetadataInfo(ctx, req.(*wrapperspb.StringValue))
	}
	return interceptor(ctx, in, info, handler)
}
