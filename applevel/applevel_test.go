package applevel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

// fakeRegistry is an in-memory registry.Registry: Expose records every call,
// Watch hands out a channel the test feeds. No network, no Nacos.
type fakeRegistry struct {
	mu           sync.Mutex
	exposed      map[string][]registry.Instance
	deregistered []string
	watches      map[string]chan []registry.Instance
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		exposed: map[string][]registry.Instance{},
		watches: map[string]chan []registry.Instance{},
	}
}

func (f *fakeRegistry) Name() string { return "nacos" }

func (f *fakeRegistry) Expose(_ context.Context, name string, in registry.Instance) (func(), error) {
	f.mu.Lock()
	f.exposed[name] = append(f.exposed[name], in)
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		f.deregistered = append(f.deregistered, name)
		f.mu.Unlock()
	}, nil
}

func (f *fakeRegistry) Watch(_ context.Context, name string) (<-chan []registry.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch, ok := f.watches[name]
	if !ok {
		ch = make(chan []registry.Instance, 8)
		f.watches[name] = ch
	}
	return ch, nil
}

func (f *fakeRegistry) feed(t *testing.T, name string, insts []registry.Instance) {
	t.Helper()
	f.mu.Lock()
	ch, ok := f.watches[name]
	f.mu.Unlock()
	if !ok {
		t.Fatalf("no watch registered for %s", name)
	}
	ch <- insts
}

func (f *fakeRegistry) lastExposed(t *testing.T, name string) registry.Instance {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	list := f.exposed[name]
	if len(list) == 0 {
		t.Fatalf("nothing exposed under %s", name)
	}
	return list[len(list)-1]
}

// fakeConfig is a map-backed ConfigClient with the real not-found semantics.
type fakeConfig struct {
	mu sync.Mutex
	m  map[nacos.ConfigKey]string
}

func newFakeConfig() *fakeConfig {
	return &fakeConfig{m: map[nacos.ConfigKey]string{}}
}

func (f *fakeConfig) Query(_ context.Context, key nacos.ConfigKey) (nacos.Config, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.m[key]; ok {
		return nacos.Config{ConfigKey: key, Content: v}, nil
	}
	return nacos.Config{}, fmt.Errorf("%w: %s", nacos.ErrNotFound, key.DataID)
}

func (f *fakeConfig) Publish(_ context.Context, key nacos.ConfigKey, in nacos.PublishInput) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = in.Content
	return nil
}

func awaitInstances(t *testing.T, ch <-chan []registry.Instance) []registry.Instance {
	t.Helper()
	select {
	case insts := <-ch:
		return insts
	case <-time.After(5 * time.Second):
		t.Fatal("no instance snapshot within 5s")
		return nil
	}
}

func TestCalRevisionGoldenVector(t *testing.T) {
	// The expected hash is computed over the exact byte sequence jaws'
	// calRevision produces (values sorted by service key, each + "\n"):
	// independently verified with `printf ... | md5`.
	services := map[string]string{
		"g/b/1": "wire://127.0.0.1:20010/sweet.example.v1.Other?group=DEFAULT_GROUP&",
		"g/a/1": "wire://127.0.0.1:20010/sweet.example.v1.Greeter?group=DEFAULT_GROUP&",
	}
	if got := CalRevision(services); got != "fcedee441db08b2c01657e4770bc8658" {
		t.Fatalf("CalRevision = %s, want the jaws-compatible golden hash", got)
	}
}

func TestServiceURLRoundTrip(t *testing.T) {
	params := map[string]string{
		"group":       "DEFAULT_GROUP",
		"application": "sweet-app",
		"tricky":      "a&b=c%d e+f",
		"unicode":     "服务发现",
	}
	raw := buildServiceURL("wire", "127.0.0.1", 20010, "sweet.example.v1.Greeter", params)
	u, err := parseServiceURL(raw)
	if err != nil {
		t.Fatalf("parseServiceURL: %v", err)
	}
	if u.Protocol != "wire" || u.Host != "127.0.0.1" || u.Port != 20010 || u.Path != "sweet.example.v1.Greeter" {
		t.Fatalf("parsed identity wrong: %+v", u)
	}
	for k, want := range params {
		if u.Params[k] != want {
			t.Fatalf("param %s = %q, want %q (encoding must survive jaws' URLDecoder contract)", k, u.Params[k], want)
		}
	}
}

func TestRevisionCacheSelfCheckAndEviction(t *testing.T) {
	c := newRevisionCache(2)
	mk := func(path string) *MetadataInfo {
		info := &MetadataInfo{Application: "app", Services: map[string]string{"g/" + path + "/1": "wire://h:1/" + path}}
		info.Revision = CalRevision(info.Services)
		return info
	}
	a, b, cc := mk("a"), mk("b"), mk("c")
	c.put(a)
	c.put(b)
	if c.get(a.Revision) == nil {
		t.Fatal("valid entry must be served")
	}
	// Tamper: the content hash no longer matches, the entry must be evicted.
	a.Services["g/a/1"] = "wire://evil:1/a"
	if c.get(a.Revision) != nil {
		t.Fatal("tampered entry must not be served")
	}
	c.put(b)
	c.put(cc) // capacity 2: b stays (touched), the oldest other entry goes
	if c.get(cc.Revision) == nil || c.get(b.Revision) == nil {
		t.Fatal("LRU eviction dropped the wrong entry")
	}
}

func TestProviderAggregatesAndServesMetadata(t *testing.T) {
	reg, cfg := newFakeRegistry(), newFakeConfig()
	p, err := NewProvider(reg, cfg, ProviderOptions{Application: "sweet-app"})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	ctx := context.Background()
	inst := registry.Instance{Host: "127.0.0.1", Port: 20010, Weight: 100}

	unreg1, err := p.Expose(ctx, "sweet.example.v1.Greeter", inst)
	if err != nil {
		t.Fatalf("Expose greeter: %v", err)
	}
	first := reg.lastExposed(t, "sweet-app")
	rev1 := first.Metadata["revision"]
	if rev1 == "" || first.Metadata["protocol"] != "wire" || first.Metadata["path"] != "sweet-app" {
		t.Fatalf("application instance payload wrong: %+v", first.Metadata)
	}
	if got := cfg.m[mappingKey("sweet.example.v1.Greeter")]; got != "sweet-app" {
		t.Fatalf("mapping = %q, want sweet-app", got)
	}

	unreg2, err := p.Expose(ctx, "sweet.example.v1.Other", inst)
	if err != nil {
		t.Fatalf("Expose other: %v", err)
	}
	second := reg.lastExposed(t, "sweet-app")
	if second.Metadata["revision"] == rev1 {
		t.Fatal("revision must change when the service list changes")
	}
	if len(reg.exposed["sweet-app"]) != 2 {
		t.Fatalf("one application instance must be upserted, got %d registrations", len(reg.exposed["sweet-app"]))
	}

	// Current revision answers with both services; the hash survives JSON.
	resp, err := p.GetMetadataInfo(ctx, wrapperspb.String(second.Metadata["revision"]))
	if err != nil {
		t.Fatalf("GetMetadataInfo: %v", err)
	}
	var info MetadataInfo
	if err := json.Unmarshal([]byte(resp.GetValue()), &info); err != nil {
		t.Fatalf("response is not the MetadataInfo JSON: %v", err)
	}
	if len(info.Services) != 2 || CalRevision(info.Services) != info.Revision {
		t.Fatalf("metadata content wrong: %+v", info)
	}

	// The old revision stays pullable from history during a rolling update.
	old, err := p.GetMetadataInfo(ctx, wrapperspb.String(rev1))
	if err != nil || old.GetValue() == "" {
		t.Fatalf("historical revision must answer: %v %q", err, old.GetValue())
	}
	// An unknown revision answers the empty string, not an error.
	unknown, err := p.GetMetadataInfo(ctx, wrapperspb.String("no-such-revision"))
	if err != nil || unknown.GetValue() != "" {
		t.Fatalf("unknown revision must answer empty: %v %q", err, unknown.GetValue())
	}

	// Removing one service re-exposes with the recomputed revision...
	unreg2()
	third := reg.lastExposed(t, "sweet-app")
	if third.Metadata["revision"] != rev1 {
		t.Fatalf("revision must return to %s after removing the second service, got %s", rev1, third.Metadata["revision"])
	}
	// ...and removing the last one deregisters the application instance.
	unreg1()
	if len(reg.deregistered) != 1 || reg.deregistered[0] != "sweet-app" {
		t.Fatalf("last unexport must deregister the app instance: %v", reg.deregistered)
	}
}

func TestProviderNamespaceAndAddressGuard(t *testing.T) {
	reg, cfg := newFakeRegistry(), newFakeConfig()
	p, err := NewProvider(reg, cfg, ProviderOptions{Application: "sweet-app", Namespace: "jaws"})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	ctx := context.Background()
	if _, err := p.Expose(ctx, "svc.A", registry.Instance{Host: "127.0.0.1", Port: 1}); err != nil {
		t.Fatalf("Expose: %v", err)
	}
	in := reg.lastExposed(t, "jaws/sweet-app")
	if in.Metadata["path"] != "sweet-app" {
		t.Fatalf("path sentinel must stay the bare application name: %q", in.Metadata["path"])
	}
	if _, err := p.Expose(ctx, "svc.B", registry.Instance{Host: "127.0.0.1", Port: 2}); err == nil {
		t.Fatal("a second address must be rejected: one application instance per provider")
	}
}

func TestConsumerPassthroughWithoutMapping(t *testing.T) {
	reg, cfg := newFakeRegistry(), newFakeConfig()
	c := NewConsumer(reg, cfg, ConsumerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := c.Watch(ctx, "sweet.example.v1.Greeter")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	want := []registry.Instance{{Host: "10.0.0.1", Port: 1, Weight: 100}}
	reg.feed(t, "sweet.example.v1.Greeter", want)
	got := awaitInstances(t, ch)
	if len(got) != 1 || got[0].Addr() != want[0].Addr() {
		t.Fatalf("passthrough must deliver the interface-level list verbatim: %+v", got)
	}
}

// startMetadataServer brings up a real gRPC server carrying only the metadata
// pull channel, on a real loopback port — the envelope is exercised end to end.
func startMetadataServer(t *testing.T, p *Provider) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	srv.RegisterService(p.MetadataServiceDesc(), p)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().(*net.TCPAddr).Port
}

func TestConsumerAppLevelEndToEnd(t *testing.T) {
	reg, cfg := newFakeRegistry(), newFakeConfig()
	p, err := NewProvider(reg, cfg, ProviderOptions{Application: "sweet-app"})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	port := startMetadataServer(t, p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inst := registry.Instance{Host: "127.0.0.1", Port: port, Weight: 100}
	if _, err := p.Expose(ctx, "sweet.example.v1.Greeter", inst); err != nil {
		t.Fatalf("Expose: %v", err)
	}

	c := NewConsumer(reg, cfg, ConsumerOptions{RetryInterval: time.Hour})
	ch, err := c.Watch(ctx, "sweet.example.v1.Greeter")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// The application snapshot the provider registered drives the consumer.
	reg.feed(t, "sweet-app", []registry.Instance{reg.lastExposed(t, "sweet-app")})
	got := awaitInstances(t, ch)
	if len(got) != 1 || got[0].Port != port {
		t.Fatalf("expanded instance wrong: %+v", got)
	}
	if got[0].Metadata["group"] != "DEFAULT_GROUP" || got[0].Metadata["application"] != "sweet-app" {
		t.Fatalf("URL parameters must travel as instance metadata: %+v", got[0].Metadata)
	}

	// An empty application snapshot empties the interface list.
	reg.feed(t, "sweet-app", nil)
	if empty := awaitInstances(t, ch); len(empty) != 0 {
		t.Fatalf("empty snapshot must expand to an empty list: %+v", empty)
	}

	// A revision change (weight tweaked) is pulled again and reflected.
	inst.Weight = 50
	if _, err := p.Expose(ctx, "sweet.example.v1.Greeter", inst); err != nil {
		t.Fatalf("re-Expose: %v", err)
	}
	reg.feed(t, "sweet-app", []registry.Instance{reg.lastExposed(t, "sweet-app")})
	updated := awaitInstances(t, ch)
	if len(updated) != 1 || updated[0].Weight != 50 {
		t.Fatalf("updated revision must refresh the instance: %+v", updated)
	}
}

func TestConsumerCacheHitSkipsSecondPull(t *testing.T) {
	reg, cfg := newFakeRegistry(), newFakeConfig()
	cfg.m[mappingKey("svc.A")] = "app"
	c := NewConsumer(reg, cfg, ConsumerOptions{RetryInterval: time.Hour})

	info := &MetadataInfo{Application: "app", Services: map[string]string{
		"DEFAULT_GROUP/svc.A/1.0": buildServiceURL("wire", "127.0.0.1", 7, "svc.A", nil),
	}}
	info.Revision = CalRevision(info.Services)
	var pulls int
	c.fetch = func(_ context.Context, revision string, _ []registry.Instance) (*MetadataInfo, error) {
		pulls++
		if revision != info.Revision {
			return nil, fmt.Errorf("unexpected revision %s", revision)
		}
		return info, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := c.Watch(ctx, "svc.A")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	appInst := registry.Instance{Host: "127.0.0.1", Port: 7, Metadata: map[string]string{"revision": info.Revision}}
	reg.feed(t, "app", []registry.Instance{appInst})
	awaitInstances(t, ch)
	// Same revision again: the cache must answer without a second pull.
	reg.feed(t, "app", []registry.Instance{appInst})
	awaitInstances(t, ch)
	if pulls != 1 {
		t.Fatalf("pulls = %d, want 1 (shared revision cache)", pulls)
	}
}

func TestConsumerMismatchVoidsAndRetries(t *testing.T) {
	reg, cfg := newFakeRegistry(), newFakeConfig()
	cfg.m[mappingKey("svc.A")] = "app"
	c := NewConsumer(reg, cfg, ConsumerOptions{RetryInterval: 20 * time.Millisecond})

	info := &MetadataInfo{Application: "app", Services: map[string]string{
		"DEFAULT_GROUP/svc.A/1.0": buildServiceURL("wire", "127.0.0.1", 7, "svc.A", nil),
	}}
	info.Revision = CalRevision(info.Services)
	var tampered sync.Mutex
	bad := true
	c.fetch = func(_ context.Context, _ string, _ []registry.Instance) (*MetadataInfo, error) {
		tampered.Lock()
		defer tampered.Unlock()
		if bad {
			// Content no longer hashes to the claimed revision.
			return &MetadataInfo{Application: "app", Revision: info.Revision,
				Services: map[string]string{"DEFAULT_GROUP/svc.A/1.0": "wire://evil:1/svc.A"}}, nil
		}
		return info, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := c.Watch(ctx, "svc.A")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	reg.feed(t, "app", []registry.Instance{{Host: "127.0.0.1", Port: 7,
		Metadata: map[string]string{"revision": info.Revision}}})

	select {
	case got := <-ch:
		t.Fatalf("tampered metadata must void the notification, got %+v", got)
	case <-time.After(100 * time.Millisecond):
	}

	tampered.Lock()
	bad = false
	tampered.Unlock()
	got := awaitInstances(t, ch) // the retry task must heal without a new push
	if len(got) != 1 || got[0].Port != 7 {
		t.Fatalf("retry must deliver the healed list: %+v", got)
	}
}

func TestConsumerUnknownRevisionIsRetryable(t *testing.T) {
	reg, cfg := newFakeRegistry(), newFakeConfig()
	p, err := NewProvider(reg, cfg, ProviderOptions{Application: "sweet-app"})
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	port := startMetadataServer(t, p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := p.Expose(ctx, "svc.A", registry.Instance{Host: "127.0.0.1", Port: port}); err != nil {
		t.Fatalf("Expose: %v", err)
	}

	c := NewConsumer(reg, cfg, ConsumerOptions{RetryInterval: time.Hour})
	ch, err := c.Watch(ctx, "svc.A")
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	// A revision this provider never published: the empty-string answer must
	// void the notification, not expand to a bogus list.
	reg.feed(t, "sweet-app", []registry.Instance{{Host: "127.0.0.1", Port: port,
		Metadata: map[string]string{"revision": "deadbeef"}}})
	select {
	case got := <-ch:
		t.Fatalf("unknown revision must not produce addresses: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestConsumerFetchSendsCoordinateHeaders(t *testing.T) {
	// A jaws provider dispatches MetadataService by group/version attachments;
	// verify the pull carries the instance's coordinates, not the defaults.
	var gotGroup, gotVersion string
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: MetadataServiceName,
		HandlerType: (*MetadataServiceServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: MetadataMethodName,
			Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				in := new(wrapperspb.StringValue)
				if err := dec(in); err != nil {
					return nil, err
				}
				md, _ := metadata.FromIncomingContext(ctx)
				if vs := md.Get("group"); len(vs) > 0 {
					gotGroup = vs[0]
				}
				if vs := md.Get("version"); len(vs) > 0 {
					gotVersion = vs[0]
				}
				return &wrapperspb.StringValue{}, nil
			},
		}},
	}, struct{ MetadataServiceServer }{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	c := NewConsumer(newFakeRegistry(), newFakeConfig(), ConsumerOptions{})
	_, err = c.fetchFrom(context.Background(), "rev", registry.Instance{
		Host: "127.0.0.1", Port: lis.Addr().(*net.TCPAddr).Port,
		Metadata: map[string]string{"group": "custom", "version": "2.0"},
	})
	// The empty answer is an error by design; the headers are the assertion.
	if err == nil {
		t.Fatal("empty envelope must surface as an error")
	}
	if gotGroup != "custom" || gotVersion != "2.0" {
		t.Fatalf("pull headers = %s/%s, want custom/2.0", gotGroup, gotVersion)
	}
}

func TestProviderRejectsMissingOptions(t *testing.T) {
	if _, err := NewProvider(nil, newFakeConfig(), ProviderOptions{Application: "a"}); err == nil {
		t.Fatal("nil registry must be rejected")
	}
	if _, err := NewProvider(newFakeRegistry(), nil, ProviderOptions{Application: "a"}); err == nil {
		t.Fatal("nil config must be rejected")
	}
	if _, err := NewProvider(newFakeRegistry(), newFakeConfig(), ProviderOptions{}); err == nil {
		t.Fatal("missing application must be rejected")
	}
	c := NewConsumer(newFakeRegistry(), newFakeConfig(), ConsumerOptions{})
	if _, err := c.Expose(context.Background(), "svc", registry.Instance{}); err == nil {
		t.Fatal("Consumer.Expose must be watch-only")
	}
	if !errors.Is(fmt.Errorf("%w: x", nacos.ErrNotFound), nacos.ErrNotFound) {
		t.Fatal("sanity: ErrNotFound wrapping")
	}
}
