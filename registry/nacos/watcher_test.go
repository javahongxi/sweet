package nacos

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	naming "github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

type fakeNamingClient struct {
	mu sync.Mutex

	hosts            []naming.Instance
	subscribeCalls   int
	unsubscribeCalls int
	closeCalls       int
	subscribed       map[string]bool
	updates          chan naming.ServiceInfo

	subscribeStarted chan struct{}
	subscribeRelease chan struct{}
	startOnce        sync.Once
}

func newFakeNamingClient(hosts ...naming.Instance) *fakeNamingClient {
	return &fakeNamingClient{
		hosts:      hosts,
		subscribed: map[string]bool{},
		updates:    make(chan naming.ServiceInfo, 16),
	}
}

func (c *fakeNamingClient) Register(context.Context, string, naming.Instance) error {
	return nil
}

func (c *fakeNamingClient) Deregister(context.Context, string, naming.Instance) error {
	return nil
}

func (c *fakeNamingClient) Subscribe(ctx context.Context, service string) (naming.ServiceInfo, error) {
	c.mu.Lock()
	c.subscribeCalls++
	call := c.subscribeCalls
	c.subscribed[service] = true
	started, release := c.subscribeStarted, c.subscribeRelease
	c.mu.Unlock()
	if started != nil {
		c.startOnce.Do(func() { close(started) })
	}
	if release != nil && call == 1 {
		select {
		case <-release:
		case <-ctx.Done():
			return naming.ServiceInfo{}, ctx.Err()
		}
	}
	c.mu.Lock()
	hosts := append([]naming.Instance(nil), c.hosts...)
	c.mu.Unlock()
	return naming.ServiceInfo{Name: service, GroupName: "DEFAULT_GROUP", Hosts: hosts}, nil
}

func (c *fakeNamingClient) Unsubscribe(_ context.Context, service string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unsubscribeCalls++
	delete(c.subscribed, service)
	return nil
}

func (c *fakeNamingClient) Query(_ context.Context, service string) (naming.ServiceInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return naming.ServiceInfo{Name: service, GroupName: "DEFAULT_GROUP", Hosts: c.hosts}, nil
}

func (c *fakeNamingClient) Updates() <-chan naming.ServiceInfo { return c.updates }

func (c *fakeNamingClient) Subscribed() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.subscribed))
	for service := range c.subscribed {
		out = append(out, service)
	}
	return out
}

func (c *fakeNamingClient) Close() error {
	c.mu.Lock()
	c.closeCalls++
	c.mu.Unlock()
	return nil
}

func (c *fakeNamingClient) counts() (subscribes, unsubscribes, closes int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscribeCalls, c.unsubscribeCalls, c.closeCalls
}

func newWatcherTestRegistry(t *testing.T, client *fakeNamingClient) *Registry {
	t.Helper()
	r := &Registry{
		client:        client,
		watchers:      map[string][]*watcher{},
		views:         map[string][]registry.Instance{},
		subscriptions: map[string]*serviceSubscription{},
		pumpDone:      make(chan struct{}),
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func watcherHost(port int) naming.Instance {
	return naming.Instance{IP: "127.0.0.1", Port: port, Weight: 100, Healthy: true, Enabled: true, Ephemeral: true}
}

func receiveInstances(t *testing.T, ch <-chan []registry.Instance) []registry.Instance {
	t.Helper()
	select {
	case insts, ok := <-ch:
		if !ok {
			t.Fatal("watcher closed before delivering its snapshot")
		}
		return insts
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watcher snapshot")
		return nil
	}
}

func requirePort(t *testing.T, insts []registry.Instance, port int) {
	t.Helper()
	if len(insts) != 1 || insts[0].Port != port {
		t.Fatalf("instances = %+v, want only port %d", insts, port)
	}
}

func requireWatcherClosed(t *testing.T, ch <-chan []registry.Instance) {
	t.Helper()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("watcher did not close")
		}
	}
}

func watcherCount(r *Registry, service string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.watchers[service])
}

func waitForWatchers(t *testing.T, r *Registry, service string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if watcherCount(r, service) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("watcher count = %d, want %d", watcherCount(r, service), want)
}

func TestWatchSeedsWatcherJoiningDuringInitialSubscribe(t *testing.T) {
	client := newFakeNamingClient(watcherHost(20001))
	client.subscribeStarted = make(chan struct{})
	client.subscribeRelease = make(chan struct{})
	r := newWatcherTestRegistry(t, client)
	ctx := context.Background()
	const service = "sweet.example.v1.WatchSeed"

	type watchResult struct {
		ch  <-chan []registry.Instance
		err error
	}
	firstResult := make(chan watchResult, 1)
	go func() {
		ch, err := r.Watch(ctx, service)
		firstResult <- watchResult{ch: ch, err: err}
	}()
	<-client.subscribeStarted

	// The first Subscribe is still blocked. A second watcher joins the same
	// shared subscription and must receive its response just like the first.
	secondResult := make(chan watchResult, 1)
	go func() {
		ch, err := r.Watch(ctx, service)
		secondResult <- watchResult{ch: ch, err: err}
	}()
	waitForWatchers(t, r, service, 2)
	close(client.subscribeRelease)
	first := <-firstResult
	if first.err != nil {
		t.Fatalf("first Watch: %v", first.err)
	}
	second := <-secondResult
	if second.err != nil {
		t.Fatalf("second Watch: %v", second.err)
	}
	requirePort(t, receiveInstances(t, first.ch), 20001)
	requirePort(t, receiveInstances(t, second.ch), 20001)
	if subscribes, _, _ := client.counts(); subscribes != 1 {
		t.Fatalf("Subscribe calls = %d, want one shared subscription", subscribes)
	}
}

func TestWatchRetriesInitialSubscribeWhenFirstContextIsCancelled(t *testing.T) {
	client := newFakeNamingClient(watcherHost(20001))
	client.subscribeStarted = make(chan struct{})
	client.subscribeRelease = make(chan struct{})
	r := newWatcherTestRegistry(t, client)
	const service = "sweet.example.v1.WatchRetry"
	firstCtx, cancelFirst := context.WithCancel(context.Background())

	type watchResult struct {
		ch  <-chan []registry.Instance
		err error
	}
	firstResult := make(chan watchResult, 1)
	go func() {
		ch, err := r.Watch(firstCtx, service)
		firstResult <- watchResult{ch: ch, err: err}
	}()
	<-client.subscribeStarted
	secondResult := make(chan watchResult, 1)
	go func() {
		ch, err := r.Watch(context.Background(), service)
		secondResult <- watchResult{ch: ch, err: err}
	}()
	waitForWatchers(t, r, service, 2)

	cancelFirst()
	first := <-firstResult
	if !errors.Is(first.err, context.Canceled) {
		t.Fatalf("first Watch error = %v, want context.Canceled", first.err)
	}
	second := <-secondResult
	if second.err != nil {
		t.Fatalf("second Watch after first cancellation: %v", second.err)
	}
	requirePort(t, receiveInstances(t, second.ch), 20001)
	if subscribes, _, _ := client.counts(); subscribes != 2 {
		t.Fatalf("Subscribe calls = %d, want the second watcher to retry", subscribes)
	}
	waitForWatchers(t, r, service, 1)
}

func TestWatchSeedsLaterWatcherWithLatestSnapshot(t *testing.T) {
	client := newFakeNamingClient(watcherHost(20001))
	r := newWatcherTestRegistry(t, client)
	ctx := context.Background()
	const service = "sweet.example.v1.WatchLatest"

	first, err := r.Watch(ctx, service)
	if err != nil {
		t.Fatalf("first Watch: %v", err)
	}
	requirePort(t, receiveInstances(t, first), 20001)
	second, err := r.Watch(ctx, service)
	if err != nil {
		t.Fatalf("second Watch: %v", err)
	}
	requirePort(t, receiveInstances(t, second), 20001)

	updated := []registry.Instance{{Host: "127.0.0.1", Port: 20002, Weight: 100}}
	r.deliver(service, updated, false)
	requirePort(t, receiveInstances(t, first), 20002)
	requirePort(t, receiveInstances(t, second), 20002)

	// A later watcher needs the newest pushed view, not the older subscribe
	// response and not an empty channel that waits for an unrelated third push.
	third, err := r.Watch(ctx, service)
	if err != nil {
		t.Fatalf("third Watch: %v", err)
	}
	requirePort(t, receiveInstances(t, third), 20002)
	if subscribes, _, _ := client.counts(); subscribes != 1 {
		t.Fatalf("Subscribe calls = %d, want one shared subscription", subscribes)
	}
}

func TestWatchContextCancellationCleansUpIndependently(t *testing.T) {
	client := newFakeNamingClient(watcherHost(20001))
	r := newWatcherTestRegistry(t, client)
	const service = "sweet.example.v1.WatchCancel"
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())

	first, err := r.Watch(firstCtx, service)
	if err != nil {
		t.Fatalf("first Watch: %v", err)
	}
	requirePort(t, receiveInstances(t, first), 20001)
	second, err := r.Watch(secondCtx, service)
	if err != nil {
		t.Fatalf("second Watch: %v", err)
	}
	requirePort(t, receiveInstances(t, second), 20001)

	cancelSecond()
	requireWatcherClosed(t, second)
	waitForWatchers(t, r, service, 1)
	if _, unsubscribes, _ := client.counts(); unsubscribes != 0 {
		t.Fatalf("Unsubscribe calls = %d while one watcher remains, want 0", unsubscribes)
	}

	// The surviving watcher still receives updates after its sibling leaves.
	r.deliver(service, []registry.Instance{{Host: "127.0.0.1", Port: 20002}}, false)
	requirePort(t, receiveInstances(t, first), 20002)

	cancelFirst()
	requireWatcherClosed(t, first)
	waitForWatchers(t, r, service, 0)
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, unsubscribes, _ := client.counts()
		if unsubscribes == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Unsubscribe calls = %d after the last cancellation, want 1", unsubscribes)
		}
		time.Sleep(time.Millisecond)
	}
	// Delivery after local cleanup is a no-op, not a send on a closed channel.
	r.deliver(service, []registry.Instance{{Host: "127.0.0.1", Port: 20003}}, false)
}

func TestWatchRejectsCancelledContext(t *testing.T) {
	client := newFakeNamingClient(watcherHost(20001))
	r := newWatcherTestRegistry(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Watch(ctx, "sweet.example.v1.Cancelled"); err == nil {
		t.Fatal("Watch succeeded with an already-cancelled context")
	}
	if got := watcherCount(r, "sweet.example.v1.Cancelled"); got != 0 {
		t.Fatalf("watcher count = %d, want 0", got)
	}
}

func TestDeliverCanRaceWithCancellation(t *testing.T) {
	client := newFakeNamingClient(watcherHost(20001))
	r := newWatcherTestRegistry(t, client)
	for i := 0; i < 100; i++ {
		service := fmt.Sprintf("sweet.example.v1.CancelRace.%d", i)
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := r.Watch(ctx, service)
		if err != nil {
			t.Fatalf("Watch %d: %v", i, err)
		}
		receiveInstances(t, ch)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			cancel()
		}()
		go func() {
			defer wg.Done()
			<-start
			r.deliver(service, []registry.Instance{{Host: "127.0.0.1", Port: 20002}}, false)
		}()
		close(start)
		wg.Wait()
		requireWatcherClosed(t, ch)
		waitForWatchers(t, r, service, 0)
	}
}

func TestDeliverCanRaceWithUnsubscribe(t *testing.T) {
	client := newFakeNamingClient(watcherHost(20001))
	r := newWatcherTestRegistry(t, client)
	for i := 0; i < 100; i++ {
		service := fmt.Sprintf("sweet.example.v1.UnsubscribeRace.%d", i)
		ch, err := r.Watch(context.Background(), service)
		if err != nil {
			t.Fatalf("Watch %d: %v", i, err)
		}
		receiveInstances(t, ch)

		start := make(chan struct{})
		errCh := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r.deliver(service, []registry.Instance{{Host: "127.0.0.1", Port: 20002}}, false)
		}()
		go func() {
			defer wg.Done()
			<-start
			if err := r.Unsubscribe(context.Background(), service); err != nil {
				errCh <- err
			}
		}()
		close(start)
		wg.Wait()
		select {
		case err := <-errCh:
			t.Fatalf("Unsubscribe %d: %v", i, err)
		default:
		}
		requireWatcherClosed(t, ch)
		if got := watcherCount(r, service); got != 0 {
			t.Fatalf("watcher count after Unsubscribe = %d, want 0", got)
		}
	}
}

func TestDeliverCanRaceWithCloseAndCloseIsIdempotent(t *testing.T) {
	for i := 0; i < 100; i++ {
		client := newFakeNamingClient(watcherHost(20001))
		r := newWatcherTestRegistry(t, client)
		service := fmt.Sprintf("sweet.example.v1.CloseRace.%d", i)
		ch, err := r.Watch(context.Background(), service)
		if err != nil {
			t.Fatalf("Watch %d: %v", i, err)
		}
		receiveInstances(t, ch)

		start := make(chan struct{})
		errCh := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r.deliver(service, []registry.Instance{{Host: "127.0.0.1", Port: 20002}}, false)
		}()
		go func() {
			defer wg.Done()
			<-start
			if err := r.Close(); err != nil {
				errCh <- err
			}
		}()
		close(start)
		wg.Wait()
		select {
		case err := <-errCh:
			t.Fatalf("Close %d: %v", i, err)
		default:
		}
		requireWatcherClosed(t, ch)
		if err := r.Close(); err != nil {
			t.Fatalf("second Close %d: %v", i, err)
		}
		if _, _, closes := client.counts(); closes != 1 {
			t.Fatalf("client Close calls = %d, want 1", closes)
		}
		if _, err := r.Watch(context.Background(), service); err == nil {
			t.Fatal("Watch succeeded after Close")
		}
	}
}
