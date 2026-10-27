package nacos

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	naming "github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

func hostsOf(t *testing.T, addrs ...string) []naming.Instance {
	t.Helper()
	out := make([]naming.Instance, 0, len(addrs))
	for _, a := range addrs {
		host, port, ok := strings.Cut(a, ":")
		if !ok {
			t.Fatalf("test address %q is not host:port", a)
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			t.Fatalf("test address %q: %v", a, err)
		}
		out = append(out, naming.Instance{
			IP: host, Port: p, Weight: 100, Healthy: true, Enabled: true, Ephemeral: true,
		})
	}
	return out
}

// newSnapshotRegistry builds a registry that can never reach its server, so every
// test here is about the disk path and nothing else.
func newSnapshotRegistry(t *testing.T, dir string) *Registry {
	t.Helper()
	r, err := New(Options{
		ServerAddr:     "127.0.0.1:1",
		SnapshotDir:    dir,
		ReconnectDelay: time.Hour, // a fallback starts the reconnect loop; keep it inert here
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func TestSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	r := newSnapshotRegistry(t, dir)
	const service = "jaws/org.hongxi.jaws.sample.api.DemoService" // a name with "/" in it

	r.remember(service, hostsOf(t, "10.0.0.1:20881", "10.0.0.2:20882"))

	insts, age, ok := r.snapshot(service)
	if !ok {
		t.Fatal("snapshot missing after remember; a name containing / must still round-trip")
	}
	if len(insts) != 2 {
		t.Fatalf("snapshot has %d instances, want 2", len(insts))
	}
	if age < 0 || age > time.Minute {
		t.Fatalf("age = %v, want a fresh non-negative age", age)
	}
	if got := r.SnapshotAge(service); got < 0 {
		t.Fatalf("SnapshotAge = %v, want non-negative", got)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 || strings.Contains(entries[0].Name(), "/") {
		t.Fatalf("unexpected files %v; the sanitised name must not contain path separators", names(entries))
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// An empty answer must never become a snapshot: it would keep a restarted consumer
// from dialling anyone at all.
func TestRememberSkipsEmptyList(t *testing.T) {
	dir := t.TempDir()
	r := newSnapshotRegistry(t, dir)
	r.remember("empty.service", nil)
	if _, err := os.ReadDir(dir); err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if _, _, ok := r.snapshot("empty.service"); ok {
		t.Fatal("an empty list was persisted as a snapshot")
	}
}

// Files that disagree about which service they describe are refused: a collision
// in the sanitised name must not become someone else's instance list.
func TestSnapshotRejectsIdentityMismatch(t *testing.T) {
	dir := t.TempDir()
	r := newSnapshotRegistry(t, dir)
	const service = "sweet.example.v1.Greeter"
	path := r.snapshotPath(service)

	body, err := json.Marshal(snapshotRecord{
		SavedAt: time.Now(), Namespace: r.opts.Namespace, GroupName: r.opts.GroupName,
		Service: "some.other.Service", Hosts: hostsOf(t, "10.0.0.9:20000"),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, ok := r.snapshot(service); ok {
		t.Fatal("a snapshot describing a different service was accepted")
	}

	// An undated record is refused too: age is the whole point of the fallback.
	if err := os.WriteFile(path, []byte(`{"service":"`+service+`","groupName":"`+
		r.opts.GroupName+`","namespace":"`+r.opts.Namespace+`"}`), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, ok := r.snapshot(service); ok {
		t.Fatal("a snapshot without a timestamp was accepted")
	}
}

// The fallback itself: server unreachable, snapshot present → Watch still hands
// out the last known list, announces it, and keeps the intent live so a later
// reconnect re-subscribes and replaces it.
func TestWatchFallsBackToSnapshot(t *testing.T) {
	dir := t.TempDir()
	seeder := newSnapshotRegistry(t, dir)
	const service = "sweet.example.v1.Ping"
	seeder.remember(service, hostsOf(t, "127.0.0.1:20001"))

	var (
		hooked  []registry.Instance
		hookAge time.Duration
		hookRun bool
	)
	r, err := New(Options{
		ServerAddr:     "127.0.0.1:1",
		SnapshotDir:    dir,
		ReconnectDelay: time.Hour,
		OnSnapshotFallback: func(_ string, insts []registry.Instance, age time.Duration) {
			hooked, hookAge, hookRun = insts, age, true
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := r.Watch(ctx, service)
	if err != nil {
		t.Fatalf("Watch with a snapshot available: %v", err)
	}
	select {
	case got := <-ch:
		if len(got) != 1 || got[0].Host != "127.0.0.1" || got[0].Port != 20001 {
			t.Fatalf("fallback delivered %v, want the snapshot", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fallback delivered nothing")
	}
	if !hookRun {
		t.Fatal("OnSnapshotFallback never ran: staleness would be invisible to callers")
	}
	if hookAge < 0 || len(hooked) != 1 {
		t.Fatalf("hook saw age=%v instances=%v", hookAge, hooked)
	}
	r.mu.Lock()
	sub := r.subscriptions[service]
	r.mu.Unlock()
	if sub == nil || !sub.live {
		t.Fatal("the subscription intent was not kept live; a recovered connection would never re-subscribe")
	}
}

// The negative control the whole feature depends on: with the directory unset, the
// same unreachable server must fail the caller instead of quietly succeeding. If
// this test ever passes by becoming identical to the one above, the fallback is
// not doing anything.
func TestWatchWithoutSnapshotDirStillFails(t *testing.T) {
	dir := t.TempDir()
	seeder := newSnapshotRegistry(t, dir)
	const service = "sweet.example.v1.Ping"
	seeder.remember(service, hostsOf(t, "127.0.0.1:20001")) // snapshot exists on disk...

	r, err := New(Options{ServerAddr: "127.0.0.1:1", ReconnectDelay: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = r.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r.Watch(ctx, service); err == nil {
		t.Fatal("Watch succeeded with SnapshotDir unset; the switch is not what gates the fallback")
	} else if !strings.Contains(err.Error(), "subscribe") {
		t.Fatalf("error = %v, want the subscribe failure to surface", err)
	}
	// The file is still there — proving the difference is the option, not timing.
	if _, _, ok := seeder.snapshot(service); !ok {
		t.Fatal("the seeder's snapshot vanished; the negative control tested nothing")
	}
}

func TestSnapshotDisabledTouchesNoDisk(t *testing.T) {
	r, err := New(Options{ServerAddr: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = r.Close() }()

	if r.snapshotEnabled() {
		t.Fatal("snapshot is enabled without SnapshotDir")
	}
	r.remember("anything", hostsOf(t, "10.0.0.1:1"))
	if _, _, ok := r.snapshot("anything"); ok {
		t.Fatal("read a snapshot with the feature off")
	}
	if got := r.SnapshotAge("anything"); got != -1 {
		t.Fatalf("SnapshotAge = %v with the feature off, want -1 (no snapshot)", got)
	}
}
