package nacos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	naming "github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
)

// snapshotRecord is one service list as written to disk. The identity fields are
// stored alongside the hosts and checked on read: a sanitised file name can
// collide with a different service, and serving someone else's instances is worse
// than serving none.
type snapshotRecord struct {
	SavedAt   time.Time         `json:"savedAt"`
	Namespace string            `json:"namespace"`
	GroupName string            `json:"groupName"`
	Service   string            `json:"service"`
	Hosts     []naming.Instance `json:"hosts"`
}

// snapshotEnabled is the single switch: no directory, no disk I/O at all. The
// default is off on purpose — a list whose age nobody asked for is a liability,
// which is also why the Java client ships nacos.naming.init.cache.at.start
// defaulting to false.
func (r *Registry) snapshotEnabled() bool { return r.opts.SnapshotDir != "" }

func (r *Registry) snapshotPath(service string) string {
	// Nacos service names are not file names: they may contain "/" (a jaws-style
	// qualified name does), and the group is separated by "@@". Replace what a
	// path cannot hold, keep the human-readable shape, and store the true names
	// inside the file.
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "*", "_",
		"?", "_", `"`, "_", "<", "_", ">", "_", "|", "_")
	return filepath.Join(r.opts.SnapshotDir,
		replacer.Replace(r.opts.GroupName+"@@"+service)+".json")
}

// remember persists a list the server actually gave us. Empty lists are not
// written: a transient or buggy empty answer must not become a snapshot that
// keeps a consumer from dialling anyone after a restart.
func (r *Registry) remember(service string, hosts []naming.Instance) {
	if !r.snapshotEnabled() || len(hosts) == 0 {
		return
	}
	rec := snapshotRecord{
		SavedAt: time.Now(), Namespace: r.opts.Namespace,
		GroupName: r.opts.GroupName, Service: service, Hosts: hosts,
	}
	body, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		r.logf("snapshot %s: encode: %v", service, err)
		return
	}
	final := r.snapshotPath(service)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		r.logf("snapshot %s: mkdir: %v", service, err)
		return
	}
	// Write-then-rename: a reader must never see a half-written file, and a torn
	// snapshot is worse than no snapshot because it still parses as a real list.
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		r.logf("snapshot %s: write: %v", service, err)
		return
	}
	if err := os.Rename(tmp, final); err != nil {
		r.logf("snapshot %s: rename: %v", service, err)
		return
	}
}

// snapshot returns the last list written for a service and how old it is.
func (r *Registry) snapshot(service string) ([]registry.Instance, time.Duration, bool) {
	if !r.snapshotEnabled() {
		return nil, 0, false
	}
	body, err := os.ReadFile(r.snapshotPath(service))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			r.logf("snapshot %s: read: %v", service, err)
		}
		return nil, 0, false
	}
	var rec snapshotRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		r.logf("snapshot %s: undecodable, ignoring: %v", service, err)
		return nil, 0, false
	}
	if rec.Service != service || rec.GroupName != r.opts.GroupName ||
		rec.Namespace != r.opts.Namespace {
		r.logf("snapshot %s: file holds namespace=%q group=%q service=%q, refusing to use it",
			service, rec.Namespace, rec.GroupName, rec.Service)
		return nil, 0, false
	}
	if rec.SavedAt.IsZero() {
		r.logf("snapshot %s: no timestamp, refusing to present an undated list", service)
		return nil, 0, false
	}
	age := time.Since(rec.SavedAt)
	if age < 0 {
		// Clock moved backwards; the file is still the best thing we have, but
		// reporting a negative age would hide that from any reader of the log.
		age = 0
	}
	insts := toInstances(rec.Hosts)
	if r.opts.HealthyOnly {
		kept := insts[:0:0]
		for _, in := range insts {
			if h := hostOf(in, rec.Hosts); h.Healthy {
				kept = append(kept, in)
			}
		}
		insts = kept
	}
	if len(insts) == 0 {
		return nil, age, false
	}
	return insts, age, true
}

// SnapshotAge exposes how stale the on-disk list for a service is. It exists so a
// caller can decide what to do with a fallback instead of discovering it in a
// log line; a negative duration reports "no snapshot".
func (r *Registry) SnapshotAge(service string) time.Duration {
	_, age, ok := r.snapshot(service)
	if !ok {
		return -1
	}
	return age
}

func hostOf(in registry.Instance, hosts []naming.Instance) naming.Instance {
	for _, h := range hosts {
		if h.IP == in.Host && h.Port == in.Port {
			return h
		}
	}
	return naming.Instance{}
}

// serveStaleOnFailure is what a failed subscribe or a failed cold dial turns into
// when a snapshot exists: the watcher is seeded with the old list, the
// subscription intent is kept so the reconnect loop re-subscribes and replaces it,
// and the staleness is announced rather than hidden.
//
// The caller must have already appended ch to r.watchers for this service.
func (r *Registry) serveStaleOnFailure(ctx context.Context, serviceName string, ch chan []registry.Instance, cause error) error {
	insts, age, ok := r.snapshot(serviceName)
	if !ok {
		r.dropWatcher(serviceName, ch)
		return fmt.Errorf("nacos: subscribe %s: %w", serviceName, cause)
	}

	r.mu.Lock()
	// Kept live on purpose: replay() re-subscribes this service as soon as a
	// connection exists again, and the fresh list then replaces the stale one.
	r.subscriptions[serviceName] = &intent{service: serviceName, live: true}
	r.mu.Unlock()

	r.logf("serving %d instance(s) for %s from snapshot (age %s) after: %v; "+
		"the list is stale, reconnect loop will replace it", len(insts), serviceName, age.Round(time.Second), cause)
	if r.opts.OnSnapshotFallback != nil {
		r.opts.OnSnapshotFallback(serviceName, insts, age)
	}
	select {
	case ch <- insts:
	case <-ctx.Done():
		r.dropWatcher(serviceName, ch)
		return ctx.Err()
	}
	r.scheduleRecover()
	return nil
}

// scheduleRecover starts the reconnect loop for a registry that never had a
// connection at all. onStreamLost only covers the case where one was lost, so a
// cold start against an unreachable server would otherwise never heal.
func (r *Registry) scheduleRecover() {
	r.mu.Lock()
	if r.closing || r.recovering {
		r.mu.Unlock()
		return
	}
	r.recovering = true
	r.mu.Unlock()
	go r.recover()
}
