package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// routeCacheRecord is one accepted route table kept on disk.
//
// The identity fields are stored inside the file and verified on read, for the
// same reason registry/nacos/snapshot.go does it: a file name can outlive the
// source it came from — a cache pointed at another dataId, or copied between
// environments, still parses as a perfectly good table. Serving routes nobody
// configured for this edge is worse than serving none.
type routeCacheRecord struct {
	SavedAt time.Time   `json:"savedAt"`
	Source  string      `json:"source"`
	Routes  []RouteSpec `json:"routes"`
}

// cacheRoutes writes the table that was just accepted. It is called only after
// ApplyRoutes has swapped the live table, so the cache can never hold a table
// the gateway refused.
//
// A failure here is reported through Options.OnRouteCache rather than returned:
// the routes are live, and turning that into an error would tell the operator
// the opposite of what happened.
func (g *Gateway) cacheRoutes(specs []RouteSpec) {
	path := g.opts.RouteCacheFile
	if path == "" {
		return
	}
	rec := routeCacheRecord{SavedAt: time.Now(), Source: g.opts.RouteCacheSource, Routes: specs}
	body, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		g.reportCacheErr(fmt.Errorf("gateway: encode route cache: %w", err))
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		g.reportCacheErr(fmt.Errorf("gateway: route cache dir %s: %w", filepath.Dir(path), err))
		return
	}
	// Write-then-rename: a reader must never see a half-written table, and a torn
	// file that still parses is worse than no file because it looks authoritative.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		g.reportCacheErr(fmt.Errorf("gateway: write route cache %s: %w", tmp, err))
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		g.reportCacheErr(fmt.Errorf("gateway: rename route cache %s: %w", path, err))
		return
	}
}

func (g *Gateway) reportCacheErr(err error) {
	if g.opts.OnRouteCache != nil {
		g.opts.OnRouteCache(err)
	}
}

// RestoreRoutes installs the last accepted table kept on disk. It is the cold
// start answer to "the config centre is unreachable", and it exists because a
// gateway that cannot read Nacos currently serves nothing at all.
//
// applied=false with a nil error means there is nothing to restore (no cache
// configured, or no file yet). Any other false comes with the reason: an
// undated, empty, mismatched or now-unresolvable table is refused rather than
// installed — a stale route table is a deliberate trade, a wrong one is a bug.
//
// Order matters at startup: restore first, then watch the live source. A live
// table that arrives afterwards simply replaces the stale one, which is what
// registry/nacos does with serveStaleOnFailure.
func (g *Gateway) RestoreRoutes() (applied bool, age time.Duration, err error) {
	path := g.opts.RouteCacheFile
	if path == "" {
		return false, 0, nil
	}
	body, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("gateway: read route cache %s: %w", path, err)
	}
	var rec routeCacheRecord
	if err := json.Unmarshal(body, &rec); err != nil {
		return false, 0, fmt.Errorf("gateway: route cache %s undecodable: %w", path, err)
	}
	if g.opts.RouteCacheSource != "" && rec.Source != g.opts.RouteCacheSource {
		return false, 0, fmt.Errorf("gateway: route cache %s holds source %q, not %q — refusing to use it",
			path, rec.Source, g.opts.RouteCacheSource)
	}
	if rec.SavedAt.IsZero() {
		return false, 0, fmt.Errorf("gateway: route cache %s has no timestamp, refusing to install an undated table", path)
	}
	if len(rec.Routes) == 0 {
		return false, 0, fmt.Errorf("gateway: route cache %s holds no routes; refusing to install an empty table", path)
	}
	age = time.Since(rec.SavedAt)
	if age < 0 {
		// The clock moved backwards. The table is still the best thing we have,
		// but reporting a negative age would hide that from anyone reading a log.
		age = 0
	}
	if err := g.ApplyRoutes(rec.Routes); err != nil {
		return false, 0, fmt.Errorf("gateway: route cache %s no longer applies: %w", path, err)
	}
	// The staleness is reported to the caller rather than logged here: what to do
	// with a table of unknown age is a policy question, and registry/nacos makes
	// the same choice by handing the age back in SnapshotAge.
	return true, age, nil
}
