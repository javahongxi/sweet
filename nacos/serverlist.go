package nacos

import (
	"sort"
	"sync"
	"time"
)

// serverList picks which Nacos endpoint to dial and remembers which ones just
// failed, so a reconnect rotates instead of hammering the node that is down.
//
// The failover is deliberately connection-level. The official client does not
// re-send a failed request somewhere else: its retry loop runs, then the
// exception is thrown to the caller (RpcClient.java:721-723), and only a
// RUNNING → UNHEALTHY transition triggers switchServerAsyncOnRequestFail()
// (:717-718) — which changes the *next* connection, not the call in flight.
// Request-level failover would mean deciding per request whether a register,
// deregister or publish is safe to replay, and that is a policy the caller owns,
// not the transport.
type serverList struct {
	mu       sync.Mutex
	addrs    []string
	failed   map[string]time.Time
	cooldown time.Duration
	cursor   int
}

// NewServerList builds a list from addresses in preference order. An empty or
// all-blank input yields nil, which callers treat as "no list, use the single
// address I already have".
func newServerList(addrs []string, cooldown time.Duration) *serverList {
	kept := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a != "" {
			kept = append(kept, a)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	if cooldown <= 0 {
		// Long enough that a restarting node is not skipped forever, short enough
		// that a real outage still rotates away from it.
		cooldown = 30 * time.Second
	}
	return &serverList{addrs: kept, failed: map[string]time.Time{}, cooldown: cooldown}
}

// Pick returns the address to try next. Preference order: a node with no failure
// record, then one whose cooldown has expired, then the least-recently-failed —
// so a cluster where *everything* is down still gets probed in a stable order
// instead of freezing on one node forever.
func (l *serverList) Pick() string {
	if l == nil {
		return ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	// 1. never-failed, in declared order starting from the cursor
	for i := range l.addrs {
		idx := (l.cursor + i) % len(l.addrs)
		a := l.addrs[idx]
		if _, bad := l.failed[a]; !bad {
			l.cursor = idx
			return a
		}
	}
	// 2. cooldown expired
	for i := range l.addrs {
		idx := (l.cursor + i) % len(l.addrs)
		a := l.addrs[idx]
		if t, bad := l.failed[a]; bad && now.Sub(t) >= l.cooldown {
			l.cursor = idx
			return a
		}
	}
	// 3. oldest failure
	oldest := l.addrs[0]
	var best time.Time
	for i, a := range l.addrs {
		t := l.failed[a]
		if i == 0 || t.Before(best) {
			oldest, best = a, t
		}
	}
	l.cursor = func() int {
		for i, a := range l.addrs {
			if a == oldest {
				return i
			}
		}
		return 0
	}()
	return oldest
}

// MarkFailed records that this address did not complete a handshake, so Pick
// avoids it until the cooldown passes.
func (l *serverList) MarkFailed(addr string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failed[addr] = time.Now()
}

// MarkHealthy clears a node's failure record: a successful handshake is the
// evidence that matters, not a timer.
func (l *serverList) MarkHealthy(addr string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failed, addr)
}

// Addrs reports the list and which entries are currently cooling down, for
// logging and for a caller that wants to surface health without reaching in here.
func (l *serverList) Addrs() (all []string, unhealthy []string) {
	if l == nil {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := append([]string(nil), l.addrs...)
	for a := range l.failed {
		unhealthy = append(unhealthy, a)
	}
	sort.Strings(unhealthy)
	return out, unhealthy
}
