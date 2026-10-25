// Package direct implements registry.Registry without a registry server: the
// peer addresses are the ones written into the constructor.
//
// It exists so sweet's end-to-end path is exercisable without a running Nacos,
// and as the stand-in the nacos registry is compared against. It performs no
// discovery: Expose is a recorded no-op (a server can come up on it), Watch
// returns a fixed list once and closes.
package direct

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/javahongxi/sweet/registry"
)

// Registry resolves every service name to a fixed instance list. A zero value
// is a valid read-only registry (see Nope).
type Registry struct {
	mu      sync.Mutex
	fixed   []registry.Instance
	exposed map[string]int // service name → live Expose calls, for observability
}

// New parses a comma-separated list of host:port endpoints.
func New(endpoints string) (*Registry, error) {
	var out []registry.Instance
	for _, ep := range strings.Split(endpoints, ",") {
		ep = strings.TrimSpace(ep)
		if ep == "" {
			continue
		}
		host, port, err := splitHostPort(ep)
		if err != nil {
			return nil, err
		}
		out = append(out, registry.Instance{Host: host, Port: port, Weight: 100})
	}
	if len(out) == 0 {
		return nil, errors.New("direct: no endpoints")
	}
	return &Registry{fixed: out, exposed: map[string]int{}}, nil
}

func (r *Registry) Name() string { return "direct" }

// Expose acknowledges the instance without publishing it: a direct client is
// addressed by the endpoints given to New, not by anything servers report.
// The returned function is idempotent.
func (r *Registry) Expose(_ context.Context, serviceName string, in registry.Instance) (func(), error) {
	if r.fixed == nil && r.exposed == nil {
		return func() {}, nil // Nope: nothing to record
	}
	if in.Host == "" || in.Port <= 0 {
		return nil, fmt.Errorf("direct: instance %q has no usable address", serviceName)
	}
	r.mu.Lock()
	r.exposed[serviceName]++
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if n := r.exposed[serviceName]; n > 0 {
				r.exposed[serviceName] = n - 1
			}
			r.mu.Unlock()
		})
	}, nil
}

// Watch sends the fixed list once and closes the channel; a closed channel
// simply means "no further updates", which the resolver treats as terminal.
func (r *Registry) Watch(_ context.Context, _ string) (<-chan []registry.Instance, error) {
	ch := make(chan []registry.Instance, 1)
	r.mu.Lock()
	fixed := r.fixed
	r.mu.Unlock()
	if len(fixed) > 0 {
		ch <- append([]registry.Instance(nil), fixed...)
	}
	close(ch)
	return ch, nil
}

// Exposed reports how many instances are currently acknowledged for a service.
// It exists so tests can assert the server side actually called through.
func (r *Registry) Exposed(serviceName string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.exposed[serviceName]
}

func splitHostPort(ep string) (string, int, error) {
	i := strings.LastIndexByte(ep, ':')
	if i <= 0 || i == len(ep)-1 {
		return "", 0, errors.New("direct: bad endpoint " + ep)
	}
	port, err := strconv.Atoi(ep[i+1:])
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, errors.New("direct: bad port in " + ep)
	}
	return ep[:i], port, nil
}

// Nope returns a read-only registry for the case where a server is started
// purely to serve an already-addressable peer — interop tests against a stock
// gRPC client, for instance. It is not a discovery mechanism: it registers
// nothing and resolves nothing.
func Nope() registry.Registry { return &Registry{} }
