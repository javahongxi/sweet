// Package direct implements registry.Registry without a registry server: the
// peer addresses are the ones written into the target URI.
//
// It exists for two reasons: sweet's end-to-end path must be exercisable
// without a running Nacos, and it is the test double the nacos registry is
// compared against. It is not a feature — there is no discovery in it.
package direct

import (
	"context"
	"errors"
	"strings"

	"github.com/javahongxi/sweet/registry"
)

// Registry resolves every service name to a fixed instance list.
type Registry struct {
	instances []registry.Instance
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
	return &Registry{instances: out}, nil
}

func (r *Registry) Name() string { return "direct" }

// Expose is not supported: there is nothing to register into.
func (r *Registry) Expose(context.Context, string, registry.Instance) (func(), error) {
	return nil, errors.New("direct: registry is read-only")
}

// Watch returns the fixed list once. The channel is closed immediately after,
// which the resolver treats as "no further updates".
func (r *Registry) Watch(ctx context.Context, _ string) (<-chan []registry.Instance, error) {
	ch := make(chan []registry.Instance, 1)
	select {
	case ch <- append([]registry.Instance(nil), r.instances...):
	case <-ctx.Done():
		close(ch)
	}
	close(ch)
	return ch, nil
}

func splitHostPort(ep string) (string, int, error) {
	i := strings.LastIndexByte(ep, ':')
	if i <= 0 || i == len(ep)-1 {
		return "", 0, errors.New("direct: bad endpoint " + ep)
	}
	port := 0
	for _, c := range ep[i+1:] {
		if c < '0' || c > '9' {
			return "", 0, errors.New("direct: bad port in " + ep)
		}
		port = port*10 + int(c-'0')
	}
	return ep[:i], port, nil
}
