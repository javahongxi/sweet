// Package resolver adapts a registry.Registry to grpc-go's resolver interface.
// It is intentionally thin: instance lists in, resolver.State out.
package resolver

import (
	"context"
	"strings"
	"sync"

	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"

	"github.com/javahongxi/sweet/registry"
)

// Builder hands every Dial for its scheme a resolver backed by Reg.
type Builder struct {
	Reg registry.Registry
}

// Scheme is the URI scheme this builder answers, e.g. "direct" or "nacos".
func (b Builder) Scheme() string { return b.Reg.Name() }

// Build implements resolver.Builder.
func (b Builder) Build(t resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	name := strings.TrimPrefix(t.URL.Path, "/")
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := b.Reg.Watch(ctx, name)
	if err != nil {
		cancel()
		return nil, err
	}
	r := &res{ctx: ctx, cancel: cancel, cc: cc}
	r.wg.Add(1)
	go r.run(ch)
	return r, nil
}

type res struct {
	ctx    context.Context
	cancel context.CancelFunc
	cc     resolver.ClientConn

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

func (r *res) run(ch <-chan []registry.Instance) {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case insts, ok := <-ch:
			if !ok {
				return
			}
			r.cc.UpdateState(toState(insts))
		}
	}
}

// ResolveNow is a no-op: pushes, not polling, drive updates.
func (r *res) ResolveNow(resolver.ResolveNowOptions) {}

func (r *res) Close() {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		r.cancel()
	}
	r.mu.Unlock()
	r.wg.Wait()
}

func toState(insts []registry.Instance) resolver.State {
	st := resolver.State{Addresses: make([]resolver.Address, 0, len(insts))}
	for _, in := range insts {
		var a *attributes.Attributes
		for k, v := range in.Metadata {
			if a == nil {
				a = attributes.New(k, v)
				continue
			}
			a = a.WithValue(k, v)
		}
		st.Addresses = append(st.Addresses, resolver.Address{
			Addr:               in.Addr(),
			BalancerAttributes: a,
		})
	}
	return st
}
