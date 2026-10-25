// Package client opens sweet-managed gRPC client connections that address
// peers through a registry instead of a literal host list.
package client

import (
	"errors"
	"strings"

	"google.golang.org/grpc"

	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/resolver"
)

// Dial creates a lazily-connecting ClientConn for serviceName via reg.
//
// The target is "<scheme>:///<serviceName>"; the resolver builder is injected
// per connection through grpc.WithResolvers, so no process-global resolver
// registration happens and independent registries can coexist.
func Dial(reg registry.Registry, serviceName string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if reg == nil {
		return nil, errors.New("client: registry required")
	}
	if strings.TrimSpace(serviceName) == "" {
		return nil, errors.New("client: serviceName required")
	}
	opts = append(opts, grpc.WithResolvers(resolver.Builder{Reg: reg}))
	return grpc.NewClient(reg.Name()+":///"+serviceName, opts...)
}
