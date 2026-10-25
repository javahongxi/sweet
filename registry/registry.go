// Package registry is sweet's service-registry abstraction: one interface, the
// smallest surface that a server needs in order to come online and a client
// needs in order to find peers.
package registry

import (
	"context"
	"strconv"
)

// Instance is one addressable provider of a service.
type Instance struct {
	Host   string
	Port   int
	Weight int
	// Metadata carries registry-specific attributes (cluster, group, version,
	// zone, ...). It is transported to the load balancer verbatim.
	Metadata map[string]string
}

// Addr renders the instance as a gRPC address.
func (in Instance) Addr() string {
	return in.Host + ":" + strconv.Itoa(in.Port)
}

// Registry exposes services and reports the instances behind a service name.
//
// Expose is used by the server side; Watch by the client side. Keeping both on
// one interface is deliberate: a registry backend that can only do one of them
// (e.g. a test double) implements both and returns an error from the other.
type Registry interface {
	// Name identifies the implementation in a target URI scheme.
	Name() string

	// Expose publishes an instance under serviceName until the returned
	// function is called. Implementations must make repeated calls with the
	// same instance idempotent.
	Expose(ctx context.Context, serviceName string, in Instance) (func(), error)

	// Watch delivers the full instance list for serviceName on every change,
	// including an initial snapshot. It stops when ctx is cancelled.
	Watch(ctx context.Context, serviceName string) (<-chan []Instance, error)
}
