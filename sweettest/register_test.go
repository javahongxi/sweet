package sweettest

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/javahongxi/sweet/registry/direct"
	"github.com/javahongxi/sweet/server"
)

// otherIface / otherDesc stand in for "a second service, with a different
// descriptor name and its own handler contract". No methods are needed: the
// checks under test look at the descriptor name and at HandlerType only.
type otherIface interface {
	Other(context.Context) error
}

type otherHandler struct{}

func (otherHandler) Other(context.Context) error { return nil }

// notOtherHandler satisfies nothing of otherIface, which is the point.
type notOtherHandler struct{}

var otherDesc = grpc.ServiceDesc{
	ServiceName: "sweet.example.v1.Other",
	HandlerType: (*otherIface)(nil),
	Metadata:    "sweet/example/other.proto",
}

func newUnstarted(t *testing.T, serviceName string) *server.Server {
	t.Helper()
	srv, err := server.New(server.Options{
		Address:     "127.0.0.1:0",
		ServiceName: serviceName,
		Registry:    direct.Nope(),
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	return srv
}

func TestRegisterRejectsNilDescriptor(t *testing.T) {
	srv := newUnstarted(t, svcName)
	err := srv.Register(nil, otherHandler{})
	if err == nil {
		t.Fatal("Register(nil, …) returned no error")
	}
	if !strings.Contains(err.Error(), "non-nil ServiceDesc") {
		t.Fatalf("error = %v, want it to name the missing descriptor", err)
	}
}

// The documented rule: with Options.ServiceName left empty, the first descriptor
// names the instance and a second, differently named service is refused rather
// than silently served on a name nobody can resolve.
func TestRegisterRefusesASecondServiceNameWhenItWasInferred(t *testing.T) {
	srv := newUnstarted(t, "")
	if err := srv.Register(&pingDesc, pingServer{}); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	err := srv.Register(&otherDesc, otherHandler{})
	if err == nil {
		t.Fatal("second Register with a different name returned no error")
	}
	for _, want := range []string{"conflicts", otherDesc.ServiceName, svcName} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
	if got := srv.ServiceName(); got != svcName {
		t.Fatalf("ServiceName() = %q after a refused Register, want %q — the failed call must not change state", got, svcName)
	}
}

// The escape hatch, asserted too: an explicit name is not inference, so several
// services are allowed.
func TestRegisterAllowsSeveralServicesWhenNameIsExplicit(t *testing.T) {
	srv := newUnstarted(t, svcName)
	if err := srv.Register(&pingDesc, pingServer{}); err != nil {
		t.Fatalf("Register ping: %v", err)
	}
	if err := srv.Register(&otherDesc, otherHandler{}); err != nil {
		t.Fatalf("Register other: %v", err)
	}
	if got := srv.ServiceName(); got != svcName {
		t.Fatalf("ServiceName() = %q, want the explicit %q", got, svcName)
	}
}

func TestRegisterReportsAHandlerThatDoesNotMatchTheDescriptor(t *testing.T) {
	srv := newUnstarted(t, "")
	err := srv.Register(&otherDesc, notOtherHandler{})
	if err == nil {
		t.Fatal("Register with a non-conforming handler returned no error; " +
			"grpc-go would have killed the process with grpclog.Fatalf instead")
	}
	if !strings.Contains(err.Error(), "does not implement") {
		t.Fatalf("error = %v, want it to say the handler does not implement the descriptor", err)
	}
	// Nothing was inferred either: the failure came before any state change.
	if got := srv.ServiceName(); got != "" {
		t.Fatalf("ServiceName() = %q after a refused Register, want it still empty", got)
	}
}

func TestRegisterAfterServeIsAnError(t *testing.T) {
	srv := startServer(t, direct.Nope())
	err := srv.Register(&otherDesc, otherHandler{})
	if err == nil {
		t.Fatal("Register after Serve returned no error")
	}
	if !strings.Contains(err.Error(), "after Serve") {
		t.Fatalf("error = %v, want it to name the ordering mistake", err)
	}
}

// The health service is registered by server.New itself, so a caller who never
// sets Options.ServiceName must still end up with its own service name — not
// grpc.health.v1.Health.
func TestInferenceIgnoresTheBuiltInServices(t *testing.T) {
	srv := newUnstarted(t, "")
	if err := srv.Register(&pingDesc, pingServer{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := srv.ServiceName(); got != svcName {
		t.Fatalf("ServiceName() = %q, want %q", got, svcName)
	}
	if err := srv.Register(&otherDesc, otherHandler{}); err == nil ||
		!strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("error = %v, want the conflict to be refused even with health/reflection registered", err)
	}
}
