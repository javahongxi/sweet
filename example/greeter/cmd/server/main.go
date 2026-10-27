// Command greeter-server brings a sweet server online with protoc-generated
// stubs — the Go-calls-Go counterpart of example/helloworld/cmd/server, whose
// service is hand-written with grpc.ServiceDesc instead.
//
// Everything about the lifecycle is sweet's own: the only difference in this
// file is what gets passed to Register (pb.Greeter_ServiceDesc instead of a
// hand-written ServiceDesc) and the method signatures of the implementation.
//
// -registry=nacos (the default) registers the instance through the hand-written
// naming protocol, so peers find it by service name alone. -registry=direct is a
// passthrough used for development without a registry: it registers nothing and
// discovers nothing, so clients must be told the address with -advertise.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
	"github.com/javahongxi/sweet/server"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
)

// greeter implements pb.GreeterServer. UnimplementedGreeterServer is embedded by
// value as the generated code requires (a nil pointer embed dereferences on the
// first unimplemented method). sweet's Register never runs the generated
// RegisterGreeterServer, so the only check on this path is grpc-go's own — and
// RegisterService reports a handler-type mismatch by calling grpclog.Fatalf, it
// returns nothing to check. The assertion below moves that to build time.
type greeter struct {
	pb.UnimplementedGreeterServer
}

var _ pb.GreeterServer = greeter{}

func (greeter) Greet(_ context.Context, in *pb.GreetRequest) (*pb.GreetReply, error) {
	if in.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return &pb.GreetReply{Message: fmt.Sprintf("Hello, %s!", in.GetName())}, nil
}

func (greeter) GreetStream(in *pb.GreetRequest, stream pb.Greeter_GreetStreamServer) error {
	if in.GetName() == "" {
		return status.Error(codes.InvalidArgument, "name is required")
	}
	for i := int32(1); i <= in.GetTimes(); i++ {
		if err := stream.Send(&pb.GreetReply{
			Message: fmt.Sprintf("Hello #%d, %s!", i, in.GetName()),
		}); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	listen := flag.String("listen", "127.0.0.1:20001", "address to listen on")
	advertise := flag.String("advertise", "", "address reported to the registry (default: -listen)")
	weight := flag.Int("weight", 100, "weight advertised to the load balancer")
	registryKind := flag.String("registry", "nacos", "nacos | direct")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	serviceName := flag.String("service", pb.Greeter_ServiceDesc.ServiceName, "service name to register and serve")
	flag.Parse()

	var reg registry.Registry
	switch *registryKind {
	case "nacos":
		nr, err := nacosregistry.New(nacosregistry.Options{ServerAddr: *serverAddr})
		if err != nil {
			log.Fatalf("registry: %v", err)
		}
		reg = nr
	case "direct":
		dr, err := direct.New(orDefault(*advertise, *listen))
		if err != nil {
			log.Fatalf("registry: %v", err)
		}
		reg = dr
		// direct registers nothing, so peers must be told the address out of band.
		log.Printf("registry=direct is a passthrough: clients should dial -endpoints %s", orDefault(*advertise, *listen))
	default:
		log.Fatalf("unknown -registry %q", *registryKind)
	}

	srv, err := server.New(server.Options{
		Address:     *listen,
		ServiceName: *serviceName,
		Weight:      *weight,
		Registry:    reg,
		DrainDelay:  200 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("server.New: %v", err)
	}
	if err := srv.Register(&pb.Greeter_ServiceDesc, greeter{}); err != nil {
		log.Fatalf("register: %v", err)
	}
	defer srv.Stop()

	// SIGTERM/SIGINT must reach Stop(): without a signal handler the process
	// dies outright, the connection drops, and Nacos removes the instance with
	// no chance to drain.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		if err := srv.Serve(); err != nil {
			log.Printf("serve: %v", err)
		}
	}()
	if err := srv.Listen(); err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("sweet server up: listen=%s advertised=%s service=%s registry=%s",
		srv.Addr(), orDefault(*advertise, *listen), srv.ServiceName(), reg.Name())

	<-stop
	log.Printf("signal received, draining (grace period %s)", srv.DrainDelay())
	srv.Stop()
	log.Printf("stopped: deregistered then drained")
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
