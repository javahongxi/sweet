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
	"io"
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

// greeter implements pb.GreeterServer. UnimplementedGreeterServer must be
// embedded by value (a nil pointer embed dereferences on the first unimplemented
// method), and embedding it has a consequence worth knowing: the embedded type
// already has every method, so the interface is satisfied whether or not this
// file implements them. A missing or misnamed method therefore compiles and
// answers codes.Unimplemented at runtime; only a wrong signature is caught at
// build time, and only because the assertion below is what makes the compiler
// look. sweet's Register never runs the generated RegisterGreeterServer, and
// grpc-go's RegisterService reports a mismatch by calling grpclog.Fatalf rather
// than returning an error, so without this line nothing checks the signature
// until a process dies. Both halves of that sentence are measured, see README.
type greeter struct {
	pb.UnimplementedGreeterServer
}

var _ pb.GreeterServer = greeter{}

// checkName is the only place a name is validated, so all four method shapes
// answer the same way. Measuring it once (see README: -name "" on the unary
// method, -names "a,,c" on the bidi one) covers every entry point that calls it.
func checkName(name string) error {
	if name == "" {
		return status.Error(codes.InvalidArgument, "name is required")
	}
	return nil
}

func (greeter) Greet(_ context.Context, in *pb.GreetRequest) (*pb.GreetReply, error) {
	if err := checkName(in.GetName()); err != nil {
		return nil, err
	}
	return &pb.GreetReply{Message: fmt.Sprintf("Hello, %s!", in.GetName())}, nil
}

func (greeter) GreetStream(in *pb.GreetRequest, stream pb.Greeter_GreetStreamServer) error {
	if err := checkName(in.GetName()); err != nil {
		return err
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

// GreetChat is the bidirectional method: it answers each name as it arrives, so
// the two directions run concurrently rather than request-then-whole-response.
// An input stream that closes without a single name is not an error here — nobody
// said anything, so there is nothing to answer.
func (greeter) GreetChat(stream pb.Greeter_GreetChatServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := checkName(in.GetName()); err != nil {
			return err
		}
		if err := stream.Send(&pb.GreetReply{
			Message: fmt.Sprintf("Hello, %s!", in.GetName()),
		}); err != nil {
			return err
		}
	}
}

// GreetAll is the client-streaming method: many names in, one reply out. The
// client's half-close is what tells the handler the input is complete — Recv
// reports it as io.EOF, there is no other signal.
func (greeter) GreetAll(stream pb.Greeter_GreetAllServer) error {
	var names []string
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			if len(names) == 0 {
				return status.Error(codes.InvalidArgument, "at least one name is required")
			}
			return stream.SendAndClose(&pb.GreetReply{
				Message: fmt.Sprintf("Hello, %s!", strings.Join(names, ", ")),
			})
		}
		if err != nil {
			return err
		}
		if err := checkName(in.GetName()); err != nil {
			return err
		}
		names = append(names, in.GetName())
	}
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
