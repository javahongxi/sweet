// Command applevel-provider exposes sweet's Greeter through application-level
// service discovery — the registry shape jaws' registerMode=instance uses, and
// the counterpart of example/greeter/cmd/server's interface-level registration.
//
// The difference is entirely below the service implementation: the registry
// handed to server.New is an applevel.Provider decorator. Serve's single Expose
// call then registers ONE lightweight application instance (address + revision
// pointer) instead of one instance per interface, publishes the interface →
// application mapping to the config center, and keeps the full service URLs in
// this process's memory. Peers pull them over gRPC through the metadata service
// descriptor registered beside the business one — same port, same address, no
// second registry entry.
//
// The defaults aim at the jaws interop leg: -namespace=jaws registers
// "jaws/<app>" the way a jaws consumer's default registryNamespace subscribes.
// The group default_rpc is shared by both frameworks; run with -namespace=
// for sweet's own bare-name convention, the consumer side picks the matching
// flag (see ../consumer and README.md).
//
// Nacos must be listening on 127.0.0.1:8848 (SDK gRPC port 9848).
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

	"github.com/javahongxi/sweet/applevel"
	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
	"github.com/javahongxi/sweet/nacos"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
	"github.com/javahongxi/sweet/server"
)

// serviceName is the proto's package.Service verbatim — it becomes the service
// URL path inside the metadata, and a jaws consumer's outbound :path is built
// from its Java interface name, so the jaws leg needs an interface called
// exactly sweet.example.v1.Greeter (jaws-sample-wire-sweet has one).
const serviceName = "sweet.example.v1.Greeter"

// greeter is the same implementation as example/greeter/cmd/server, minus the
// logging interceptor: this example's variable is the discovery shape, not the
// handler, so the four method shapes stay identical for line-by-line comparison.
type greeter struct {
	pb.UnimplementedGreeterServer
}

var _ pb.GreeterServer = greeter{}

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
	listen := flag.String("listen", "127.0.0.1:20010", "address to listen on")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	app := flag.String("app", "sweet-greeter-app", "application name peers subscribe to")
	namespace := flag.String("namespace", "jaws",
		"prefix of the registered application name: jaws matches a jaws consumer's default "+
			"registryNamespace, empty registers the bare name sweet's own consumers watch")
	group := flag.String("group", "default_rpc",
		"Nacos group AND the URL group parameter; default_rpc is shared by sweet and jaws")
	weight := flag.Int("weight", 100, "instance weight advertised through the service URL")
	flag.Parse()

	nr, err := nacosregistry.New(nacosregistry.Options{ServerAddr: *serverAddr, GroupName: *group})
	if err != nil {
		log.Fatalf("registry: %v", err)
	}
	defer nr.Close()
	cc, err := nacos.NewConfigClient(nacos.ConfigOptions{Addr: *serverAddr})
	if err != nil {
		log.Fatalf("config client: %v", err)
	}
	defer cc.Close()

	provider, err := applevel.NewProvider(nr, cc, applevel.ProviderOptions{
		Application: *app,
		Namespace:   *namespace,
		Group:       *group,
		Logger:      log.Default(),
	})
	if err != nil {
		log.Fatalf("applevel provider: %v", err)
	}

	srv, err := server.New(server.Options{
		Address:     *listen,
		ServiceName: serviceName,
		Weight:      *weight,
		Registry:    provider,
		DrainDelay:  2 * time.Second,
	})
	if err != nil {
		log.Fatalf("server: %v", err)
	}
	if err := srv.Register(&pb.Greeter_ServiceDesc, greeter{}); err != nil {
		log.Fatalf("register greeter: %v", err)
	}
	// The metadata pull channel rides the SAME server: peers fetch revisions
	// from the instance address they discovered, so it must answer there.
	if err := srv.Register(provider.MetadataServiceDesc(), provider); err != nil {
		log.Fatalf("register metadata service: %v", err)
	}

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
	registered := *app
	if *namespace != "" {
		registered = *namespace + "/" + *app
	}
	log.Printf("app-level provider up: listen=%s application=%s registered=%s group=%s service=%s",
		srv.Addr(), *app, registered, *group, serviceName)

	<-stop
	log.Printf("signal received, draining (grace period %s)", srv.DrainDelay())
	srv.Stop()
	log.Printf("stopped: application instance deregistered then drained")
}
