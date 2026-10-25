// Command server brings a sweet server online.
//
// The example ships with registry=direct, which is a passthrough: it registers
// nothing and discovers nothing, so the address clients must reach is given
// with -advertise. When registry/nacos lands, that argument disappears — the
// instance registers itself and peers find it by service name.
package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/javahongxi/sweet/example/helloworld/ping"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
	"github.com/javahongxi/sweet/server"
)

type pingImpl struct{}

func (pingImpl) Ping(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String("pong"), nil
}

func (pingImpl) Events(in *wrapperspb.StringValue, stream grpc.ServerStreamingServer[wrapperspb.StringValue]) error {
	for _, e := range strings.Split(in.GetValue(), ",") {
		if err := stream.Send(wrapperspb.String(e)); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	listen := flag.String("listen", "127.0.0.1:20000", "address to listen on")
	advertise := flag.String("advertise", "", "address reported to the registry (default: -listen)")
	weight := flag.Int("weight", 100, "weight advertised to the load balancer")
	registryKind := flag.String("registry", "nacos", "nacos | direct")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
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
		ServiceName: ping.ServiceName,
		Weight:      *weight,
		Registry:    reg,
		DrainDelay:  200 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("server.New: %v", err)
	}
	if err := srv.Register(&ping.ServiceDesc, pingImpl{}); err != nil {
		log.Fatalf("register: %v", err)
	}
	defer srv.Stop()

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

	select {}
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}
