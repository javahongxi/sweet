// Command client resolves a service through a registry and calls it.
//
// -registry=nacos (the default) discovers the instance by service name alone.
// -registry=direct is a passthrough that discovers nothing, so the caller has to
// pass the address in -endpoints.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/example/helloworld/ping"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
)

func main() {
	registryKind := flag.String("registry", "nacos", "nacos | direct")
	endpoints := flag.String("endpoints", "127.0.0.1:20000", "comma-separated host:port, used by direct")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	service := flag.String("service", ping.ServiceName, "service name to resolve")
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
		dr, err := direct.New(*endpoints)
		if err != nil {
			log.Fatalf("registry: %v", err)
		}
		reg = dr
	default:
		log.Fatalf("unknown -registry %q", *registryKind)
	}

	conn, err := client.Dial(reg, *service, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("client.Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pong, err := ping.NewClient(conn).Ping(ctx, "hi")
	if err != nil {
		log.Fatalf("Ping: %v", err)
	}
	events, err := ping.NewClient(conn).Events(ctx, "a,b,c")
	if err != nil {
		log.Fatalf("Events: %v", err)
	}
	log.Printf("resolved %s through %s registry → Ping=%q Events=%v", *service, reg.Name(), pong, events)
}
