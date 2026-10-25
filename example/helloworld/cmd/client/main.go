// Command client resolves a service through a registry and calls it.
//
// With -registry=direct the "registry" is just the fixed endpoints passed in
// -endpoints, which is what runs today. The nacos path is not implemented yet.
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
	"github.com/javahongxi/sweet/registry/direct"
)

func main() {
	registryKind := flag.String("registry", "direct", "registry implementation: direct (nacos pending)")
	endpoints := flag.String("endpoints", "127.0.0.1:20000", "comma-separated host:port, used by direct")
	service := flag.String("service", ping.ServiceName, "service name to resolve")
	flag.Parse()

	if *registryKind != "direct" {
		log.Fatalf("registry %q not implemented yet (registry/nacos is the hand-written part of this project)", *registryKind)
	}
	reg, err := direct.New(*endpoints)
	if err != nil {
		log.Fatalf("registry: %v", err)
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
	log.Printf("resolved %s through %s registry (%s) → Ping=%q Events=%v",
		*service, reg.Name(), *endpoints, pong, events)
}
