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
	"strconv"
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

	// Same line format as example/greeter/cmd/client: one line per frame as it
	// lands, then a closing count. The recv loop still lives inside ping.Events —
	// that is this example's whole point — but delivery is now incremental, so the
	// output shows a stream instead of the shape of a collected slice.
	pong, err := ping.NewClient(conn).Ping(ctx, "hi")
	if err != nil {
		log.Fatalf("Ping: %v", err)
	}
	log.Printf("resolved %s through %s registry", *service, reg.Name())
	log.Printf("Ping       <= %q", pong)

	frames := 0
	if err := ping.NewClient(conn).Events(ctx, "a,b,c", func(event string) {
		frames++
		log.Printf("Events     <= %d %q", frames, event)
	}); err != nil {
		log.Fatalf("Events: %v (after %s)", err, plural(frames, "frame"))
	}
	log.Printf("Events     == %s, closed by server", plural(frames, "frame"))
}

// plural counts a noun the way these lines should read: "1 frame", "3 frames".
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}
