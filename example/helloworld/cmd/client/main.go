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
	"strings"
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
	values := flag.String("values", "a,b,c", "comma-separated payloads, one per request for Echo/Ack")
	snapshot := flag.String("snapshot", "", "directory holding the instance-list snapshot; when set, a "+
		"cold start against an unreachable Nacos falls back to the last list it wrote, and the age is "+
		"reported. Empty — the default — means no snapshot is read or written.")
	flag.Parse()

	requests := strings.Split(*values, ",")

	var reg registry.Registry
	switch *registryKind {
	case "nacos":
		nr, err := nacosregistry.New(nacosregistry.Options{
			ServerAddr:  *serverAddr,
			SnapshotDir: *snapshot,
			// The registry logs it too; this puts the fallback in this program's own
			// output so -snapshot can be seen working rather than trusted.
			OnSnapshotFallback: func(service string, insts []registry.Instance, age time.Duration) {
				log.Printf("serving %s from snapshot: %d instance(s), %s old — addresses may be stale",
					service, len(insts), age.Round(time.Second))
			},
		})
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
	// lands, then a closing count. The recv loops still live inside the ping
	// methods — that is this example's whole point — but delivery is incremental,
	// so the output shows a stream instead of the shape of a collected slice.
	//
	// Call order matches example/greeter: unary, server-streaming, bidirectional,
	// client-streaming.
	cli := ping.NewClient(conn)
	frames := 0

	pong, err := cli.Ping(ctx, "hi")
	if err != nil {
		log.Fatalf("Ping: %v", err)
	}
	log.Printf("resolved %s through %s registry", *service, reg.Name())
	log.Printf("Ping       <= %q", pong)

	frames = 0
	if err := cli.Events(ctx, *values, func(event string) {
		frames++
		log.Printf("Events     <= %d %q", frames, event)
	}); err != nil {
		log.Fatalf("Events: %v (after %s)", err, plural(frames, "frame"))
	}
	log.Printf("Events     == %s, closed by server", plural(frames, "frame"))

	frames = 0
	if err := cli.Echo(ctx, requests, func(reply string) {
		frames++
		log.Printf("Echo       <= %d %q", frames, reply)
	}); err != nil {
		log.Fatalf("Echo: %v (after %s)", err, plural(frames, "frame"))
	}
	log.Printf("Echo       == %s from %s, closed by server",
		plural(frames, "frame"), plural(len(requests), "request"))

	// Client-streaming: one reply out, so there is a single <= line; the closing
	// count says what produced it.
	ack, err := cli.Ack(ctx, requests)
	if err != nil {
		log.Fatalf("Ack: %v", err)
	}
	log.Printf("Ack        <= %q", ack)
	log.Printf("Ack        == %s from %s", plural(1, "frame"), plural(len(requests), "request"))
}

// plural counts a noun the way these lines should read: "1 frame", "3 frames".
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}
