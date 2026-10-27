// Command greeter-client resolves a service through a registry and calls it with
// protoc-generated stubs — the Go-calls-Go counterpart of
// example/helloworld/cmd/client, which calls a hand-written ServiceDesc.
//
// The generated client is used as-is: NewGreeterClient takes the *grpc.ClientConn
// that client.Dial returns, so sweet changes how the connection is addressed and
// nothing about the call site.
//
// -registry=nacos (the default) discovers the instance by service name alone.
// -registry=direct needs -endpoints because it discovers nothing.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
)

// plural counts a noun the way these lines should read: "1 frame", "3 frames".
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return strconv.Itoa(n) + " " + noun + "s"
}

func main() {
	registryKind := flag.String("registry", "nacos", "nacos | direct")
	endpoints := flag.String("endpoints", "127.0.0.1:20001", "comma-separated host:port, used by direct")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	snapshot := flag.String("snapshot", "", "directory holding the instance-list snapshot; when set, a "+
		"cold start against an unreachable Nacos falls back to the last list it wrote, and the age is "+
		"reported. Empty — the default — means no snapshot is read or written.")
	service := flag.String("service", pb.Greeter_ServiceDesc.ServiceName, "service name to resolve")
	name := flag.String("name", "sweet", "name to greet")
	times := flag.Int("times", 3, "how many greetings to ask the stream for")
	names := flag.String("names", "a,b,c", "comma-separated names, one request per name, used by GreetChat and GreetAll")
	hold := flag.Int("hold", 0, "seconds to stay alive after the first round, then call once more; "+
		"used to watch a snapshot fallback heal itself when Nacos comes back")
	flag.Parse()

	// An empty -names deliberately sends zero requests, which is the one input that
	// tells GreetChat (nothing said → nothing answered) and GreetAll (a reply is
	// promised, so zero names is InvalidArgument) apart.
	var speakers []string
	if s := strings.TrimSpace(*names); s != "" {
		speakers = strings.Split(s, ",")
	}

	var reg registry.Registry
	switch *registryKind {
	case "nacos":
		nr, err := nacosregistry.New(nacosregistry.Options{
			ServerAddr:  *serverAddr,
			SnapshotDir: *snapshot,
			// The registry logs the fallback too; this makes it a line in this
			// program's own output, so -snapshot can be seen working rather than
			// trusted.
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

	cli := pb.NewGreeterClient(conn)

	reply, err := cli.Greet(ctx, &pb.GreetRequest{Name: *name})
	if err != nil {
		log.Fatalf("Greet: %v", err)
	}

	log.Printf("resolved %s through %s registry", *service, reg.Name())
	log.Printf("Greet       <= %q", reply.GetMessage())

	stream, err := cli.GreetStream(ctx, &pb.GreetRequest{Name: *name, Times: int32(*times)})
	if err != nil {
		log.Fatalf("GreetStream: %v", err)
	}
	// One line per received frame, printed as it arrives: collecting the whole
	// stream into a slice and printing it once would hide that this is a stream,
	// and would hide how many frames had landed if the stream ever broke early.
	frames := 0
	for {
		// Recv returns io.EOF when the server closes the stream, which is the
		// hand-written client's only real extra work: ping.Events hides this loop.
		m, err := stream.Recv()
		if err == io.EOF {
			log.Printf("GreetStream == %s, closed by server", plural(frames, "frame"))
			break
		}
		if err != nil {
			log.Fatalf("GreetStream Recv: %v (after %s)", err, plural(frames, "frame"))
		}
		frames++
		log.Printf("GreetStream <= %d %q", frames, m.GetMessage())
	}

	// Bidirectional: the server answers each name as it arrives. Requests are sent
	// first and replies read afterwards here, which is safe for a handful of small
	// frames — they fit inside the flow-control window, so the send loop cannot
	// block while nothing is reading. A conversation that has to interleave needs a
	// second goroutine for Send.
	chat, err := cli.GreetChat(ctx)
	if err != nil {
		log.Fatalf("GreetChat: %v", err)
	}
	for _, n := range speakers {
		if err := chat.Send(&pb.GreetRequest{Name: n}); err != nil {
			log.Fatalf("GreetChat Send: %v", err)
		}
	}
	if err := chat.CloseSend(); err != nil {
		log.Fatalf("GreetChat CloseSend: %v", err)
	}
	frames = 0
	for {
		m, err := chat.Recv()
		if err == io.EOF {
			log.Printf("GreetChat   == %s from %s, closed by server",
				plural(frames, "frame"), plural(len(speakers), "request"))
			break
		}
		if err != nil {
			log.Fatalf("GreetChat Recv: %v (after %s)", err, plural(frames, "frame"))
		}
		frames++
		log.Printf("GreetChat   <= %d %q", frames, m.GetMessage())
	}

	// Client streaming: many requests in, exactly one reply out. CloseAndRecv is the
	// half-close plus that reply; the server sees the half-close as io.EOF from Recv,
	// which is the only signal that its input is complete.
	all, err := cli.GreetAll(ctx)
	if err != nil {
		log.Fatalf("GreetAll: %v", err)
	}
	for _, n := range speakers {
		if err := all.Send(&pb.GreetRequest{Name: n}); err != nil {
			log.Fatalf("GreetAll Send: %v", err)
		}
	}
	allReply, err := all.CloseAndRecv()
	if err != nil {
		log.Fatalf("GreetAll CloseAndRecv: %v", err)
	}
	log.Printf("GreetAll    <= %q", allReply.GetMessage())
	log.Printf("GreetAll    == %s from %s", plural(1, "frame"), plural(len(speakers), "request"))

	// Staying alive is the point: a process that exits cannot show that a
	// fallback healed. With Nacos restarted during the hold, the reconnect loop
	// re-subscribes, and this second call runs on the server-confirmed list.
	if *hold > 0 {
		log.Printf("holding %ds: restart Nacos now to watch the snapshot fallback heal", *hold)
		time.Sleep(time.Duration(*hold) * time.Second)
		// A fresh context: the round above had a 5s budget, and reusing it after a
		// hold measures the sleep, not the healing.
		hctx, hcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer hcancel()
		healed, err := cli.Greet(hctx, &pb.GreetRequest{Name: *name})
		if err != nil {
			log.Fatalf("Greet after hold: %v", err)
		}
		log.Printf("after hold  <= %q", healed.GetMessage())
	}
}
