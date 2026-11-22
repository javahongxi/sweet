// Command applevel-consumer discovers a service through application-level
// discovery and calls it with the stock generated stub — the client-side
// counterpart of ../provider, and of jaws' registerMode=instance consumers.
//
// The only sweet code between the stub and Nacos is the applevel.Consumer
// decorator: Watch reads the interface → application mapping from the config
// center, subscribes to the ONE application instance, pulls its revision's
// metadata over gRPC (StringValue envelope, the channel jaws-wire speaks), and
// expands it back into interface-level instances. client.Dial, the resolver and
// the call site are the same lines as any interface-level example.
//
// -target picks the interoperability direction:
//
//   - jaws (default): consume jaws-sample-wire-boot-provider, which runs with
//     register-mode: instance. Start it first, from the jaws repository:
//     mvn -pl jaws-samples/jaws-sample-wire-boot-provider spring-boot:run
//     The lookup name is the Java interface FQN (that is what jaws' mapping is
//     keyed by); the stub still calls /greeter.Greeter/… because jaws indexes
//     both names server-side — the same fact example/jaws-wire relies on.
//
//   - sweet: consume ../provider (or any applevel.Provider). The lookup name is
//     the proto's package.Service, and all four method shapes run in the same
//     order and line format as jaws-sample-wire-sweet's SweetGreeterConsumer,
//     so the two consumers' output can be compared line by line.
//
// No mapping present? Consumer passes through to interface-level discovery, so
// this binary also calls example/greeter's classic server with -target=sweet
// -namespace= — the mode decision lives in the config
// center, not in a flag, exactly as jaws' ConsumerCoordinator decides.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/applevel"
	"github.com/javahongxi/sweet/client"
	greeterpb "github.com/javahongxi/sweet/example/greeter/greeterpb"
	wirepb "github.com/javahongxi/sweet/example/jaws-wire/greeterpb"
	"github.com/javahongxi/sweet/nacos"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
)

func main() {
	target := flag.String("target", "jaws", "jaws | sweet — which provider to consume")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	group := flag.String("group", "default_rpc", "Nacos group the application instance registered under")
	namespace := flag.String("namespace", "jaws",
		"prefix of the application name to subscribe: jaws matches providers registered with "+
			"-namespace=jaws (and jaws' own default), empty watches the bare name")
	resolve := flag.String("resolve", "",
		"interface name to look up; empty picks per target: the Java interface FQN for jaws, "+
			"sweet.example.v1.Greeter for sweet")
	name := flag.String("name", "sweet", "name to greet")
	times := flag.Int("times", 3, "frames for the server-streaming method")
	flag.Parse()

	if *resolve == "" {
		switch *target {
		case "jaws":
			*resolve = "org.hongxi.jaws.sample.wire.proto.GreeterService"
		case "sweet":
			*resolve = "sweet.example.v1.Greeter"
		default:
			log.Fatalf("unknown -target %q (jaws | sweet)", *target)
		}
	}

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

	consumer := applevel.NewConsumer(nr, cc, applevel.ConsumerOptions{
		Namespace: *namespace,
		Logger:    log.Default(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Ask the decorator first, so the addresses the call is about to use are
	// printed rather than assumed — and an empty expansion fails here with a
	// hint instead of later as "produced zero addresses".
	ch, err := consumer.Watch(ctx, *resolve)
	if err != nil {
		log.Fatalf("watch %s: %v", *resolve, err)
	}
	select {
	case insts := <-ch:
		if len(insts) == 0 {
			log.Fatalf("no instance of %s: is the provider running, and do -namespace/-group "+
				"match how it registered? (a mapping routes to the application instance; "+
				"without one the lookup falls back to the interface name)", *resolve)
		}
		log.Printf("resolved %s through application-level discovery: %v", *resolve, insts)
	case <-time.After(10 * time.Second):
		log.Fatalf("no instance list for %s within 10s (provider up? mapping published?)", *resolve)
	}

	conn, err := client.Dial(consumer, *resolve,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	switch *target {
	case "jaws":
		callJaws(ctx, conn, *name)
	case "sweet":
		callSweet(ctx, conn, *name, *times)
	}
}

// callJaws exercises the two shapes example/jaws-wire uses against the boot
// provider: unary plus server streaming, both through the stock stub.
func callJaws(ctx context.Context, conn *grpc.ClientConn, name string) {
	cli := wirepb.NewGreeterClient(conn)
	log.Printf("calling %s", wirepb.Greeter_SayHello_FullMethodName)

	reply, err := cli.SayHello(ctx, &wirepb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("SayHello: %v", err)
	}
	log.Printf("unary  SayHello        → %q", reply.GetMessage())

	stream, err := cli.SayHelloStream(ctx, &wirepb.HelloRequest{Name: name})
	if err != nil {
		log.Fatalf("SayHelloStream: %v", err)
	}
	for {
		m, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("stream Recv: %v", err)
		}
		log.Printf("stream SayHelloStream → %q", m.GetMessage())
	}
}

// callSweet runs all four method shapes in SweetGreeterConsumer's order with
// its line format, so a jaws consumer and this one against the same provider
// produce comparable output.
func callSweet(ctx context.Context, conn *grpc.ClientConn, name string, times int) {
	cli := greeterpb.NewGreeterClient(conn)
	names := []string{"alice", "bob", "charlie"}

	reply, err := cli.Greet(ctx, &greeterpb.GreetRequest{Name: name})
	if err != nil {
		log.Fatalf("Greet: %v", err)
	}
	fmt.Printf("Greet       <= %q\n", reply.GetMessage())

	stream, err := cli.GreetStream(ctx, &greeterpb.GreetRequest{Name: name, Times: int32(times)})
	if err != nil {
		log.Fatalf("GreetStream: %v", err)
	}
	for i := 1; ; i++ {
		m, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("GreetStream Recv: %v", err)
		}
		fmt.Printf("GreetStream <= %d %q\n", i, m.GetMessage())
	}

	chat, err := cli.GreetChat(ctx)
	if err != nil {
		log.Fatalf("GreetChat: %v", err)
	}
	for _, n := range names {
		if err := chat.Send(&greeterpb.GreetRequest{Name: n}); err != nil {
			log.Fatalf("GreetChat Send: %v", err)
		}
	}
	if err := chat.CloseSend(); err != nil {
		log.Fatalf("GreetChat CloseSend: %v", err)
	}
	for i := 1; ; i++ {
		m, err := chat.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("GreetChat Recv: %v", err)
		}
		fmt.Printf("GreetChat   <= %d %q\n", i, m.GetMessage())
	}

	all, err := cli.GreetAll(ctx)
	if err != nil {
		log.Fatalf("GreetAll: %v", err)
	}
	for _, n := range names {
		if err := all.Send(&greeterpb.GreetRequest{Name: n}); err != nil {
			log.Fatalf("GreetAll Send: %v", err)
		}
	}
	joined, err := all.CloseAndRecv()
	if err != nil {
		log.Fatalf("GreetAll CloseAndRecv: %v", err)
	}
	fmt.Printf("GreetAll    <= %q\n", joined.GetMessage())
}
