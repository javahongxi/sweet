// Command jaws-wire-client calls jaws' wire provider — a Java gRPC server we do
// not control — through sweet's own dial path.
//
// Both modes aim at the same provider, jaws' Spring Boot one, and the flag picks
// only how its address is obtained:
//
//   - -registry=direct (default): host:port from the command line. This isolates
//     the wire-format interoperability claim — nothing else is in play.
//   - -registry=nacos: the address comes from Nacos, where that provider published
//     itself. Two names then differ and both matter: jaws registers under
//     "jaws/<interface FQN>" in group default_rpc, while the stub calls
//     /greeter.Greeter/… — -resolve picks the lookup name. That the call still
//     lands is jaws indexing both names server-side, not luck.
//
// Everything past the address is the stock generated stub, which is the other half
// of the point: a caller written the normal way gets sweet's resolver, its
// graceful lifecycle and its connection semantics without changing a line of the
// call site.
//
// Start the provider first, from /Users/hongxi/dev/jaws:
//
//	mvn -pl jaws-samples/jaws-sample-wire-boot-provider spring-boot:run
//
// It listens on 50051 and registers into Nacos on 127.0.0.1:8848, so Nacos has to
// be up for -registry=nacos.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/example/jaws-wire/greeterpb"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "jaws wire provider address, used by -registry=direct")
	registryKind := flag.String("registry", "nacos", "direct | nacos")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	group := flag.String("group", "default_rpc", "Nacos group the provider registered under")
	resolve := flag.String("resolve", "",
		"service name to look up in the registry; empty picks per mode: nacos needs the name "+
			"jaws registered under (jaws/<interface FQN>), direct ignores it")
	name := flag.String("name", "sweet", "name to greet")
	flag.Parse()

	// The gRPC service name the stub calls is fixed by greeter.proto (see the
	// generated *_FullMethodName constants below); the lookup name is a separate
	// string, and defaulting it per mode is what makes a bare `go run` work in
	// either mode instead of only with the flags spelled out.
	if *resolve == "" {
		if *registryKind == "nacos" {
			*resolve = "jaws/org.hongxi.jaws.sample.wire.proto.GreeterService"
		} else {
			*resolve = "greeter.Greeter"
		}
	}
	var (
		reg registry.Registry
		nr  *nacosregistry.Registry
	)
	switch *registryKind {
	case "direct":
		dr, err := direct.New(*addr)
		if err != nil {
			log.Fatalf("registry: %v", err)
		}
		reg = dr
	case "nacos":
		var err error
		nr, err = nacosregistry.New(nacosregistry.Options{ServerAddr: *serverAddr, GroupName: *group})
		if err != nil {
			log.Fatalf("registry: %v", err)
		}
		reg = nr
	default:
		log.Fatalf("unknown -registry %q", *registryKind)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if nr != nil {
		// Ask the registry, so the address the call is about to use is printed
		// rather than assumed. Watch, not Query: the connection is lazy, and Query
		// only reports what an existing connection can see. The first snapshot is
		// what the subscribe response itself carries.
		ch, err := nr.Watch(ctx, *resolve)
		if err != nil {
			log.Fatalf("watch %s (group %s): %v", *resolve, *group, err)
		}
		select {
		case insts := <-ch:
			if len(insts) == 0 {
				// Better than letting it die later as "produced zero addresses":
				// an empty list means either the lookup name/group is not what the
				// provider registered under, or nothing is published at all.
				log.Fatalf("no instance of %s in group %s: is the provider running, "+
					"and is -resolve its registry name rather than the gRPC service name? "+
					"(jaws registers as jaws/<interface FQN>)", *resolve, *group)
			}
			log.Printf("resolved %s in group %s from nacos: %v", *resolve, *group, insts)
		case <-time.After(10 * time.Second):
			log.Fatalf("nacos gave no instance list for %s (group %s) within 10s", *resolve, *group)
		}
	}

	conn, err := client.Dial(reg, *resolve,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	cli := greeterpb.NewGreeterClient(conn)

	// The path the stub puts on the wire, read from the generated constant rather
	// than retyped here — if the proto ever renames, this line says so.
	log.Printf("calling %s against %s found as %q", greeterpb.Greeter_SayHello_FullMethodName,
		*registryKind, *resolve)

	reply, err := cli.SayHello(ctx, &greeterpb.HelloRequest{Name: *name})
	if err != nil {
		log.Fatalf("SayHello (provider up? -registry=%s reached %s?): %v", *registryKind, *resolve, err)
	}
	log.Printf("unary  SayHello        → %q", reply.GetMessage())

	stream, err := cli.SayHelloStream(ctx, &greeterpb.HelloRequest{Name: *name})
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
