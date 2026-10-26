// Command jaws-wire-client calls jaws' wire provider — a Java gRPC server we
// do not control — through sweet's own dial path.
//
// The provider sample registers with no registry (jaws' WireProvider direct
// exports), so the default here is the direct registry: discovery is exercised
// separately in example/helloworld. Everything past the address is the stock
// generated stub, which is the point: a caller written the normal way gets
// sweet's resolver, its graceful lifecycle and its connection semantics without
// changing a line of the call site.
//
// Start the provider first, from /Users/hongxi/dev/jaws:
//
//	./run-sample.sh wire        # one-shot pair, or run WireProvider standalone on 50051
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
	"github.com/javahongxi/sweet/registry/direct"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "jaws wire provider address")
	name := flag.String("name", "sweet", "name to greet")
	flag.Parse()

	// The gRPC service name comes from greeter.proto's package + service, i.e.
	// the same string jaws serves; no second identifier is introduced.
	const service = "greeter.Greeter"

	reg, err := direct.New(*addr)
	if err != nil {
		log.Fatalf("registry: %v", err)
	}
	conn, err := client.Dial(reg, service,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cli := greeterpb.NewGreeterClient(conn)

	reply, err := cli.SayHello(ctx, &greeterpb.HelloRequest{Name: *name})
	if err != nil {
		log.Fatalf("SayHello (is the provider up on %s?): %v", *addr, err)
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
