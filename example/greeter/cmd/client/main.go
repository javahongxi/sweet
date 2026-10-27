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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/client"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
)

func main() {
	registryKind := flag.String("registry", "nacos", "nacos | direct")
	endpoints := flag.String("endpoints", "127.0.0.1:20001", "comma-separated host:port, used by direct")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	service := flag.String("service", pb.Greeter_ServiceDesc.ServiceName, "service name to resolve")
	name := flag.String("name", "sweet", "name to greet")
	times := flag.Int("times", 3, "how many greetings to ask the stream for")
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

	cli := pb.NewGreeterClient(conn)

	reply, err := cli.Greet(ctx, &pb.GreetRequest{Name: *name})
	if err != nil {
		log.Fatalf("Greet: %v", err)
	}

	stream, err := cli.GreetStream(ctx, &pb.GreetRequest{Name: *name, Times: int32(*times)})
	if err != nil {
		log.Fatalf("GreetStream: %v", err)
	}
	var hellos []string
	for {
		// Recv returns io.EOF when the server closes the stream, which is the
		// hand-written client's only real extra work: ping.Events hides this loop.
		m, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("GreetStream Recv: %v", err)
		}
		hellos = append(hellos, m.GetMessage())
	}

	log.Printf("resolved %s through %s registry → Greet=%q GreetStream=%v",
		*service, reg.Name(), reply.GetMessage(), hellos)
}
