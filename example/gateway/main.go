// Command gateway puts an HTTP edge in front of a sweet service.
//
// The provider is example/greeter's own server, registered into Nacos by hand —
// nothing about it changes for this to work, which is the point: the gateway is
// a consumer of the same registry abstraction the clients use, not a new mode
// the server has to know about.
//
// Unary and server-streaming methods are both exposed. Client streaming and
// bidi are not: they need a duplex transport (WebSocket, or SSE plus a second
// POST), and a half-built duplex endpoint is worse than an absent one.
//
// The route table can also come from Nacos' config domain, which is the point of
// -routes=nacos: the mapping HTTP path → gRPC method becomes data an operator
// edits, while the message types stay compiled Go (Options.Services). The
// gateway then applies a new table without a restart, and refuses a bad one
// without touching the live one.
//
// Run it after starting the provider, e.g.
//
//	go run ./example/greeter/cmd/server -registry=nacos -server 127.0.0.1:9848 -listen 127.0.0.1:20001
//	go run ./example/gateway
//
// then:
//
//	curl -s localhost:20010/v1/greeter/greet -d '{"name":"web"}'
//	curl -sN localhost:20010/v1/greeter/greet-stream -d '{"name":"web","times":3}'
//
// To drive the table from a config centre instead (see doc/nacos-config-routes.md
// for the format and the contract it rests on):
//
//	curl -s -X POST 'http://127.0.0.1:8848/nacos/v1/cs/configs' \
//	  -d dataId=sweet-gateway-routes.json -d group=DEFAULT_GROUP \
//	  --data-urlencode 'content={"routes":[{"method":"POST","path":"/v1/greeter/greet",
//	    "service":"sweet.example.v1.Greeter","fullMethod":"/sweet.example.v1.Greeter/Greet"}]}'
//	go run ./example/gateway -routes=nacos
//
// and publishing a second table while it runs swaps the routes in place.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
	"github.com/javahongxi/sweet/gateway"
	"github.com/javahongxi/sweet/nacos"
	"github.com/javahongxi/sweet/registry"
	"github.com/javahongxi/sweet/registry/direct"
	nacosregistry "github.com/javahongxi/sweet/registry/nacos"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:20010", "address to listen on")
	registryKind := flag.String("registry", "nacos", "nacos | direct")
	serverAddr := flag.String("server", "127.0.0.1:9848", "Nacos SDK gRPC address (mainPort+1000)")
	endpoints := flag.String("endpoints", "127.0.0.1:20001", "provider addresses, used only with -registry=direct")
	service := flag.String("service", pb.Greeter_ServiceDesc.ServiceName,
		"name to look up in the registry — a lookup key, independent of the gRPC method below")
	timeout := flag.Duration("timeout", 2*time.Second,
		"bound applied when the HTTP caller set no deadline of its own")
	verbose := flag.Bool("verbose-errors", false, "surface the provider's status message at the edge")
	routesFrom := flag.String("routes", "embedded", "embedded | nacos — where the route table comes from")
	routesDataID := flag.String("routes.dataId", "sweet-gateway-routes.json", "config dataId holding the route table")
	routesGroup := flag.String("routes.group", "DEFAULT_GROUP", "config group of the route table")
	namespace := flag.String("namespace", "", "Nacos namespace (the config domain calls it tenant)")
	printTable := flag.Bool("print-status-table", false, "print the gRPC to HTTP mapping and exit")
	flag.Parse()

	if *printTable {
		for _, line := range gateway.StatusTableLines() {
			log.Println(line)
		}
		return
	}

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

	// Credentials are stated rather than defaulted: a gateway that silently
	// dialled its backends in plaintext would hide that choice in the one place
	// nobody looks. This example is local, so plaintext is what it says.
	gw, err := gateway.New(gateway.Options{
		Registry:       reg,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		RequestTimeout: *timeout,
		VerboseErrors:  *verbose,
		// The compiled half of a config-driven table: what a route in Nacos may
		// name. Only the two methods this gateway can actually serve are listed,
		// so a table asking for client streaming is refused at apply time with
		// "method not registered" rather than half-served as a unary call.
		Services: map[string]gateway.ServiceTypes{
			pb.Greeter_ServiceDesc.ServiceName: {
				"Greet": {
					Request: func() proto.Message { return &pb.GreetRequest{} },
					Reply:   func() proto.Message { return &pb.GreetReply{} },
				},
				"GreetStream": {
					Request: func() proto.Message { return &pb.GreetRequest{} },
					Reply:   func() proto.Message { return &pb.GreetReply{} },
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("gateway.New: %v", err)
	}

	switch *routesFrom {
	case "embedded":
		// One route per gRPC method. FullMethod comes from the generated constant,
		// so the wire spelling cannot drift from the IDL; -service only names the
		// key the registry is asked about, and the two are free to differ.
		routes := []gateway.Route{{
			Method:     http.MethodPost,
			Path:       "/v1/greeter/greet",
			Service:    *service,
			FullMethod: pb.Greeter_Greet_FullMethodName,
			NewRequest: func() proto.Message { return &pb.GreetRequest{} },
			NewReply:   func() proto.Message { return &pb.GreetReply{} },
		}, {
			// The server-streaming method answers many messages, so the HTTP side is
			// one JSON object per line rather than one body. Watch it with
			// curl -N (--no-buffer), or any reader that takes one object at a time.
			Method:       http.MethodPost,
			Path:         "/v1/greeter/greet-stream",
			Service:      *service,
			FullMethod:   pb.Greeter_GreetStream_FullMethodName,
			ServerStream: true,
			NewRequest:   func() proto.Message { return &pb.GreetRequest{} },
			NewReply:     func() proto.Message { return &pb.GreetReply{} },
		}}
		for _, r := range routes {
			if err := gw.Handle(r); err != nil {
				log.Fatalf("route %s %s: %v", r.Method, r.Path, err)
			}
			kind := "unary"
			if r.ServerStream {
				kind = "server stream"
			}
			log.Printf("route: %s %s -> %s (%s) via %s registry", r.Method, r.Path, r.FullMethod, kind, reg.Name())
		}

	case "nacos":
		// A second SDK connection, deliberately: Nacos labels connections by
		// module and its naming module ignores anything not labelled "naming"
		// (ConnectionBasedClientManager.java:60-64), so config cannot ride on the
		// registry's connection. Within this domain, query and listen do share it.
		cfg, err := nacos.NewConfigClient(nacos.ConfigOptions{
			Addr:   *serverAddr,
			Tenant: *namespace,
			OnError: func(key nacos.ConfigKey, err error) {
				log.Printf("config %s/%s: %v (keeping the live route table)", key.Group, key.DataID, err)
			},
		})
		if err != nil {
			log.Fatalf("config client %s: %v", *serverAddr, err)
		}
		defer func() { _ = cfg.Close() }()
		log.Printf("config: connected to %s as %s (its own SDK connection; the %s registry keeps a separate one)",
			*serverAddr, cfg.ConnectionID(), reg.Name())

		var applied atomic.Bool
		key := nacos.ConfigKey{DataID: *routesDataID, Group: *routesGroup}
		watch := func(c nacos.Config) {
			specs, err := gateway.ParseRouteSpecs([]byte(c.Content))
			if err != nil {
				log.Printf("routes: table from %s/%s rejected: %v — live table untouched", key.Group, key.DataID, err)
				return
			}
			if err := gw.ApplyRoutes(specs); err != nil {
				log.Printf("routes: apply failed: %v — live table untouched", err)
				return
			}
			applied.Store(true)
			log.Printf("routes: applied %d route(s) from %s/%s (md5 %s)", len(specs), key.Group, key.DataID, c.MD5)
			for _, r := range gw.Routes() {
				log.Printf("  %s %s -> %s via %s", r.Method, r.Path, r.FullMethod, r.Service)
			}
		}
		if err := cfg.Watch(context.Background(), key, watch); err != nil {
			log.Fatalf("watch %s/%s: %v", key.Group, key.DataID, err)
		}
		if !applied.Load() {
			log.Printf("no route table yet in %s/%s — the edge answers 404 until one is published",
				key.Group, key.DataID)
		}

	default:
		log.Fatalf("unknown -routes %q (embedded | nacos)", *routesFrom)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- gw.ListenAndServe(*listen) }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		// A bind failure surfaces here, before any signal, so the process does
		// not look healthy when it never started.
		log.Fatalf("serve on %s: %v", *listen, err)
	case sig := <-sigCh:
		log.Printf("signal %v, draining", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := gw.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Printf("stopped: %d route(s) served through the %s registry", len(gw.Routes()), reg.Name())
}
