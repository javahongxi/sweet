// Command bench measures sweet's gRPC path with the same shape as jaws'
// benchmark suite (jaws doc/benchmark.md): closed loop, fixed concurrency,
// warmup discarded, latencies merged and percentiled, provider and consumer in
// separate processes (jaws' ROLE=provider / ROLE=consumer mode).
//
// Two arms, and the pair is the point:
//
//	-server native  -client native   stock grpc-go on both ends: the baseline
//	-server sweet   -client sweet    sweet's server.Register + client.Dial path
//
// Subtracting one from the other is sweet's own per-call cost. Comparing either
// against jaws' table compares whole stacks — language runtime, network library,
// protocol and serialization all change at once — so such a number must be
// labelled stack-vs-stack, never "protocol vs protocol".
//
// Usage:
//
//	go run ./bench -mode provider -server sweet -port 50051
//	go run ./bench -mode consumer -client sweet -addr 127.0.0.1:50051 \
//	    -threads 20 -warmup 5 -duration 40
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/javahongxi/sweet/client"
	pb "github.com/javahongxi/sweet/example/jaws-wire/greeterpb"
	"github.com/javahongxi/sweet/registry/direct"
	"github.com/javahongxi/sweet/server"
)

// benchmarkName matches jaws' GrpcBenchmark/WireBenchmark payload, so the bytes
// on the wire are the same size on both sides of the comparison.
const benchmarkName = "benchmark"

func main() {
	log.SetFlags(log.Ltime)
	mode := flag.String("mode", "provider", "provider | consumer")
	serverKind := flag.String("server", "sweet", "provider side: sweet | native")
	clientKind := flag.String("client", "sweet", "consumer side: sweet | native")
	addr := flag.String("addr", "127.0.0.1:50051", "consumer: provider address")
	host := flag.String("host", "127.0.0.1", "provider: listen host")
	port := flag.String("port", "50051", "provider: listen port")
	threads := flag.Int("threads", 20, "closed-loop concurrency (jaws: THREADS)")
	warmup := flag.Int("warmup", 5, "warmup seconds, discarded (jaws: WARMUP)")
	duration := flag.Int("duration", 40, "measured seconds (jaws: DURATION)")
	flag.Parse()

	switch *mode {
	case "provider":
		runProvider(*serverKind, *host+":"+*port)
	case "consumer":
		runConsumer(*clientKind, *addr, *threads, *warmup, *duration)
	default:
		log.Fatalf("unknown -mode %q", *mode)
	}
}

// greeter answers SayHello only; UnimplementedGreeterServer covers the other
// three shapes. That embedding is also a standing reminder that a misnamed
// method would compile and degrade to UNIMPLEMENTED at runtime instead of
// failing the build — see sweet's example/greeter README.
type greeter struct{ pb.UnimplementedGreeterServer }

func (greeter) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello, " + in.GetName() + "! (from sweet-bench)"}, nil
}

func runProvider(kind, listen string) {
	switch kind {
	case "native":
		lis, err := net.Listen("tcp", listen)
		if err != nil {
			log.Fatalf("listen %s: %v", listen, err)
		}
		srv := grpc.NewServer()
		pb.RegisterGreeterServer(srv, greeter{})
		log.Printf("provider: stock grpc-go on %s (pid %d)", lis.Addr(), os.Getpid())
		if err := srv.Serve(lis); err != nil {
			log.Fatalf("serve: %v", err)
		}
	case "sweet":
		srv, err := server.New(server.Options{
			Address:     listen,
			ServiceName: pb.Greeter_ServiceDesc.ServiceName,
			// Registry is deliberately not in the hot path here: the point of the
			// native/sweet pair is the RPC path, and the restart drill already
			// covers discovery.
			Registry: direct.Nope(),
		})
		if err != nil {
			log.Fatalf("server.New: %v", err)
		}
		if err := srv.Register(&pb.Greeter_ServiceDesc, greeter{}); err != nil {
			log.Fatalf("Register: %v", err)
		}
		go func() {
			if err := srv.Serve(); err != nil {
				log.Printf("serve: %v", err)
			}
		}()
		if err := srv.Listen(); err != nil {
			log.Fatalf("Listen: %v", err)
		}
		log.Printf("provider: sweet server on %s (pid %d)", srv.Addr(), os.Getpid())
		// Same contract as the examples: Ctrl-C must reach Stop(), otherwise the
		// instance vanishes without deregistering.
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
		<-stop
		log.Printf("signal received, stopping")
		srv.Stop()
	default:
		log.Fatalf("unknown -server %q", kind)
	}
}

func runConsumer(kind, addr string, threads, warmup, duration int) {
	newClient, closeConn, err := dialer(kind, addr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer closeConn()

	fmt.Println("=== sweet bench (consumer) ===")
	fmt.Printf("runtime      : %s %s/%s, GOMAXPROCS=%d\n",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))
	fmt.Printf("arms         : client=%s addr=%s\n", kind, addr)
	fmt.Printf("params       : threads=%d warmup=%ds duration=%ds\n", threads, warmup, duration)
	fmt.Printf(`payload      : HelloRequest{name=%q}`, benchmarkName)
	fmt.Println()

	w := run(threads, warmup, newClient)
	r := run(threads, duration, newClient)
	fmt.Printf("(warmup: %s calls discarded)\n", group(w.calls))
	printResult(r, duration)
}

type sample struct {
	calls   int64
	errs    int64
	lat     []time.Duration
	elapsed time.Duration
}

// run drives `threads` closed-loop callers for `seconds`. Every caller dials
// first and they start together, so a slow connect does not eat measurement
// time from the threads that happened to connect fast.
func run(threads, seconds int, newClient func() pb.GreeterClient) sample {
	var (
		stop   int32
		wg     sync.WaitGroup
		perT   = make([][]time.Duration, threads)
		errs   int64
		calls  int64
		loaded sync.WaitGroup
	)
	loaded.Add(threads)
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			cli := newClient()
			lat := make([]time.Duration, 0, 1<<14)
			loaded.Done()
			loaded.Wait()
			for atomic.LoadInt32(&stop) == 0 {
				t0 := time.Now()
				if _, err := cli.SayHello(context.Background(), &pb.HelloRequest{Name: benchmarkName}); err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				lat = append(lat, time.Since(t0))
				atomic.AddInt64(&calls, 1)
			}
			perT[slot] = lat
		}(i)
	}
	start := time.Now()
	time.Sleep(time.Duration(seconds) * time.Second)
	atomic.StoreInt32(&stop, 1)
	wg.Wait()
	elapsed := time.Since(start)

	out := sample{calls: atomic.LoadInt64(&calls), errs: atomic.LoadInt64(&errs), elapsed: elapsed}
	for _, l := range perT {
		out.lat = append(out.lat, l...)
	}
	sort.Slice(out.lat, func(i, j int) bool { return out.lat[i] < out.lat[j] })
	return out
}

func printResult(s sample, seconds int) {
	if len(s.lat) == 0 {
		log.Fatalf("no successful call measured (%d errors)", s.errs)
	}
	var sum time.Duration
	for _, d := range s.lat {
		sum += d
	}
	pct := func(p float64) time.Duration {
		i := int(p / 100 * float64(len(s.lat)))
		if i >= len(s.lat) {
			i = len(s.lat) - 1
		}
		return s.lat[i]
	}
	us := func(d time.Duration) string { return fmt.Sprintf("%s us", groupI(int64(d.Seconds()*1e6))) }
	qps := float64(len(s.lat)) / s.elapsed.Seconds()

	fmt.Println("--- result ---")
	fmt.Printf("  Total calls : %s\n", groupI(int64(len(s.lat))))
	fmt.Printf("  Errors      : %s\n", groupI(s.errs))
	fmt.Printf("  Elapsed     : %.2fs\n", s.elapsed.Seconds())
	fmt.Printf("  QPS         : %s\n", groupI(int64(qps)))
	fmt.Printf("  Avg         : %s\n", us(sum/time.Duration(len(s.lat))))
	fmt.Printf("  P50         : %s\n", us(pct(50)))
	fmt.Printf("  P99         : %s\n", us(pct(99)))
	fmt.Printf("  P99.9       : %s\n", us(pct(99.9)))
	fmt.Printf("  Max         : %s\n", us(s.lat[len(s.lat)-1]))
}

func dialer(kind, addr string) (func() pb.GreeterClient, func(), error) {
	switch kind {
	case "native":
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return func() pb.GreeterClient { return pb.NewGreeterClient(conn) }, func() { _ = conn.Close() }, nil
	case "sweet":
		reg, err := direct.New(addr)
		if err != nil {
			return nil, nil, err
		}
		conn, err := client.Dial(reg, pb.Greeter_ServiceDesc.ServiceName,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		return func() pb.GreeterClient { return pb.NewGreeterClient(conn) }, func() { _ = conn.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unknown -client %q", kind)
	}
}

// group renders thousands separators; Go's fmt has no %,.d like Java's %,d.
func group(n int64) string { return groupI(n) }

func groupI(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
