package nacos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcStatus is the shape every gRPC error carries. It is reached for here
// instead of only through status.FromError because the two disagree about the
// message once an error has been wrapped — see TestFromErrorVsAsMessage.
type grpcStatus interface{ GRPCStatus() *status.Status }

// TestCauseStaysClassifiableThroughWrapping pins the convention this package
// dials by: every layer wraps with %w, so a caller at the top of the chain can
// still pull the gRPC code out with errors.As. "The server is unreachable" and
// "the reply was wrong" have to stay distinguishable after several fmt.Errorf
// hops, or the only way to tell them apart is string matching.
//
// The target is a pointer to an *interface*. A pointer to *status.Status does
// not even compile — errors.As requires a target whose type implements error or
// is an interface, and *status.Status implements neither:
//
//	second argument to errors.As must be a non-nil pointer to either a type
//	that implements error, or to any interface type
//
// so the concrete form is off the table, not merely discouraged.
func TestCauseStaysClassifiableThroughWrapping(t *testing.T) {
	// Two more %w hops on top of the one Unary already does, standing in for
	// what registry/ and server/ add; a chain of one proves nothing.
	top := func(err error) error {
		return fmt.Errorf("sweet: register demo-provider: %w", fmt.Errorf("nacos: %s: %w", "RegisterInstance", err))
	}

	tests := []struct {
		name string
		// call runs one unary request and returns its error.
		call func(ctx context.Context, c *Conn) error
		want codes.Code
	}{
		{
			name: "peer unreachable",
			call: func(ctx context.Context, c *Conn) error {
				_, _, err := c.Unary(ctx, HealthCheckRequest{RequestID: "1"})
				return err
			},
			want: codes.Unavailable, // port 1: nothing listens, connect is refused
		},
		{
			name: "caller gave up",
			call: func(ctx context.Context, c *Conn) error {
				cctx, cancel := context.WithCancel(ctx)
				cancel() // cancelled before the call even starts
				_, _, err := c.Unary(cctx, HealthCheckRequest{RequestID: "1"})
				return err
			},
			want: codes.Canceled,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c, err := Dial("127.0.0.1:1")
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer c.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = tc.call(ctx, c)
			if err == nil {
				t.Fatal("expected a failure, got none")
			}
			err = top(err)

			var gs grpcStatus
			if !errors.As(err, &gs) {
				t.Fatalf("errors.As found no gRPC status in %v", err)
			}
			if got := gs.GRPCStatus().Code(); got != tc.want {
				t.Fatalf("code through the wrapping chain = %v, want %v", got, tc.want)
			}
			// The package-qualified form is what an error message looks like on
			// the way up; if it ever stops containing the request class, the
			// wrapping has gotten lossier than the code alone can show.
			if !strings.Contains(err.Error(), "HealthCheckRequest") {
				t.Fatalf("wrapper dropped the request class: %v", err)
			}
		})
	}

	// The other half of the contract: a local failure must not classify as a
	// status, or a bug in this package reads as "server unreachable".
	var gs grpcStatus
	if errors.As(errors.New("nacos: not ready"), &gs) {
		t.Fatal("a plain error classified as a gRPC status")
	}
}

// TestCancelledRPCIsNotSentinelReachable records a limit of errors.Is: gRPC
// turns a cancelled context into a status whose *code* is Canceled and does not
// put context.Canceled in the Unwrap chain, so the sentinel check that works for
// io.EOF or a hand-rolled errNotReady finds nothing here. Map the code.
func TestCancelledRPCIsNotSentinelReachable(t *testing.T) {
	c, err := Dial("127.0.0.1:1")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = c.Unary(ctx, HealthCheckRequest{RequestID: "1"})
	if err == nil {
		t.Fatal("expected a failure on a cancelled context")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("this build chains context.Canceled through Unwrap; the assertion below is now redundant, not wrong")
	}

	var gs grpcStatus
	if !errors.As(err, &gs) {
		t.Fatalf("errors.As found no gRPC status in %v", err)
	}
	if got := gs.GRPCStatus().Code(); got != codes.Canceled {
		t.Fatalf("code = %v, want Canceled", got)
	}
}

// TestFromErrorVsAsMessage compares the two ways out of a wrapped gRPC error.
// status.FromError keeps the inner *code* but re-reports the outer chain's text
// as the message; errors.As hands back the inner status untouched. Code
// comparisons agree either way, message comparisons do not — so classify on the
// code, and treat the message as human-facing.
func TestFromErrorVsAsMessage(t *testing.T) {
	c, err := Dial("127.0.0.1:1")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = c.Unary(ctx, HealthCheckRequest{RequestID: "1"})
	if err == nil {
		t.Fatal("expected a failure on a cancelled context")
	}
	wrapped := fmt.Errorf("registry: %w", err)

	st, ok := status.FromError(wrapped)
	if !ok {
		t.Fatalf("status.FromError(%v) reported no status", wrapped)
	}
	var gs grpcStatus
	if !errors.As(wrapped, &gs) {
		t.Fatalf("errors.As found no gRPC status in %v", wrapped)
	}
	inner := gs.GRPCStatus()

	if st.Code() != inner.Code() {
		t.Fatalf("codes disagree: FromError %v vs As %v", st.Code(), inner.Code())
	}
	if inner.Message() != "context canceled" {
		t.Fatalf("message from errors.As = %q, want the transport's own text", inner.Message())
	}
	if !strings.HasPrefix(st.Message(), "registry: nacos: call ") {
		t.Fatalf("message from FromError = %q, want the whole chain's text", st.Message())
	}
}
