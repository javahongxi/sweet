package nacos_test

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestKindOfBucketsFailures exercises the drill's tolerance rule directly. The
// drill itself only reaches these branches when something actually fails, so a
// green run with zero failures proves nothing about the classifier — and a
// classifier that lumps discovery-empty into the tolerated bucket would hide the
// exact bug the drill is meant to catch.
func TestKindOfBucketsFailures(t *testing.T) {
	// Each case is wrapped twice on the way up, the way registry/ and server/
	// wrap what grpc-go returns, because a classifier that only works on a bare
	// status error is not the one the drill uses.
	wrap := func(err error) error {
		return fmt.Errorf("sweet: ping: %w", fmt.Errorf("nacos: %s: %w", "call", err))
	}
	tests := []struct {
		name string
		err  error
		want failKind
	}{
		{
			name: "resolver had not caught up",
			err:  status.Error(codes.Unavailable, "last resolution error: produced zero addresses"),
			want: failKind{code: codes.Unavailable, reason: "discovery-empty"},
		},
		{
			name: "call raced its address away",
			err:  status.Error(codes.Unavailable, "connection error: desc = \"transport: Error while dialing: connection refused\""),
			want: failKind{code: codes.Unavailable, reason: "dead-peer"},
		},
		{
			name: "in flight when the peer drained",
			err:  status.Error(codes.Unavailable, "Subchannel Stream terminated"),
			want: tolerated,
		},
		{
			name: "caller gave up",
			err:  status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			want: failKind{code: codes.DeadlineExceeded, reason: "in-flight"},
		},
		{
			name: "not a status at all",
			err:  errors.New("sweet: registry not ready"),
			want: failKind{code: codes.OK, reason: "not-a-status"},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := kindOf(wrap(tc.err)); got != tc.want {
				t.Fatalf("kindOf(%v) = %+v, want %+v", tc.err, got, tc.want)
			}
		})
	}
}
