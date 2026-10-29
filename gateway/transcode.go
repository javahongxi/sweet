package gateway

import (
	"bytes"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// maxRequestBytes bounds how much body one call will read.
//
// A gateway is reachable by whoever can reach it, so an unbounded read is a
// denial-of-service invitation written into the happy path. 1 MiB is a
// deliberate, boring default for a JSON-to-protobuf edge; the number matters
// less than the fact that there is one.
const maxRequestBytes = 1 << 20

// decodeRequest fills msg from a JSON body.
//
// An empty body means "all fields default", and it is answered rather than
// rejected: a provider method that takes no required fields is a real shape,
// and forcing every curl invocation to type "{}" is ceremony.
func decodeRequest(body io.Reader, msg proto.Message, allowUnknown bool) error {
	data, err := io.ReadAll(io.LimitReader(body, maxRequestBytes+1))
	if err != nil {
		return status.Error(codes.InvalidArgument, "read body: "+err.Error())
	}
	if len(data) > maxRequestBytes {
		// HTTP would call this 413. gRPC has no code for "your body was too
		// big", so it surfaces as InvalidArgument (400) with the limit stated
		// in the message rather than inventing a status this package owns.
		return status.Error(codes.InvalidArgument, "request body exceeds the 1 MiB limit")
	}
	if len(bytes.TrimSpace(data)) == 0 {
		data = []byte("{}")
	}
	opts := protojson.UnmarshalOptions{DiscardUnknown: allowUnknown}
	if err := opts.Unmarshal(data, msg); err != nil {
		return status.Error(codes.InvalidArgument, "decode JSON: "+err.Error())
	}
	return nil
}

// marshalReply renders a reply message as JSON.
//
// Three choices, each with a reason:
//
//   - UseProtoNames keeps the JSON field names identical to the .proto file.
//     protojson's default converts snake_case to camelCase, which is a rename
//     nobody can see in the IDL — and this package's whole thesis is that
//     invisible renames are where interop breaks.
//   - EmitUnpopulated keeps zero-valued fields in the output. A gateway whose
//     response shape depends on whether a field happened to be zero gives
//     callers a schema that moves under them.
//   - No Indent: this is a machine endpoint.
//
// Note that protojson deliberately inserts randomized whitespace between
// fields to discourage byte-for-byte comparison of its output. Tests here
// compare decoded values, not strings — see gateway_test.go.
func marshalReply(msg proto.Message) ([]byte, error) {
	opts := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}
	return opts.Marshal(msg)
}
