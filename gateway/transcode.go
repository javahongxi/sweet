package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/dynamicpb"
)

// maxRequestBytes bounds how much body one call will read.
//
// A gateway is reachable by whoever can reach it, so an unbounded read is a
// denial-of-service invitation written into the happy path. 1 MiB is a
// deliberate, boring default for a JSON-to-protobuf edge; the number matters
// less than the fact that there is one.
const maxRequestBytes = 1 << 20

// decodeRequest assembles one request from its three HTTP binding sources.
// Body is decoded first and path parameters are written afterwards, matching
// google.api.http's override rule; query parameters only see unclaimed fields.
func decodeRequest(r *http.Request, rt Route, pathValues map[string]string, msg proto.Message, allowUnknown bool) error {
	data, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBytes+1))
	if err != nil {
		return status.Error(codes.InvalidArgument, "read body: "+err.Error())
	}
	if len(data) > maxRequestBytes {
		// HTTP would call this 413. gRPC has no code for "your body was too
		// big", so it surfaces as InvalidArgument (400) with the limit stated
		// in the message rather than inventing a status this package owns.
		return status.Error(codes.InvalidArgument, "request body exceeds the 1 MiB limit")
	}

	wholeBody := rt.Body == nil || *rt.Body == "*"
	if rt.Body == nil || *rt.Body != "" {
		body := data
		if len(bytes.TrimSpace(body)) == 0 {
			if wholeBody {
				body = []byte("{}")
			} else {
				body = nil
			}
		}
		if len(body) > 0 {
			if !wholeBody {
				body = wrapBodyField(rt.bodyFields, body)
			}
			opts := protojson.UnmarshalOptions{DiscardUnknown: allowUnknown}
			if err := opts.Unmarshal(body, msg); err != nil {
				return status.Error(codes.InvalidArgument, "decode JSON: "+err.Error())
			}
		}
	}

	if rt.pattern != nil {
		for _, segment := range rt.pattern.segments {
			if segment.field == "" {
				continue
			}
			if err := setFieldPath(msg.ProtoReflect(), segment.fields, pathValues[segment.field]); err != nil {
				return status.Errorf(codes.InvalidArgument, "decode path field %q: %v", segment.field, err)
			}
		}
	}
	if wholeBody || r.URL.RawQuery == "" {
		return nil
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "decode query: %v", err)
	}
	occupied := rt.pattern.occupied()
	if len(rt.bodyFields) > 0 {
		occupied = append(occupied, rt.bodyFields)
	}
	if err := populateQuery(msg.ProtoReflect(), values, occupied, allowUnknown); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return nil
}

// wrapBodyField uses the actual descriptors' JSON names at every level. That
// lets protojson handle scalars, lists, maps, well-known types and presence
// without converting the request through a float64-based generic JSON value.
func wrapBodyField(fields fieldPath, raw []byte) []byte {
	wrapped := raw
	for i := len(fields) - 1; i >= 0; i-- {
		name, _ := json.Marshal(fields[i].JSONName())
		next := make([]byte, 0, len(name)+len(wrapped)+3)
		next = append(next, '{')
		next = append(next, name...)
		next = append(next, ':')
		next = append(next, wrapped...)
		next = append(next, '}')
		wrapped = next
	}
	return wrapped
}

func marshalResponse(msg proto.Message, fields fieldPath) ([]byte, error) {
	if len(fields) == 0 {
		return marshalReply(msg)
	}
	current := msg.ProtoReflect()
	for _, fd := range fields[:len(fields)-1] {
		current = current.Get(fd).Message()
	}
	last := fields[len(fields)-1]

	// protojson marshals messages, not standalone scalar/list/map values. Set
	// only the selected field on a dynamic copy of its parent, then extract the
	// raw JSON value. Presence is forced so an unset optional scalar still has
	// its protobuf zero value, and int64 remains a JSON string throughout.
	wrapper := dynamicpb.NewMessage(current.Descriptor())
	wrapper.Set(last, current.Get(last))
	data, err := marshalReply(wrapper)
	if err != nil {
		return nil, err
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, fmt.Errorf("gateway: extract response field: %w", err)
	}
	selected, ok := object[string(last.Name())]
	if !ok {
		return []byte("null"), nil
	}
	return selected, nil
}

func marshalStreamResult(msg proto.Message, fields fieldPath) ([]byte, error) {
	result, err := marshalResponse(msg, fields)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 0, len(result)+11)
	frame = append(frame, `{"result":`...)
	frame = append(frame, result...)
	frame = append(frame, '}')
	return frame, nil
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
