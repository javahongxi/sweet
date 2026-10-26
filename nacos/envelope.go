// Package conn implements Nacos' client transport by hand: one bidi stream
// carries the connection handshake and server pushes, one unary RPC carries
// naming requests. The wire format is Nacos' own Payload/Metadata envelope.
//
// Facts this file encodes (local nacos checkout @ tag 3.2.3, commit c843da5be):
//
//   - api/src/main/proto/nacos_grpc_service.proto — Metadata{type=3,
//     headers=7, clientIp=8} (note: `clientIp`, camelCase on the wire),
//     Payload{metadata=2, body=3 google.protobuf.Any}.
//   - common/.../grpc/GrpcUtils.java:79-88 — Metadata.type is the request
//     class' *simple name*, clientIp is the sender's local ip, headers come
//     from the request object; the body is the request's JSON *without* its
//     headers map (GrpcUtils.java:108-113 clears them before serialising).
//   - Naming requests never call putHeader(), so for naming the headers map is
//     empty and the only mandatory fields are type + clientIp.
package nacos

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"

	"github.com/javahongxi/sweet/gen"
)

// envelope is the JSON view of a Nacos request or response: the class name
// travels in Metadata.type, everything else is the object's own fields.
type envelope interface {
	// typeName returns the Java class' simple name, i.e. Metadata.type.
	typeName() string
}

// marshalRequest renders an envelope as a Payload with the sender's ip and the
// type in metadata. body carries the object's JSON only, matching
// GrpcUtils.convertRequestToByte.
func marshalRequest(e envelope, clientIP string) (*gen.Payload, error) {
	body, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("nacos: marshal %s: %w", e.typeName(), err)
	}
	// Nacos wraps the JSON bytes in an Any with an empty type_url
	// (GrpcUtils.java:87 sets only Any.value), and the server reads
	// body.value.toStringUtf8() rather than resolving the type — so a bare
	// Any{Value: json} is what the wire actually carries.
	return &gen.Payload{
		Metadata: &gen.Metadata{
			Type:     e.typeName(),
			ClientIp: clientIP,
		},
		Body: &anypb.Any{Value: body},
	}, nil
}

// marshalResponse renders an ACK-style reply. Nacos' own convert(Response)
// (GrpcUtils.java:98-106) sets type only and leaves clientIp empty.
func marshalResponse(e envelope) (*gen.Payload, error) {
	body, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("nacos: marshal response %s: %w", e.typeName(), err)
	}
	return &gen.Payload{
		Metadata: &gen.Metadata{Type: e.typeName()},
		Body:     &anypb.Any{Value: body},
	}, nil
}

// unmarshal decodes a Payload's JSON body into out, and reports the type the
// peer claimed so callers can dispatch on it.
func unmarshal(p *gen.Payload, out any) (string, error) {
	if p == nil {
		return "", fmt.Errorf("nacos: nil payload")
	}
	var t string
	if m := p.GetMetadata(); m != nil {
		t = m.GetType()
	}
	if out == nil {
		return t, nil
	}
	if err := json.Unmarshal(p.GetBody().GetValue(), out); err != nil {
		return t, fmt.Errorf("nacos: decode %s: %w", t, err)
	}
	return t, nil
}
