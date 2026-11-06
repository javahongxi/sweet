package gateway

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"

	"google.golang.org/grpc/metadata"
)

// DefaultForwardHeaders are the HTTP request headers copied into gRPC metadata
// without the caller asking for them.
//
// The list is short on purpose. An edge that forwards every header it receives
// hands each backend a private dependency on the caller's client library, and
// one header name that happens to collide later becomes an interface nobody
// can remove. These three are chosen because they are protocol-wide rather than
// per-service: a request id and the W3C trace context need to survive the hop
// or the trace stops at the gateway.
//
// Anything else — including Authorization, which is deliberately *not* here —
// has to be named in Options.ForwardHeaders. Authorization is off by default
// because a token addressed to the gateway is not a token addressed to every
// service behind it; forwarding it verbatim silently turns each backend into an
// authentication consumer that never asked for one. When the intent really is
// "pass the credential through", say so explicitly.
var DefaultForwardHeaders = []string{"x-request-id", "traceparent", "tracestate"}

// neverForward holds headers that are dropped even if someone lists them.
//
// These are hop-by-hop or framing fields: they describe *this* HTTP
// connection, not the RPC, so copying them is a protocol mistake rather than a
// policy choice. Content-Type belongs here for the same reason — the RPC's
// content type is set by the gRPC transport, and a stray
// "application/json" in metadata would only confuse the provider.
var neverForward = map[string]bool{
	"transfer-encoding":   true,
	"content-length":      true,
	"content-type":        true,
	"connection":          true,
	"keep-alive":          true,
	"upgrade":             true,
	"te":                  true,
	"trailer":             true,
	"proxy-connection":    true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"host":                true,
	"expect":              true,
}

// withMetadata attaches the forwarded headers to ctx as outgoing gRPC
// metadata. Keys are lower-cased, which is what gRPC requires anyway, so a
// caller writing "X-Request-Id" and one writing "x-request-id" arrive as the
// same key on the other side.
func withMetadata(ctx context.Context, h http.Header, extra []string) context.Context {
	allow := make(map[string]bool, len(DefaultForwardHeaders)+len(extra))
	for _, k := range DefaultForwardHeaders {
		allow[strings.ToLower(k)] = true
	}
	for _, k := range extra {
		allow[strings.ToLower(k)] = true
	}

	var kv []string
	for name, values := range h {
		key := strings.ToLower(name)
		if neverForward[key] || !allow[key] {
			continue
		}
		// grpc-go documents the key alphabet (letters, digits, - _ .) and that a
		// "grpc-" prefix is reserved for the library's own keys — see
		// google.golang.org/grpc/metadata New and Pairs. An HTTP token is
		// broader than that alphabet, so a header name like "x-a!b" is dropped
		// instead of being silently mangled or rejected at the transport.
		if !isGRPCKey(key) || strings.HasPrefix(key, "grpc-") {
			continue
		}
		for _, v := range values {
			if isPrintableASCII(v) {
				kv = append(kv, key, v)
				continue
			}
			// gRPC restricts a non "-bin" value to printable ASCII, so a UTF-8
			// header value cannot go across as it came in. The documented
			// escape hatch is the "-bin" suffix plus base64, which the receiver
			// decodes under the same convention.
			kv = append(kv, key+"-bin", base64.StdEncoding.EncodeToString([]byte(v)))
		}
	}
	if len(kv) == 0 {
		return ctx
	}
	// Pairs is safe here: kv is built strictly in twos, and metadata.Pairs
	// panics on an odd length.
	return metadata.NewOutgoingContext(ctx, metadata.Pairs(kv...))
}

func isGRPCKey(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '-' || c == '_' || c == '.':
		default:
			return false
		}
	}
	return true
}

func isPrintableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// copyResponseHeaders exposes only explicitly allowed backend metadata. The
// same framing and alphabet rules apply in both directions: a gRPC server is
// not a trusted source of arbitrary HTTP response headers.
func copyResponseHeaders(header http.Header, md metadata.MD, allowed []string) {
	for _, name := range allowed {
		key := strings.ToLower(strings.TrimSpace(name))
		if !isGRPCKey(key) || neverForward[key] || strings.HasPrefix(key, "grpc-") {
			continue
		}
		values, ok := responseMetadataValues(md, key)
		if !ok {
			continue
		}
		headerName := http.CanonicalHeaderKey(key)
		header.Del(headerName)
		for _, value := range values {
			header.Add(headerName, value)
		}
	}
}

// declareResponseTrailers returns the metadata-key to HTTP-trailer-name mapping
// that was actually announced. net/http only treats a post-WriteHeader Set as a
// trailer when its name was declared first, so announcing and writing must use
// this same map rather than two independently filtered lists.
func declareResponseTrailers(header http.Header, r *http.Request, allowed []string) map[string]string {
	if !acceptsTrailers(r) {
		return nil
	}
	mapping := make(map[string]string)
	var names []string
	for _, name := range allowed {
		key := strings.ToLower(strings.TrimSpace(name))
		if !isGRPCKey(key) || neverForward[key] || strings.HasPrefix(key, "grpc-") {
			continue
		}
		trailerName := http.CanonicalHeaderKey("Grpc-Trailer-" + key)
		mapping[key] = trailerName
		names = append(names, trailerName)
	}
	if len(names) > 0 {
		header.Set("Trailer", strings.Join(names, ", "))
	}
	return mapping
}

func writeResponseTrailers(header http.Header, md metadata.MD, declared map[string]string) {
	for key, trailerName := range declared {
		values, ok := responseMetadataValues(md, key)
		if !ok {
			continue
		}
		header[trailerName] = values
	}
}

func responseMetadataValues(md metadata.MD, key string) ([]string, bool) {
	values := md.Get(key)
	if len(values) == 0 {
		return nil, false
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		if strings.HasSuffix(key, "-bin") {
			result = append(result, base64.StdEncoding.EncodeToString([]byte(value)))
			continue
		}
		if !isPrintableASCII(value) {
			return nil, false
		}
		result = append(result, value)
	}
	return result, true
}

func acceptsTrailers(r *http.Request) bool {
	for _, line := range r.Header.Values("TE") {
		for _, value := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(value), "trailers") {
				return true
			}
		}
	}
	return false
}
