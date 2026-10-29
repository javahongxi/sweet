package gateway

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StatusToHTTP maps a gRPC status code to the HTTP status this edge answers
// with. It is the gRPC-to-HTTP convention the ecosystem already converged on
// (Envoy's bridge and grpc-gateway use the same numbers), reproduced here so
// the table is auditable rather than imported.
//
// Two entries are worth arguing for rather than assuming:
//
//   - Canceled becomes 499. It is not an RFC 9110 status; it is nginx's
//     "Client Closed Request". A cancelled call is the caller's own doing, and
//     500 would blame the gateway for it, so the non-standard number is the
//     lesser evil — and it is spelled out in the body either way.
//   - Aborted and AlreadyExists both become 409: one is "the transaction was
//     rolled back because of a concurrent change", the other is "that resource
//     is already there". Both are conflicts, and HTTP has one code for that.
//
// Anything not listed falls through to 500 — including Unknown, which is also
// what a plain Go error that never went through status.FromError becomes. A
// gateway that invented a 4xx for an error it cannot classify would be hiding
// a provider bug behind a client-looking status code.
var StatusToHTTP = map[codes.Code]int{
	codes.Canceled:           499,
	codes.Unknown:            http.StatusInternalServerError,
	codes.InvalidArgument:    http.StatusBadRequest,
	codes.DeadlineExceeded:   http.StatusGatewayTimeout,
	codes.NotFound:           http.StatusNotFound,
	codes.AlreadyExists:      http.StatusConflict,
	codes.PermissionDenied:   http.StatusForbidden,
	codes.ResourceExhausted:  http.StatusTooManyRequests,
	codes.FailedPrecondition: http.StatusBadRequest,
	codes.Aborted:            http.StatusConflict,
	codes.OutOfRange:         http.StatusBadRequest,
	codes.Unimplemented:      http.StatusNotImplemented,
	codes.Internal:           http.StatusInternalServerError,
	codes.Unavailable:        http.StatusServiceUnavailable,
	codes.DataLoss:           http.StatusInternalServerError,
	codes.Unauthenticated:    http.StatusUnauthorized,
}

// StatusName is the canonical spelling of a gRPC status code — the one the
// protobuf enum uses (INVALID_ARGUMENT), not the one grpc-go's Code.String
// returns (InvalidArgument; see codes/code_string.go:31 versus the
// canonicalString at :72, which the library keeps in internal and therefore out
// of reach for us).
//
// The edge reports the canonical spelling for the same reason this package
// marshals replies with UseProtoNames: an error surface that renames things
// relative to the IDL is where debugging quietly goes wrong. Google's own error
// shapes and grpc-gateway use these names, so a reader who has met
// google.rpc.Code before does not have to translate.
var StatusName = map[codes.Code]string{
	codes.OK:                 "OK",
	codes.Canceled:           "CANCELLED", // grpc-go's constant is Canceled; the spec spells it with two Ls
	codes.Unknown:            "UNKNOWN",
	codes.InvalidArgument:    "INVALID_ARGUMENT",
	codes.DeadlineExceeded:   "DEADLINE_EXCEEDED",
	codes.NotFound:           "NOT_FOUND",
	codes.AlreadyExists:      "ALREADY_EXISTS",
	codes.PermissionDenied:   "PERMISSION_DENIED",
	codes.ResourceExhausted:  "RESOURCE_EXHAUSTED",
	codes.FailedPrecondition: "FAILED_PRECONDITION",
	codes.Aborted:            "ABORTED",
	codes.OutOfRange:         "OUT_OF_RANGE",
	codes.Unimplemented:      "UNIMPLEMENTED",
	codes.Internal:           "INTERNAL",
	codes.Unavailable:        "UNAVAILABLE",
	codes.DataLoss:           "DATA_LOSS",
	codes.Unauthenticated:    "UNAUTHENTICATED",
}

// statusName falls back to the numeric form for a code outside the table, so a
// provider that returns something impossible still produces a parseable body.
func statusName(c codes.Code) string {
	if n, ok := StatusName[c]; ok {
		return n
	}
	return "CODE_" + strconv.FormatUint(uint64(c), 10)
}

// HTTPStatusOf reports the HTTP status for a gRPC code, defaulting to 500.
func HTTPStatusOf(c codes.Code) int {
	if s, ok := StatusToHTTP[c]; ok {
		return s
	}
	return http.StatusInternalServerError
}

// StatusTableLines returns the mapping in code-name order, for publishing or
// asserting the table where a reader can check it against this file.
func StatusTableLines() []string {
	type row struct {
		name string
		code codes.Code
	}
	rows := make([]row, 0, len(StatusToHTTP))
	for c := range StatusToHTTP {
		rows = append(rows, row{name: statusName(c), code: c})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })

	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.name+" -> "+http.StatusText(HTTPStatusOf(r.code)))
	}
	return out
}

// ErrorBody is the JSON written for every failure, so the edge has exactly one
// error shape. Code carries the canonical gRPC status name; CodeNumber is -1
// when the request never became an RPC (an unknown route, a wrong method, a bad
// JSON body that the gateway refused to send).
//
// HTTPStatus is omitted when it is zero, which happens in exactly one place: a
// terminal error line inside a stream that already committed to 200
// (writeInBandError). There the field would be a contradiction — the response
// status really is 200, and the error is about the RPC, not the exchange.
type ErrorBody struct {
	HTTPStatus int    `json:"httpStatus,omitempty"`
	Code       string `json:"code"`
	CodeNumber int32  `json:"codeNumber"`
	Message    string `json:"message"`
}

// maskedMessage is what replaces a backend's status text when VerboseErrors is
// off. Kept deliberately vague but actionable: a caller needs to know the
// request failed here, and an operator needs to know where to look.
const maskedMessage = "the gateway could not complete the request; check its logs"

// writeStatus answers with the error shape derived from an error, which for
// anything that came off the wire is a gRPC status.
//
// The masking rule is one line and worth stating plainly: 4xx messages are
// always echoed because they describe the caller's own request, 5xx messages
// are echoed only when verbose is set because they describe our machinery.
func writeStatus(w http.ResponseWriter, err error, verbose bool) {
	st, _ := status.FromError(err)
	code := st.Code()
	httpStatus := HTTPStatusOf(code)

	body := ErrorBody{
		HTTPStatus: httpStatus,
		Code:       statusName(code),
		CodeNumber: int32(code),
		Message:    st.Message(),
	}
	if body.Message == "" {
		body.Message = http.StatusText(httpStatus)
	}
	if !verbose && httpStatus >= http.StatusInternalServerError {
		body.Message = maskedMessage
	}
	writeBody(w, httpStatus, body)
}

// writeInBandError appends one final JSON line, {"error":{…}}, to a stream whose
// 200 is already committed.
//
// Same fields as the ordinary error body minus httpStatus, for the reason in
// ErrorBody's comment: the exchange succeeded (200 is on the wire), the RPC
// failed. A consumer that only reads frames sees a stream that ended early; one
// that parses the last line sees the same code and message it would have seen on
// a unary call.
func writeInBandError(w http.ResponseWriter, flusher http.Flusher, err error, verbose bool) {
	st, _ := status.FromError(err)
	code := st.Code()
	httpStatus := HTTPStatusOf(code)

	body := ErrorBody{
		Code:       statusName(code),
		CodeNumber: int32(code),
		Message:    st.Message(),
	}
	if body.Message == "" {
		body.Message = http.StatusText(httpStatus)
	}
	if !verbose && httpStatus >= http.StatusInternalServerError {
		body.Message = maskedMessage
	}

	line := struct {
		Error ErrorBody `json:"error"`
	}{Error: body}
	data, merr := json.Marshal(line)
	if merr != nil {
		// Nothing can be reported: the status line is already out, and this is
		// the one place a marshalling failure has no channel left to complain
		// through. The stream simply ends, which a caller reads as truncation.
		return
	}
	if _, werr := w.Write(append(data, '\n')); werr == nil && flusher != nil {
		flusher.Flush()
	}
}

// writeLocalError answers a failure the gateway decided on its own, before or
// instead of any RPC.
func writeLocalError(w http.ResponseWriter, httpStatus int, code, message string, codeNumber int32) {
	writeBody(w, httpStatus, ErrorBody{
		HTTPStatus: httpStatus,
		Code:       code,
		CodeNumber: codeNumber,
		Message:    message,
	})
}

// writeBody encodes first and only then commits headers. Encoding an
// ErrorBody can fail in exactly one way — nothing here holds an unmarshalable
// type — but a gateway that wrote 200 and then failed is a gateway whose
// failures look like successes.
func writeBody(w http.ResponseWriter, httpStatus int, body ErrorBody) {
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_, _ = w.Write(data)
}
