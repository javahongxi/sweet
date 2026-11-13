package nacos

import (
	"errors"
	"fmt"
)

// The stable error surface. Anything this package returns either is one of these
// (possibly wrapped) or is a transport error left untouched, so a caller can use
// errors.Is for the protocol outcomes and errors.As for the detail.
//
// The distinction matters because several of these are *normal answers*, not
// failures: a config that does not exist yet, or a subscribe that the server
// answered with an empty list. A caller that treats them as errors will build
// retry loops around ordinary states.
var (
	// ErrNotFound is the server's "this dataId / service does not exist" answer
	// (config errorCode 300, ConfigQueryResponse.java:31).
	ErrNotFound = errors.New("nacos: not found")

	// ErrDenied is errorCode 403: auth is enabled on the server and this request
	// carried no acceptable credential. The cached token is invalidated when this
	// is returned, so a retry after GetAccess may succeed.
	ErrDenied = errors.New("nacos: denied (auth)")

	// ErrUnavailable reports a transport-level failure: no server answered the
	// handshake, or the connection went away mid-call.
	ErrUnavailable = errors.New("nacos: server unavailable")

	// ErrRefused is the server's own "I got the request and said no" — resultCode
	// non-success with an errorCode that is not 403/404. The server's message is
	// preserved in the error text.
	ErrRefused = errors.New("nacos: request refused")

	// ErrConflict is a compare-and-swap publish that lost: the md5 sent with the
	// request is no longer what the server holds, so somebody wrote in between the
	// query and the publish. It is a normal outcome, not a failure — the retry loop
	// is query again, then publish with the new md5. See checkWrite for why this one
	// has to be recognised from the message rather than an errorCode.
	ErrConflict = errors.New("nacos: md5 conflict")
)

// Response is the shared base of every Nacos response, config and naming alike:
// api/.../remote/response/Response.java:29-35 declares exactly
// resultCode / errorCode / message / requestId, and isSuccess() is the derived
// "success" property Jackson serialises.
//
// This used to be two different structs per domain, and the naming one declared
// errorMsg and result fields that the server never sends — so every naming
// refusal was logged with an empty reason.
type Response struct {
	ResultCode int    `json:"resultCode"`
	ErrorCode  int    `json:"errorCode"`
	Message    string `json:"message,omitempty"`
	RequestID  string `json:"requestId,omitempty"`
	Success    bool   `json:"success"`
}

// Response and error codes (api/.../remote/ResponseCode.java;
// ConfigQueryResponse.java:31-33; Response.java setErrorInfo:118-122 puts
// resultCode at 500 and moves the specific code into errorCode).
const (
	resultSuccess   = 200
	errCodeNotFound = 300
	errCodeNoRight  = 403
)

// classify turns a response header into one of the sentinels, keeping the
// server's own message. what and subject only feed the error text.
func (r Response) classify(what, subject string) error {
	switch {
	case r.Success || r.ResultCode == resultSuccess:
		return nil
	case r.ErrorCode == errCodeNotFound:
		return fmt.Errorf("%w: %s %s: %s", ErrNotFound, what, subject, r.Message)
	case r.ErrorCode == errCodeNoRight:
		return fmt.Errorf("%w: %s %s: %s", ErrDenied, what, subject, r.Message)
	default:
		return fmt.Errorf("%w: %s %s (errorCode=%d): %s", ErrRefused, what, subject, r.ErrorCode, r.Message)
	}
}

// denied reports whether this response says "authenticate again".
func (r Response) denied() bool { return r.ErrorCode == errCodeNoRight }
