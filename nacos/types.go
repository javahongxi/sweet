package nacos

// Java class simple names that appear in Metadata.type on the naming path.
// Verified against nacos @3.2.3: GrpcClient.java:404 (setup), :607-625 (setup
// ack pair), and api/.../naming/remote/request/*.java for the naming requests.
// Exported spellings for callers that dispatch on frames.
const (
	TypeInstanceRequest          = "InstanceRequest"
	TypeServiceQueryRequest      = "ServiceQueryRequest"
	TypeSubscribeServiceRequest  = "SubscribeServiceRequest"
	TypeNotifySubscriberRequest  = "NotifySubscriberRequest"
	TypeSubscribeServiceResponse = "SubscribeServiceResponse"

	// TypeRegisterInstance / TypeDeregisterInstance are the InstanceRequest.type
	// values (NamingRemoteConstants.java:27,31), not Metadata.type.
	TypeRegisterInstance   = "registerInstance"
	TypeDeregisterInstance = "deregisterInstance"
)

const (
	typeConnectionSetupRequest = "ConnectionSetupRequest"
	typeSetupAckRequest        = "SetupAckRequest"
	typeSetupAckResponse       = "SetupAckResponse"
	typeServerCheckRequest     = "ServerCheckRequest"
	typeHealthCheckRequest     = "HealthCheckRequest"

	typeInstanceRequest          = "InstanceRequest"
	typeQueryServiceResponse     = "QueryServiceResponse"
	typeSubscribeServiceResponse = "SubscribeServiceResponse"
	typeServiceQueryRequest      = "ServiceQueryRequest"
	typeSubscribeServiceRequest  = "SubscribeServiceRequest"
	typeNotifySubscriberRequest  = "NotifySubscriberRequest"
	typeEmptyResponse            = "EmptyResponse"
)

// Correction, measured against a live 3.2.3 server: requestId *does* appear on
// the naming push path, but not where the earlier note expected it — it is a
// field of the pushed object's JSON (ServerRequest.requestId, e.g.
// NotifySubscriberRequest → {"headers":{},"requestId":"5","serviceInfo":{...}}),
// never a Metadata.headers key. Unary calls still need no correlation id of
// their own (gRPC does it), but a push MUST be acked with the same requestId or
// the server re-pushes it. See Conn.Ack.

// ClientVersion is reported in the setup request. The server only records it
// (GrpcBiStreamRequestAcceptor.java:170 → RemotingHeartBeatEvent), it does not
// gate behaviour on it.
const ClientVersion = "sweet/0.1"

// ConnectionSetupRequest mirrors api/.../remote/request/
// ConnectionSetupRequest.java:30-36 — exactly four fields, and it is an
// InternalRequest so it inherits no extra JSON members.
//
// AbilityTable must be serialised even when empty: the server replies to the
// setup only if the field is non-null —
//
//	GrpcBiStreamRequestAcceptor.java:203-212
//	  // server sends abilities only when:
//	  //   1. client sends setUpRequest with its abilities table
//	  //   2. client sends setUpRequest with empty table
//	  if (setUpRequest.getAbilityTable() != null) { ... send SetupAckRequest }
//
// so omitting "abilityTable" makes the server stay silent forever. The Java
// client always fills it (GrpcClient.java:407-408). An empty object is enough
// to select that branch; it also means "I speak ability negotiation".
type ConnectionSetupRequest struct {
	ClientVersion string            `json:"clientVersion"`
	Tenant        string            `json:"tenant,omitempty"`
	Labels        map[string]string `json:"labels,omitempty"`
	AbilityTable  map[string]bool   `json:"abilityTable"`
}

func (ConnectionSetupRequest) typeName() string { return typeConnectionSetupRequest }

// SetupAckRequest is what the server pushes back over the same bidi stream to
// acknowledge the setup; it carries the negotiated abilities.
// api/.../remote/request/SetupAckRequest.java:31.
type SetupAckRequest struct {
	AbilityTable map[string]bool `json:"abilityTable,omitempty"`
}

func (SetupAckRequest) typeName() string { return typeSetupAckRequest }

// SetupAckResponse completes the handshake: SetupRequestHandler returns it
// (GrpcClient.java:619-625), so the client must send it or the server keeps
// waiting.
type SetupAckResponse struct{}

func (SetupAckResponse) typeName() string { return typeSetupAckResponse }

// HealthCheckRequest is the application-level idle keepalive probe
// (api/.../remote/request/HealthCheckRequest.java:25 extends InternalRequest,
// so it adds no fields of its own). It probes whether the *connection* is alive;
// it does NOT keep an instance registered — ephemeral liveness *is* the
// connection (harbor-vs-nacos.md §8.5, gRPC 模型无 beat).
type HealthCheckRequest struct {
	// RequestID mirrors Request.requestId (api/.../remote/request/Request.java:32),
	// which Jackson serialises; the server routes its answer back to it via
	// RpcAckCallbackSynchronizer.
	RequestID string `json:"requestId,omitempty"`
}

func (HealthCheckRequest) typeName() string { return "HealthCheckRequest" }

// ServerCheckRequest is the smallest unary call available and is what Nacos
// itself uses to probe a server (GrpcClient.java:239-247).
type ServerCheckRequest struct{}

func (ServerCheckRequest) typeName() string { return typeServerCheckRequest }
