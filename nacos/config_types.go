package nacos

import "fmt"

// Config-domain wire shapes, verified against nacos @3.2.3 (tag c843da5be):
//
//   - api/.../config/remote/request/AbstractConfigRequest.java:30-34 — every
//     config request carries dataId / group / tenant. Note the asymmetry with
//     naming: the namespace is "namespace" there (naming_types.go) and "tenant" here.
//   - api/.../config/remote/request/ConfigQueryRequest.java:29 — adds tag.
//   - api/.../config/remote/request/ConfigBatchListenRequest.java:33-35,90-98 —
//     listen flag plus a list of ConfigListenContext{group,md5,dataId,tenant}.
//   - api/.../config/remote/response/ConfigChangeBatchListenResponse.java:33,84-90
//     — changedConfigs carries only the *keys* whose md5 went stale, no content.
//   - api/.../config/remote/request/ConfigChangeNotifyRequest.java:28-34 — the
//     push, also key-only. Content is always pulled by the client, never pushed
//     (jaws/doc/nacos-config.md:79 — "服务端任何一跳都不推正文").
//   - api/.../remote/response/Response.java:29-35 — resultCode defaults to 200;
//     setErrorInfo (:118-122) sets resultCode=500 and moves the specific code
//     into errorCode, so success is resultCode==200 and "not found" is
//     errorCode==300 (ConfigQueryResponse.java:31).

// Exported push class names, so a caller can dispatch on Push.Type.
const (
	TypeConfigChangeNotifyRequest = "ConfigChangeNotifyRequest"
)

// ConfigResponse is the shared Response base; kept as a name because the config
// structs embed it by name and the wire docs refer to it.
type ConfigResponse = Response

// check turns a non-success config response into one of the sentinels in
// errors.go. subject is only for the message.
func (r ConfigResponse) check(what string, key configKey) error {
	return r.classify(what, key.Group+"/"+key.DataID)
}

// ConfigQueryRequest pulls one config's content.
// api/.../config/remote/request/ConfigQueryRequest.java:29.
type ConfigQueryRequest struct {
	RequestID string `json:"requestId,omitempty"`
	DataID    string `json:"dataId"`
	Group     string `json:"group"`
	Tenant    string `json:"tenant,omitempty"`
	Tag       string `json:"tag,omitempty"`
}

func (ConfigQueryRequest) typeName() string { return "ConfigQueryRequest" }

// ConfigQueryResponse carries the content plus the md5 the client must echo back
// on the next batch listen. encryptedDataKey is surfaced but not decrypted —
// encryption is out of scope (doc/nacos-config-routes.md 不做 #4).
// api/.../config/remote/response/ConfigQueryResponse.java:29-47.
type ConfigQueryResponse struct {
	ConfigResponse
	Content          string `json:"content,omitempty"`
	EncryptedDataKey string `json:"encryptedDataKey,omitempty"`
	ContentType      string `json:"contentType,omitempty"`
	MD5              string `json:"md5,omitempty"`
	LastModified     int64  `json:"lastModified"`
	Beta             bool   `json:"beta"`
	Tag              string `json:"tag,omitempty"`
}

func (ConfigQueryResponse) typeName() string { return "ConfigQueryResponse" }

// ConfigListenContext is one watched key with the md5 the client currently holds.
// api/.../config/remote/request/ConfigBatchListenRequest.java:90-98.
type ConfigListenContext struct {
	Group  string `json:"group"`
	MD5    string `json:"md5"`
	DataID string `json:"dataId"`
	Tenant string `json:"tenant,omitempty"`
}

// ConfigBatchListenRequest subscribes (Listen=true) or unsubscribes (false) a
// batch of keys. The server answers immediately — there is no 30s suspension in
// gRPC mode; the HTTP long-polling timeout has no equivalent here
// (ConfigChangeBatchListenRequestHandler.java:60-96).
type ConfigBatchListenRequest struct {
	RequestID            string                `json:"requestId,omitempty"`
	Listen               bool                  `json:"listen"`
	ConfigListenContexts []ConfigListenContext `json:"configListenContexts"`
}

func (ConfigBatchListenRequest) typeName() string { return "ConfigBatchListenRequest" }

// ConfigKey identifies one config: dataId + group + tenant(namespace).
type ConfigKey struct {
	DataID string
	Group  string
	Tenant string
}

// ConfigContext is the key-only shape the server uses to say "this one changed".
type ConfigContext struct {
	Group  string `json:"group"`
	DataID string `json:"dataId"`
	Tenant string `json:"tenant,omitempty"`
}

func (c ConfigContext) key() ConfigKey {
	return ConfigKey{DataID: c.DataID, Group: c.Group, Tenant: c.Tenant}
}

// ConfigChangeBatchListenResponse is the immediate answer to a batch listen:
// the subset of watched keys whose md5 the server considers stale.
type ConfigChangeBatchListenResponse struct {
	ConfigResponse
	ChangedConfigs []ConfigContext `json:"changedConfigs"`
}

func (ConfigChangeBatchListenResponse) typeName() string { return "ConfigChangeBatchListenResponse" }

// ConfigChangeNotifyRequest is the server's push. It is a ServerRequest, so the
// client must answer with ConfigChangeNotifyResponse echoing the same requestId
// on the same stream, or the server re-pushes and eventually unregisters the
// connection (GrpcClient.java:282-285, RpcAckCallbackSynchronizer).
type ConfigChangeNotifyRequest struct {
	RequestID string `json:"requestId,omitempty"`
	DataID    string `json:"dataId"`
	Group     string `json:"group"`
	Tenant    string `json:"tenant,omitempty"`
}

func (ConfigChangeNotifyRequest) typeName() string { return TypeConfigChangeNotifyRequest }

// configKey is the internal map key; ConfigKey is exported for callers.
type configKey = ConfigKey

// The two write requests. They are not an extension: the handlers are @Since
// ("2.0.0") and live in the same package as the read ones this file already
// covers (ConfigPublishRequestHandler.java, ConfigRemoveRequestHandler.java),
// reached through the same RequestHandler registry that GrpcRequestAcceptor
// dispatches on, and guarded by the same @Secured(action=WRITE, signType=CONFIG).
// The official Java client writes through this port too — it builds a
// ConfigPublishRequest at ClientWorker.java:1436-1444 and a ConfigRemoveRequest
// at :1470 — so a client that only reads has implemented half the domain.

// ConfigPublishRequest writes one config's content.
// api/.../config/remote/request/ConfigPublishRequest.java:30-36 — content, casMd5
// and a free-form additionMap. The keys the server actually reads are tag,
// appName, type, src_user, encryptedDataKey, config_tags, desc, use, effect,
// schema and betaIps (ConfigPublishRequestHandler.java:84-117).
type ConfigPublishRequest struct {
	RequestID string            `json:"requestId,omitempty"`
	DataID    string            `json:"dataId"`
	Group     string            `json:"group"`
	Tenant    string            `json:"tenant,omitempty"`
	Content   string            `json:"content"`
	CasMD5    string            `json:"casMd5,omitempty"`
	Addition  map[string]string `json:"additionMap,omitempty"`
}

func (ConfigPublishRequest) typeName() string { return "ConfigPublishRequest" }

// ConfigPublishResponse adds nothing to Response — api/.../ConfigPublishResponse.java
// declares only the two build helpers. Note what the success builder does NOT
// set: buildSuccessResponse() returns a bare object, so resultCode stays at its
// field default of 200 (Response.java:29) and success serialises as true.
type ConfigPublishResponse struct {
	ConfigResponse
}

func (ConfigPublishResponse) typeName() string { return "ConfigPublishResponse" }

// ConfigRemoveRequest deletes one config.
// api/.../config/remote/request/ConfigRemoveRequest.java:27 — the only field
// beyond the key is tag, and it is a trap: ConfigRemoveRequestHandler.java:84
// passes it to deleteConfig as the *grayName* argument, so a non-empty tag
// deletes a gray rule rather than clearing the tag of the formal config
// (ConfigOperationService.java:313-326). Send it empty.
type ConfigRemoveRequest struct {
	RequestID string `json:"requestId,omitempty"`
	DataID    string `json:"dataId"`
	Group     string `json:"group"`
	Tenant    string `json:"tenant,omitempty"`
	Tag       string `json:"tag,omitempty"`
}

func (ConfigRemoveRequest) typeName() string { return "ConfigRemoveRequest" }

// ConfigRemoveResponse adds nothing to Response either, but note its failure
// builder takes only a message (ConfigRemoveResponse.buildFailResponse(String)):
// a remove refusal arrives with errorCode 0, so it can only be reported through
// the message text. That is why Remove's error goes through check, not checkWrite.
type ConfigRemoveResponse struct {
	ConfigResponse
}

func (ConfigRemoveResponse) typeName() string { return "ConfigRemoveResponse" }

// casConflictMessage is the exact text the server produces when the md5 sent
// with a publish no longer matches what it holds
// (ConfigOperationService.java:145-147).
const casConflictMessage = "Cas publish fail, server md5 may have changed."

// checkWrite classifies a publish answer. It is check plus one case: the CAS
// rejection is not given its own errorCode. ConfigPublishRequestHandler.java:134-137
// routes both NacosApiException and ConfigAlreadyExistsException through
// buildFailResponse(ResponseCode.FAIL.getCode(), …), which sets resultCode=500
// AND errorCode=500 (ConfigPublishResponse.java:45-51) — RESOURCE_CONFLICT never
// reaches the wire, only its message does. So the message is the signal, matched
// exactly rather than by substring guessing, and anything unrecognised still
// falls back to the sentinels check returns.
func (r ConfigResponse) checkWrite(what string, key configKey) error {
	if r.Message == casConflictMessage {
		// errorCode is kept in the text on purpose: the server puts
		// ResponseCode.FAIL (500) there for this outcome, which is the evidence that
		// no dedicated code reaches the wire.
		return fmt.Errorf("%w: %s %s (errorCode=%d): %s", ErrConflict, what, key.Group+"/"+key.DataID, r.ErrorCode, r.Message)
	}
	return r.check(what, key)
}
