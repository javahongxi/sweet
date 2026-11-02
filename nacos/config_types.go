package nacos

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
