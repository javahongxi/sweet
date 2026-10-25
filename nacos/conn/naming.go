package conn

// Naming wire shapes, verified against nacos @3.2.3 (tag c843da5be):
//
//   - api/.../naming/remote/NamingRemoteConstants.java:27,31,33,35 — type and
//     action strings ("registerInstance", "deregisterInstance", …).
//   - api/.../naming/remote/request/AbstractNamingRequest.java:30-34 — every
//     naming request carries namespace / serviceName / groupName.
//   - api/.../naming/pojo/ServiceInfo.java — hosts, name, groupName, clusters,
//     cacheMillis, lastRefTime, checksum.
//
// Jackson serialises Java *getters*, so a Go type must mirror property names.
// Two properties exist only as derived getters (Service.qualifiedName,
// Instance.serviceName) — copying field declarations alone would drop them.

// Instance mirrors api/.../naming/pojo/Instance.java.
type Instance struct {
	InstanceID  string            `json:"instanceId,omitempty"`
	IP          string            `json:"ip"`
	Port        int               `json:"port"`
	Weight      float64           `json:"weight"`
	Healthy     bool              `json:"healthy"`
	Enabled     bool              `json:"enabled"`
	Ephemeral   bool              `json:"ephemeral"`
	ClusterName string            `json:"clusterName,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ServiceInfo mirrors api/.../naming/pojo/ServiceInfo.java, the payload of both
// query responses and subscriber pushes.
type ServiceInfo struct {
	Name        string     `json:"name,omitempty"`
	GroupName   string     `json:"groupName,omitempty"`
	Clusters    string     `json:"clusters,omitempty"`
	CacheMillis int64      `json:"cacheMillis"`
	Hosts       []Instance `json:"hosts"`
	LastRefTime int64      `json:"lastRefTime"`
	Checksum    string     `json:"checksum,omitempty"`
	AllIPs      bool       `json:"allIps"`
}

// InstanceRequest registers or deregisters one instance.
// api/.../naming/remote/request/InstanceRequest.java:28-30.
type InstanceRequest struct {
	Namespace   string   `json:"namespace,omitempty"`
	ServiceName string   `json:"serviceName"`
	GroupName   string   `json:"groupName,omitempty"`
	Type        string   `json:"type"`
	Instance    Instance `json:"instance"`
}

func (InstanceRequest) typeName() string { return typeInstanceRequest }

// ServiceQueryRequest asks for the current instances of a service.
// api/.../naming/remote/request/ServiceQueryRequest.java:26-28.
type ServiceQueryRequest struct {
	Namespace   string `json:"namespace,omitempty"`
	ServiceName string `json:"serviceName"`
	GroupName   string `json:"groupName,omitempty"`
	Cluster     string `json:"cluster,omitempty"`
	HealthyOnly bool   `json:"healthyOnly"`
}

func (ServiceQueryRequest) typeName() string { return typeServiceQueryRequest }

// SubscribeServiceRequest turns pushes for one service on or off.
// api/.../naming/remote/request/SubscribeServiceRequest.java:26-28.
type SubscribeServiceRequest struct {
	Namespace   string `json:"namespace,omitempty"`
	ServiceName string `json:"serviceName"`
	GroupName   string `json:"groupName,omitempty"`
	Clusters    string `json:"clusters,omitempty"`
	Subscribe   bool   `json:"subscribe"`
}

func (SubscribeServiceRequest) typeName() string { return typeSubscribeServiceRequest }

// Response is the shared shape of every naming response: the JSON of
// api/.../remote/response/Response.java.
type Response struct {
	ErrorCode int    `json:"errorCode"`
	ErrorMsg  string `json:"errorMsg,omitempty"`
	Result    string `json:"result,omitempty"`
	Success   bool   `json:"success"`
}

// QueryServiceResponse wraps the instance list (response class name carries it).
type QueryServiceResponse struct {
	Response
	ServiceInfo ServiceInfo `json:"serviceInfo"`
}

// SubscribeServiceResponse is the answer to a subscribe call; it carries the
// initial snapshot, so a subscriber never needs a separate query.
type SubscribeServiceResponse struct {
	Response
	ServiceInfo ServiceInfo `json:"serviceInfo"`
}
