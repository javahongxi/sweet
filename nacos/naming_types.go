package nacos

import (
	"encoding/json"
	"fmt"
	"strings"
)

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

// Response — the shared base of every Nacos response — lives in errors.go,
// because config and naming answer with the same four fields.
//
// The naming-domain RPCs are not here: they are private methods on NamingClient
// (naming_client.go), which owns the connection these calls need.

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

// NotifySubscriber mirrors NotifySubscriberRequest. Measured on a live 3.2.3
// server its JSON is {"headers":{},"requestId":"5","serviceInfo":{...}} — there
// is no top-level serviceName; identity lives in serviceInfo.name and is
// qualified ("GROUP@@service" whenever the group is not DEFAULT_GROUP).
type NotifySubscriber struct {
	Headers     map[string]string `json:"headers,omitempty"`
	RequestID   string            `json:"requestId,omitempty"`
	Namespace   string            `json:"namespace,omitempty"`
	ServiceInfo ServiceInfo       `json:"serviceInfo"`
}

// DecodeNotify unpacks a pushed frame. The caller still has to ack it — decoding
// and acking are separate on purpose, because an owner that drops the frame on
// an undecodable body may still want to stop the server's re-push loop.
func DecodeNotify(p Push) (NotifySubscriber, error) {
	var note NotifySubscriber
	if err := json.Unmarshal(p.Body, &note); err != nil {
		return note, fmt.Errorf("nacos: decode %s: %w", p.Type, err)
	}
	return note, nil
}

// ServiceName is the unqualified name the subscriber asked for.
func (n NotifySubscriber) ServiceName() string {
	return trimGroup(n.ServiceInfo.Name, n.ServiceInfo.GroupName)
}

// trimGroup strips the "GROUP@@" prefix a qualified Nacos service name carries.
// Both the DEFAULT_GROUP and the "default" cluster group appear in practice, so
// both are peeled; anything unqualified passes through untouched.
func trimGroup(qualified, group string) string {
	if qualified == "" {
		return qualified
	}
	if group != "" && group != "DEFAULT_GROUP" {
		return strings.TrimPrefix(qualified, group+"@@")
	}
	return strings.TrimPrefix(strings.TrimPrefix(qualified, "DEFAULT_GROUP@@"), "default@@")
}
