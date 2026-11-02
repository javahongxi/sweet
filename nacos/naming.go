package nacos

import (
	"context"
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

// namingCaller issues naming-domain calls over one sdkConn someone else owns,
// filling the identity fields every naming request carries — namespace and
// groupName, per AbstractNamingRequest.java:30-34.
//
// It holds no lifecycle: no reconnect, no intents, no serverlist. NamingClient
// (naming_client.go) is the lifecycle owner and wraps one of these per
// connection. The split exists because registry/nacos spent several rounds
// earning its reconnect semantics (deregister-first, stale-consumer guard), and
// those guarantees had to keep running while the owner moved.
type namingCaller struct {
	conn        *sdkConn
	namespace   string
	group       string
	healthyOnly bool
}

// NewNamingCaller wraps a connection with the identity fields requests should
// carry.
func newNamingCaller(c *sdkConn, namespace, group string, healthyOnly bool) *namingCaller {
	return &namingCaller{conn: c, namespace: namespace, group: group, healthyOnly: healthyOnly}
}

// sdkConn returns the transport the owner still manages: pushes, acks, epoch.
func (n *namingCaller) sdkConn() *sdkConn { return n.conn }

// RegisterInstance registers or deregisters one instance. action is one of
// TypeRegisterInstance / TypeDeregisterInstance — the caller decides which,
// because "deregister first, then register" is a policy of the owner, not of the
// wire format.
func (n *namingCaller) RegisterInstance(ctx context.Context, serviceName, action string, in Instance) error {
	req := InstanceRequest{
		Namespace: n.namespace, ServiceName: serviceName, GroupName: n.group,
		Type: action, Instance: in,
	}
	t, body, err := n.conn.Unary(ctx, req)
	if err != nil {
		return err
	}
	var resp Response
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("nacos: %s answered with type %q, undecodable: %w", req.Type, t, err)
	}
	return resp.classify("register "+req.Type, serviceName)
}

// Subscribe turns pushes for one service on or off and returns the list the
// server answered with. Whether to persist that list is the caller's business.
func (n *namingCaller) Subscribe(ctx context.Context, serviceName string, subscribe bool) (ServiceInfo, error) {
	t, body, err := n.conn.Unary(ctx, SubscribeServiceRequest{
		Namespace: n.namespace, ServiceName: serviceName, GroupName: n.group, Subscribe: subscribe,
	})
	if err != nil {
		return ServiceInfo{}, err
	}
	if t != typeSubscribeServiceResponse {
		return ServiceInfo{}, fmt.Errorf("nacos: subscribe answered with type %q", t)
	}
	var resp SubscribeServiceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return ServiceInfo{}, err
	}
	if err := resp.classify("subscribe", serviceName); err != nil {
		return ServiceInfo{}, err
	}
	return resp.ServiceInfo, nil
}

// Query asks what the server currently believes is registered, independently of
// the push path.
func (n *namingCaller) Query(ctx context.Context, serviceName string) (ServiceInfo, error) {
	t, body, err := n.conn.Unary(ctx, ServiceQueryRequest{
		Namespace: n.namespace, ServiceName: serviceName, GroupName: n.group, HealthyOnly: n.healthyOnly,
	})
	if err != nil {
		return ServiceInfo{}, err
	}
	if t != typeQueryServiceResponse {
		return ServiceInfo{}, fmt.Errorf("nacos: query answered with type %q", t)
	}
	var resp QueryServiceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return ServiceInfo{}, err
	}
	if err := resp.classify("query", serviceName); err != nil {
		return ServiceInfo{}, err
	}
	return resp.ServiceInfo, nil
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
