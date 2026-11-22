// Package applevel implements application-level service discovery (local
// metadata mode), wire-compatible with jaws' registerMode=instance.
//
// The model is the one jaws and Dubbo 3 share: the registry holds ONE
// lightweight instance per process — keyed by host:port under the application
// name, so a scaled-out application has one instance per replica — whose
// "revision" parameter is a content hash pointer, the heavy service metadata
// lives in the provider's memory, and a config-center mapping
// ("mapping:{interface}" in group JAWS_CONFIG) routes an ordinary interface
// reference to the application's instances. Consumers pull
// metadata point-to-point over gRPC — /org.hongxi.jaws.metadata.MetadataService/
// getMetadataInfo with a google.protobuf.StringValue envelope: the request
// carries the revision, the response carries the MetadataInfo as JSON (empty
// string = unknown revision). This is the same envelope jaws-wire uses, so a
// sweet provider serves jaws consumers and a sweet consumer pulls from jaws
// providers without either side knowing the other's language.
//
// Two decorators carry the whole feature, both satisfying registry.Registry so
// server.Server and client.Dial work unchanged:
//
//   - Provider wraps the registry on the server side: Expose aggregates service
//     URLs into a revision-addressed MetadataInfo, registers one application
//     instance, publishes the mapping, and serves the metadata pull channel on
//     the business gRPC server (register MetadataServiceDesc beside the business
//     descriptors).
//   - Consumer wraps the registry on the client side: Watch checks the mapping
//     once — absent means plain interface-level passthrough — then subscribes to
//     the application, resolves every revision through a shared cache or a
//     point-to-point pull, and expands the metadata back into interface-level
//     instances.
//
// Naming follows the interop convention (example/jaws-wire): both frameworks
// share the naming group default_rpc; jaws registers under "jaws/{name}",
// sweet under the bare name, so the prefix alone tells the frameworks apart.
// ConsumerOptions.Namespace and ProviderOptions.Namespace pick the side of
// that convention; the URL "group" parameter is separate from the Nacos
// naming group and must match what a jaws consumer reference sets.
package applevel

import (
	"container/list"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/javahongxi/sweet/nacos"
)

const (
	// ConfigGroup is the Nacos group jaws' DynamicConfiguration uses on every
	// registry leg; mapping entries live there, not beside the naming data.
	ConfigGroup = "JAWS_CONFIG"

	// MappingPrefix separates mapping entries from dynamic-config toggles,
	// mirroring jaws' ServiceNameMapping.KEY_PREFIX.
	MappingPrefix = "mapping:"

	// MetadataServiceName is the internal metadata pull service, addressed by
	// its Java interface FQN on the gRPC path — jaws consumers dial exactly this.
	MetadataServiceName = "org.hongxi.jaws.metadata.MetadataService"

	// MetadataMethodName is the camelCase method jaws' AppLevelRegistryDirectory
	// sends; the PascalCase alias is served too for grpcurl-style callers.
	MetadataMethodName = "getMetadataInfo"

	// DefaultURLGroup and DefaultURLVersion are jaws' URL parameter defaults
	// (UrlParam.Identity); the metadata pull sends them as headers so the
	// provider-side serviceKey lookup matches a default-coordinate export.
	DefaultURLGroup   = "default_rpc"
	DefaultURLVersion = "1.0"

	// cacheCapacity bounds both the consumer's revision cache and the
	// provider's revision history, the same LRU-100 jaws keeps.
	cacheCapacity = 100
)

// ConfigClient is the slice of *nacos.ConfigClient this package needs: read a
// mapping, publish a mapping. An interface so offline tests supply a map.
type ConfigClient interface {
	Query(ctx context.Context, key nacos.ConfigKey) (nacos.Config, error)
	Publish(ctx context.Context, key nacos.ConfigKey, in nacos.PublishInput) error
}

// MetadataInfo mirrors jaws' org.hongxi.jaws.metadata.MetadataInfo JSON shape:
// application, revision, and serviceKey (group/path/version) → full URL string.
type MetadataInfo struct {
	Application string            `json:"application"`
	Revision    string            `json:"revision"`
	Services    map[string]string `json:"services"`
}

// CalRevision computes the content hash exactly like jaws'
// MetadataInfo.calRevision: values sorted by service key, each followed by a
// newline, MD5, lowercase hex. Order-independent, so two instances exporting
// the same service list compute the same revision.
func CalRevision(services map[string]string) string {
	keys := make([]string, 0, len(services))
	for k := range services {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(services[k])
		b.WriteByte('\n')
	}
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// serviceURL is the parsed form of one jaws URL string
// ("wire://host:port/path?k=v&..."). Only what discovery needs is kept.
type serviceURL struct {
	Protocol string
	Host     string
	Port     int
	Path     string
	Params   map[string]string
}

// buildServiceURL renders a jaws-compatible full URL string. Values are
// form-encoded (url.Values.Encode), which jaws' URL.valueOf decodes with
// URLDecoder — the same contract as URL.toFullStr.
func buildServiceURL(protocol, host string, port int, path string, params map[string]string) string {
	v := url.Values{}
	for k, val := range params {
		v.Set(k, val)
	}
	return protocol + "://" + net.JoinHostPort(host, strconv.Itoa(port)) + "/" + path + "?" + v.Encode()
}

// parseServiceURL reads a jaws URL string back. Query values are decoded with
// url.ParseQuery, the counterpart of the encoding buildServiceURL and jaws'
// toFullStr apply.
func parseServiceURL(raw string) (serviceURL, error) {
	var u serviceURL
	body, query, _ := strings.Cut(raw, "?")
	proto, rest, ok := strings.Cut(body, "://")
	if !ok {
		return u, fmt.Errorf("applevel: URL has no scheme: %q", raw)
	}
	u.Protocol = proto
	hostPort, path, _ := strings.Cut(rest, "/")
	u.Path = path
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return u, fmt.Errorf("applevel: URL has no host:port: %q", raw)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return u, fmt.Errorf("applevel: URL port %q: %w", portStr, err)
	}
	u.Host, u.Port = host, port
	params, err := url.ParseQuery(query)
	if err != nil {
		return u, fmt.Errorf("applevel: URL query %q: %w", query, err)
	}
	u.Params = make(map[string]string, len(params))
	for k, vs := range params {
		if len(vs) > 0 {
			u.Params[k] = vs[0]
		}
	}
	return u, nil
}

// serviceKey is jaws' group/path/version identity within an application.
func serviceKey(group, path, version string) string {
	return group + "/" + path + "/" + version
}

// revisionCache is an LRU of revision → MetadataInfo with the same self-check
// jaws' MetadataCache applies: every read recomputes the content hash, so a
// tampered or corrupted entry is evicted instead of served.
type revisionCache struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List
	entries  map[string]*list.Element
}

type cacheEntry struct {
	revision string
	info     *MetadataInfo
}

func newRevisionCache(capacity int) *revisionCache {
	return &revisionCache{
		capacity: capacity,
		ll:       list.New(),
		entries:  make(map[string]*list.Element),
	}
}

func (c *revisionCache) get(revision string) *MetadataInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[revision]
	if !ok {
		return nil
	}
	info := el.Value.(*cacheEntry).info
	if info.Revision != revision || CalRevision(info.Services) != revision {
		c.ll.Remove(el)
		delete(c.entries, revision)
		return nil
	}
	c.ll.MoveToFront(el)
	return info
}

func (c *revisionCache) put(info *MetadataInfo) {
	if info == nil || info.Revision == "" || CalRevision(info.Services) != info.Revision {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[info.Revision]; ok {
		c.ll.MoveToFront(el)
		return
	}
	c.entries[info.Revision] = c.ll.PushFront(&cacheEntry{revision: info.Revision, info: info})
	if c.ll.Len() > c.capacity {
		oldest := c.ll.Back()
		c.ll.Remove(oldest)
		delete(c.entries, oldest.Value.(*cacheEntry).revision)
	}
}

// mappingKey is the config dataId carrying interface → application.
func mappingKey(interfaceName string) nacos.ConfigKey {
	return nacos.ConfigKey{DataID: MappingPrefix + interfaceName, Group: ConfigGroup}
}

// jsonMetadata renders the response envelope payload.
func jsonMetadata(info *MetadataInfo) (string, error) {
	b, err := json.Marshal(info)
	if err != nil {
		return "", fmt.Errorf("applevel: marshal metadata: %w", err)
	}
	return string(b), nil
}
