# Application-level service discovery, interoperable with jaws

Interface-level discovery (every other example) registers one registry instance
per service interface. Application-level discovery — what jaws calls
`registerMode=instance`, following Dubbo 3 — registers **one lightweight instance
per process** under the application name: the address plus a `revision` pointer,
so a scaled-out application shows one instance per replica. The full service URLs
live in the provider's memory; peers pull them over gRPC. The registry carries
orders of magnitude less data, and one application instance serves every
interface the process exports.

sweet implements it as two `registry.Registry` decorators in
[`applevel`](../../applevel), so nothing above the registry changes:
`server.New` takes the `Provider` decorator, `client.Dial` takes the `Consumer`
decorator, and the resolver, lifecycle and generated stubs are the same lines as
`example/greeter`.

```
provider                                    consumer
─────────────────────────────────────       ─────────────────────────────────────
Expose(service, addr)                       Watch(interface)
  ├─ publish mapping:{interface} → app        ├─ read mapping:{interface}
  │    (config center, group JAWS_CONFIG)     │    absent → interface-level passthrough
  ├─ aggregate service URL into MetadataInfo  ├─ subscribe app instance (one per app)
  ├─ revision = MD5(sorted URL values)        ├─ group instances by revision
  └─ register ONE app instance                ├─ cache hit? else pull revision over gRPC
       metadata: revision, protocol, path     │    /org.hongxi.jaws.metadata.MetadataService
                                              │    /getMetadataInfo  (StringValue envelope)
MetadataService answers pulls from memory     ├─ verify content hash == claimed revision
  (current + 100 archived revisions)          └─ expand URLs → interface-level instances
```

## The contract shared with jaws

Both sides speak the same protocol, so a sweet provider is consumable by a jaws
reference and vice versa, with no adapter on either side:

| Piece | Value |
| --- | --- |
| Mapping entry | dataId `mapping:{interface FQN}`, group `JAWS_CONFIG`, content = application name |
| App instance | name `{namespace}/{application}`, metadata sentinels `protocol=wire`, `path={application}`, plus `revision` |
| Revision | MD5 (lowercase hex) of the service URL values sorted by service key, each followed by `\n` |
| Metadata pull | gRPC `/org.hongxi.jaws.metadata.MetadataService/getMetadataInfo`, request `StringValue(revision)`, response `StringValue(MetadataInfo JSON)`; empty string = unknown revision |
| Service URL | `wire://host:port/{interface}?group=…&version=…&endpointType=service&…`, parameter values form-encoded |
| Coordinates | the pull carries `group`/`version` gRPC headers; jaws dispatches its metadata service by them |

The metadata channel is the one place a non-protobuf payload crosses the wire:
jaws-wire wraps the internal `MetadataService` in the well-known `StringValue`
type (JSON inside), business services stay pure protobuf on both sides.

## Run

Prerequisite: Nacos 3.x on `127.0.0.1:8848` (SDK gRPC port `9848`), auth off.

### sweet → sweet

```
# terminal 1: application-level provider
go run ./example/applevel/cmd/provider -namespace=

# terminal 2: application-level consumer
go run ./example/applevel/cmd/consumer -target=sweet -namespace=
```

`-namespace=` drops the `jaws/` prefix on both sides (sweet's bare-name
convention); the naming group `default_rpc` is shared by both frameworks. The consumer prints the expanded instance list before calling all
four method shapes. Kill the provider mid-run and the list empties; restart it
and the revision pull brings it back.

### sweet → jaws (consume `jaws-sample-wire-boot-provider`)

The jaws sample already runs with `register-mode: instance`:

```
# terminal 1, in the jaws repository
mvn -pl jaws-samples/jaws-sample-wire-boot-provider spring-boot:run

# terminal 2, in sweet
go run ./example/applevel/cmd/consumer            # -target=jaws is the default
```

The consumer reads `mapping:org.hongxi.jaws.sample.wire.proto.GreeterService`,
subscribes `jaws/sample-wire-boot-provider` in group `default_rpc`, pulls the
revision from the Java process over the StringValue envelope, and calls
`/greeter.Greeter/SayHello` — jaws indexes both the interface FQN and the proto
name server-side, the same fact `example/jaws-wire` relies on.

### jaws → sweet (consume this example's provider)

```
# terminal 1, in sweet — the defaults already match jaws' conventions
go run ./example/applevel/cmd/provider

# terminal 2, in the jaws repository
mvn -q -pl jaws-samples/jaws-sample-wire-sweet -am -DskipTests install
# run SweetGreeterConsumer with -applevel (see that sample's README)
```

The provider registers `jaws/sweet-greeter-app` in group `default_rpc` with URL
group `default_rpc` and version `1.0` — jaws' defaults, so a jaws reference
needs no namespace/group adaptation, only the mapping to exist. The service URL
path is `sweet.example.v1.Greeter`, which is exactly the Java interface
`jaws-sample-wire-sweet` builds its outbound `:path` from.

## What to watch in Nacos

- Naming, group `default_rpc`: one instance per provider process under
  `jaws/sweet-greeter-app` (or the bare name with `-namespace=`), metadata
  carrying `revision` — no per-interface entries at all.
- Config, group `JAWS_CONFIG`: `mapping:sweet.example.v1.Greeter` →
  `sweet-greeter-app`. The mapping survives unexport on purpose, the same
  boundary jaws keeps.
- Re-export with a different `-weight`: the revision changes, the instance is
  upserted in place (Nacos keys by ip:port), and consumers pull the new metadata
  without a second registration.

## Offline evidence

The wire contract is pinned without Nacos in
[`applevel/applevel_test.go`](../../applevel/applevel_test.go): the revision
golden vector, URL encoding round-trips, provider aggregation and revision
history, and a real loopback gRPC metadata pull end to end — including tampered
metadata being voided and healed by the retry task.