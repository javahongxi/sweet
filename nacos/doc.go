// Package nacos is a Nacos client written by hand against the 3.x wire
// protocol — no nacos-sdk-go, and no HTTP long polling.
//
// # What is supported API
//
// Two clients, one per domain, each owning its own connection:
//
//   - NamingClient: register, deregister, subscribe, query, with reconnect that
//     replays the intents, and failover across a server list. Pushes arrive on one
//     shared Updates() channel for all services, so a multi-service caller demuxes
//     by ServiceInfo.Name.
//   - ConfigClient: query and watch a config, with pull-after-notify and the same
//     reconnect replay keyed by md5.
//   - Their Options structs, AccessGetter (with PasswordAccess / StaticAccess),
//     the value types they return (Instance, ServiceInfo, Config, ConfigKey,
//     Push) and the error sentinels in errors.go.
//
// Everything else in this package is the protocol, not the API: the transport
// (sdkConn, dialConn), the envelope codec, and the request/response DTOs. Those
// DTOs stay exported on purpose — this package's value is that the wire contract
// is readable and assertable line by line against a real server, and hiding it
// behind a narrow façade would throw that away. Do not build on them: they may
// change shape when the protocol does. Call the clients.
//
// This comment is the whole boundary. There is no plan to split the package or
// release it as its own module, so nothing enforces the layering: an outsider
// building on a DTO gets no compiler error, only this paragraph.
//
// # Why one connection per domain
//
// Nacos labels each SDK connection by module and its naming module ignores any
// connection not labelled "naming" (ConnectionBasedClientManager.java:60-64),
// so the two domains cannot share one. Within a domain, query and listen do
// share the connection — the unary RPC and the bidi stream live on one channel.
//
// # Facts this implementation rests on
//
// Verified against a local nacos checkout at tag 3.2.3 (commit c843da5be) and a
// live server, not inferred from documentation:
//
//   - Ephemeral instance liveness IS the connection: there is no beat; when the
//     stream goes, the server drops every instance registered on it. Hence the
//     reconnect-and-replay in naming_client.go.
//   - A deregistration must survive a reconnect, so intents are marked not-live
//     before the request is sent (注销优先).
//   - Config pushes carry keys only; the client must query for content, then
//     re-listen with the new md5.
//   - Every pushed ServerRequest must be acked with a *registered* response
//     class on the same stream, echoing its requestId — see ackClassFor.
//   - Credentials ride on every request's Metadata.headers; the handshake itself
//     is never authenticated.
package nacos
