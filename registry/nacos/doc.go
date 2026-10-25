// Package nacos will implement registry.Registry over Nacos's naming protocol,
// with the client protocol written by hand (no nacos-sdk-go).
//
// It is intentionally empty until S2–S4 land: the connection layer (bidi
// channel, ConnectionSetupRequest handshake, keepalive and reconnect) comes
// first, then the four unary naming requests, then push handling.
package nacos
