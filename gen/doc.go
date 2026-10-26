// Package gen holds the Go types generated from Nacos' own
// api/src/main/proto/nacos_grpc_service.proto.
//
// Only two message types are needed — Payload and Metadata, where Metadata.type
// carries the Java request class' simple name. No gRPC stubs are generated or
// used: the unary method and the bidi stream are invoked through
// grpc.ClientConn.Invoke / NewStream with explicit method names, so
// protoc-gen-go-grpc is deliberately not part of this toolchain.
//
// Regenerate with:
//
//	go generate github.com/javahongxi/sweet/gen
//
// which reads the vendored nacos_grpc_service.proto beside it (go:generate runs
// from the directive's own directory) and rewrites
// nacos_grpc_service.pb.go. The proto is vendored rather than fetched from a
// local Nacos checkout so this repo regenerates on its own, and so the schema
// stays reviewable — generated .pb.go diffs are not.
//
// Vendored from Nacos (tag 3.2.3, api/src/main/proto/nacos_grpc_service.proto),
// byte-for-byte, so the generated file's “// source:” line still reads plain
// “nacos_grpc_service.proto” — identical to what upstream's own path would
// produce. Its Apache header is upstream's, not ours, so provenance lives here
// rather than as an edit to the vendored file.
//
// The copy is consumed, not decorative: deleting nacos_grpc_service.pb.go and
// running the go:generate line above recreates it byte-for-byte (verified).
//
// Everything in this package except this file is generated — put hand-written
// notes here, because regenerating wipes comments added inside the .pb.go.
//
//go:generate protoc -I . --go_out=. --go_opt=paths=source_relative --go_opt=Mnacos_grpc_service.proto=github.com/javahongxi/sweet/gen nacos_grpc_service.proto
package gen
