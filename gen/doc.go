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
// which reads the proto from a local Nacos checkout (tag 3.2.3) under
// $HOME/github/nacos and rewrites nacos_grpc_service.pb.go. The local file name
// matches upstream's, so no renaming step is involved. The repo keeps no copy of
// the .proto: the generated file carries the Apache header and the "// source:"
// line, and the previous copy stays retrievable from git history.
//
// Everything in this package except this file is generated — put hand-written
// notes here, because regenerating wipes comments added inside the .pb.go.
package gen
