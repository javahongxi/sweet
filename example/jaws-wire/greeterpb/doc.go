// Package greeterpb holds the Go stubs for jaws' greeter service, generated
// from ../greeter.proto (vendored from jaws' jaws-sample-wire-api; only
// go_package was added — see that file's header for provenance).
//
// Both files here are generated. Put hand-written notes in this file: regenerating
// wipes anything added inside greeter.pb.go / greeter_grpc.pb.go.
//
// Requires two protoc plugins at generation time only, no runtime dependency:
//
//	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
//	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
//
// go:generate runs from this directory, hence -I .. and the plain "." output.
// paths=source_relative is what keeps the output flat here instead of recreating
// github.com/javahongxi/sweet/... under the output directory.
//
// The plugin path is written absolute rather than bare because protoc resolves
// it against its own PATH, which a build/CI shell frequently lacks. Wrapped in
// sh -c so $HOME expands — go:generate does not expand it itself.
//
//go:generate sh -c "protoc -I .. --plugin=protoc-gen-go=$HOME/go/bin/protoc-gen-go --go_out=. --go_opt=paths=source_relative ../greeter.proto"
//go:generate sh -c "protoc -I .. --plugin=protoc-gen-go-grpc=$HOME/go/bin/protoc-gen-go-grpc --go-grpc_out=. --go-grpc_opt=paths=source_relative ../greeter.proto"
package greeterpb
