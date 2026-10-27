// Package greeterpb holds the Go stubs generated from ../greeter.proto, sweet's
// own service definition for the Go-calls-Go example.
//
// Both greeter.pb.go and greeter_grpc.pb.go are generated. Put hand-written notes
// in this file: regenerating wipes anything added inside them.
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
