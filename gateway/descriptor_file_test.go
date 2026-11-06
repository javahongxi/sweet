package gateway

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	pb "github.com/javahongxi/sweet/example/greeter/greeterpb"
)

func TestDescriptorFileUsesDynamicMessagesWithGeneratedBackend(t *testing.T) {
	b := newBackend(t, "greeter")
	descriptorPath := filepath.Join(t.TempDir(), "greeter.pb")
	writeDescriptorSet(t, descriptorPath, protodesc.ToFileDescriptorProto(pb.File_greeter_proto))
	g := newGateway(t, b, func(opts *Options) {
		opts.DescriptorFiles = []string{descriptorPath}
	})
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	if _, ok := g.Routes()[0].NewRequest().(*dynamicpb.Message); !ok {
		t.Fatalf("NewRequest = %T, want a file-bound dynamic message", g.Routes()[0].NewRequest())
	}
	rec := post(t, g, `{"name":"descriptor-file"}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "descriptor-file") {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestDescriptorFileReloadAddsServiceWithoutCompiledProto(t *testing.T) {
	b := newBackend(t, "greeter")
	descriptorPath := filepath.Join(t.TempDir(), "services.pb")
	writeDescriptorSet(t, descriptorPath, fileOnlyDescriptor("Echo"))
	g := newGateway(t, b, func(opts *Options) {
		opts.DescriptorFiles = []string{descriptorPath}
	})
	if err := g.ApplyRoutes([]RouteSpec{fileOnlyRoute("Echo", "/v1/echo")}); err != nil {
		t.Fatalf("ApplyRoutes Echo: %v", err)
	}
	assertDynamicRequest(t, g.Routes()[0], "gateway.fileonly.v1.EchoRequest")

	// Replacing the configured file and applying another table introduces a
	// service that has never been linked into this test binary.
	writeDescriptorSet(t, descriptorPath, fileOnlyDescriptor("Echo", "Lookup"))
	if err := g.ApplyRoutes([]RouteSpec{fileOnlyRoute("Lookup", "/v1/lookup")}); err != nil {
		t.Fatalf("ApplyRoutes Lookup after file replacement: %v", err)
	}
	assertDynamicRequest(t, g.Routes()[0], "gateway.fileonly.v1.EchoRequest")
	if g.Routes()[0].Path != "/v1/lookup" {
		t.Fatalf("live path = %q, want the reloaded route", g.Routes()[0].Path)
	}
	for _, service := range []string{"gateway.fileonly.v1.Echo", "gateway.fileonly.v1.Lookup"} {
		if _, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service)); !errors.Is(err, protoregistry.NotFound) {
			t.Errorf("file service %q leaked into GlobalFiles: %v", service, err)
		}
	}
}

func TestDescriptorFileFailureKeepsLiveRoutes(t *testing.T) {
	b := newBackend(t, "greeter")
	g := newGateway(t, b, nil)
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("initial ApplyRoutes: %v", err)
	}
	g.opts.DescriptorFiles = []string{filepath.Join(t.TempDir(), "missing.pb")}
	if err := g.ApplyRoutes([]RouteSpec{greetSpec("/v1/replacement")}); err == nil || !strings.Contains(err.Error(), "missing.pb") {
		t.Fatalf("ApplyRoutes error = %v, want the missing descriptor path", err)
	}
	if got := g.Routes(); len(got) != 1 || got[0].Path != greetPath {
		t.Fatalf("live routes changed after descriptor failure: %+v", got)
	}
	if rec := post(t, g, `{"name":"still-live"}`); rec.Code != http.StatusOK {
		t.Fatalf("old route after descriptor failure: status = %d", rec.Code)
	}
}

func TestDescriptorFileRouteCacheRestore(t *testing.T) {
	b := newBackend(t, "greeter")
	dir := t.TempDir()
	descriptorPath := filepath.Join(dir, "greeter.pb")
	cachePath := filepath.Join(dir, "routes.cache.json")
	writeDescriptorSet(t, descriptorPath, protodesc.ToFileDescriptorProto(pb.File_greeter_proto))
	configure := func(g *Gateway) {
		g.opts.Services = nil
		g.opts.DescriptorFiles = []string{descriptorPath}
	}

	g := newCacheGateway(t, b, cachePath)
	configure(g)
	if err := g.ApplyRoutes([]RouteSpec{greetSpec(greetPath)}); err != nil {
		t.Fatalf("ApplyRoutes: %v", err)
	}
	g2 := newCacheGateway(t, b, cachePath)
	configure(g2)
	applied, _, err := g2.RestoreRoutes()
	if err != nil || !applied {
		t.Fatalf("RestoreRoutes = (%v, %v)", applied, err)
	}
	if _, ok := g2.Routes()[0].NewRequest().(*dynamicpb.Message); !ok {
		t.Fatalf("restored NewRequest = %T, want dynamicpb", g2.Routes()[0].NewRequest())
	}
	if rec := post(t, g2, `{"name":"cached-file"}`); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "cached-file") {
		t.Fatalf("restored route: status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func assertDynamicRequest(t *testing.T, route Route, want protoreflect.FullName) {
	t.Helper()
	message, ok := route.NewRequest().(*dynamicpb.Message)
	if !ok {
		t.Fatalf("NewRequest = %T, want dynamicpb", route.NewRequest())
	}
	if got := message.Descriptor().FullName(); got != want {
		t.Fatalf("request descriptor = %q, want %q", got, want)
	}
}

func fileOnlyRoute(service, path string) RouteSpec {
	return RouteSpec{
		Method: http.MethodPost, Path: path, Service: "file-only",
		FullMethod: "/gateway.fileonly.v1." + service + "/Echo", ResponseBody: "message",
	}
}

func fileOnlyDescriptor(services ...string) *descriptorpb.FileDescriptorProto {
	file := &descriptorpb.FileDescriptorProto{
		Name: proto.String("gateway_file_only.proto"), Package: proto.String("gateway.fileonly.v1"),
		Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("EchoRequest"), Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("name"), Number: proto.Int32(1),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}}},
			{Name: proto.String("EchoReply"), Field: []*descriptorpb.FieldDescriptorProto{{
				Name: proto.String("message"), Number: proto.Int32(1),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
			}}},
		},
	}
	for _, service := range services {
		file.Service = append(file.Service, &descriptorpb.ServiceDescriptorProto{
			Name: proto.String(service), Method: []*descriptorpb.MethodDescriptorProto{{
				Name: proto.String("Echo"), InputType: proto.String(".gateway.fileonly.v1.EchoRequest"),
				OutputType: proto.String(".gateway.fileonly.v1.EchoReply"),
			}},
		})
	}
	return file
}

func writeDescriptorSet(t *testing.T, path string, files ...*descriptorpb.FileDescriptorProto) {
	t.Helper()
	data, err := proto.Marshal(&descriptorpb.FileDescriptorSet{File: files})
	if err != nil {
		t.Fatalf("marshal descriptor set: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write descriptor set: %v", err)
	}
}
