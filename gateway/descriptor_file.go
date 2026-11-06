package gateway

import (
	"errors"
	"fmt"
	"os"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// loadDescriptorFiles builds a private registry from binary FileDescriptorSet
// files. Nothing is registered globally: a rejected table cannot leave types
// behind, and one Gateway cannot affect another Gateway in the same process.
func loadDescriptorFiles(paths []string) (*protoregistry.Files, error) {
	if len(paths) == 0 {
		return nil, nil
	}
	set := new(descriptorpb.FileDescriptorSet)
	type sourceFile struct {
		path string
		file *descriptorpb.FileDescriptorProto
	}
	seen := make(map[string]sourceFile)
	for _, path := range paths {
		if path == "" {
			return nil, errors.New("gateway: descriptor file path is empty")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("gateway: read descriptor file %s: %w", path, err)
		}
		var current descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(data, &current); err != nil {
			return nil, fmt.Errorf("gateway: decode descriptor file %s: %w", path, err)
		}
		if len(current.File) == 0 {
			return nil, fmt.Errorf("gateway: descriptor file %s contains no file descriptors", path)
		}
		for _, file := range current.File {
			name := file.GetName()
			if name == "" {
				return nil, fmt.Errorf("gateway: descriptor file %s contains a file with no name", path)
			}
			if previous, ok := seen[name]; ok {
				// Self-contained sets produced with --include_imports commonly
				// carry the same WKT dependencies. Identical copies are harmless;
				// conflicting definitions must not be resolved by input order.
				if proto.Equal(previous.file, file) {
					continue
				}
				return nil, fmt.Errorf("gateway: descriptor file %s conflicts with %s for proto file %q", path, previous.path, name)
			}
			seen[name] = sourceFile{path: path, file: file}
			set.File = append(set.File, file)
		}
	}
	files, err := protodesc.NewFiles(set)
	if err != nil {
		return nil, fmt.Errorf("gateway: build descriptor files: %w", err)
	}
	return files, nil
}

// resolveMethod prefers the file snapshot when it defines the service. A
// service absent from that snapshot can still use a compiled descriptor, but a
// method missing from a file-defined service is an error rather than a reason
// to mix two schema versions.
func resolveMethod(files *protoregistry.Files, service, method string) (MessageTypes, bool, error) {
	if files != nil {
		mt, stream, err := fileMethod(files, service, method)
		if err == nil || !errors.Is(err, protoregistry.NotFound) {
			return mt, stream, err
		}
	}
	mt, stream, err := compiledMethod(service, method)
	if err != nil && files != nil && errors.Is(err, protoregistry.NotFound) {
		return MessageTypes{}, false, fmt.Errorf("gRPC service %q is not present in Options.Services, descriptor files, or compiled protobuf descriptors", service)
	}
	return mt, stream, err
}

func fileMethod(files *protoregistry.Files, service, method string) (MessageTypes, bool, error) {
	descriptor, err := files.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return MessageTypes{}, false, fmt.Errorf("descriptor files: %w", err)
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return MessageTypes{}, false, fmt.Errorf("descriptor file entry %q is not a gRPC service", service)
	}
	return methodTypes(serviceDescriptor, service, method, false)
}
