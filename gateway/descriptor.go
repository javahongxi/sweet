package gateway

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// compiledMethod resolves the descriptors registered by generated Go protobuf
// code. This is process-local type information, not a call to the backend's
// reflection service: a gateway can therefore route to servers that disable
// reflection while still keeping its Nacos route table free of Go factories.
func compiledMethod(service, method string) (MessageTypes, bool, error) {
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(service))
	if err != nil {
		return MessageTypes{}, false, fmt.Errorf("gRPC service %q is not present in Options.Services or compiled protobuf descriptors: %w", service, err)
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return MessageTypes{}, false, fmt.Errorf("compiled descriptor %q is not a gRPC service", service)
	}
	return methodTypes(serviceDescriptor, service, method, true)
}

func methodTypes(serviceDescriptor protoreflect.ServiceDescriptor, service, method string, preferGenerated bool) (MessageTypes, bool, error) {
	methodDescriptor := serviceDescriptor.Methods().ByName(protoreflect.Name(method))
	if methodDescriptor == nil {
		methods := serviceDescriptor.Methods()
		names := make([]string, 0, methods.Len())
		for i := 0; i < methods.Len(); i++ {
			names = append(names, string(methods.Get(i).Name()))
		}
		sort.Strings(names)
		return MessageTypes{}, false, fmt.Errorf("method %q of service %q is not registered (known: %s)",
			method, service, strings.Join(names, ", "))
	}
	if methodDescriptor.IsStreamingClient() {
		return MessageTypes{}, false, fmt.Errorf("method %q of service %q uses client streaming; the HTTP gateway supports unary and server streaming only",
			method, service)
	}
	constructor := dynamicMessageConstructor
	if preferGenerated {
		constructor = messageConstructor
	}
	return MessageTypes{
		Request: constructor(methodDescriptor.Input()),
		Reply:   constructor(methodDescriptor.Output()),
	}, methodDescriptor.IsStreamingServer(), nil
}

func messageConstructor(descriptor protoreflect.MessageDescriptor) func() proto.Message {
	if messageType, err := protoregistry.GlobalTypes.FindMessageByName(descriptor.FullName()); err == nil {
		return func() proto.Message { return messageType.New().Interface() }
	}
	return dynamicMessageConstructor(descriptor)
}

func dynamicMessageConstructor(descriptor protoreflect.MessageDescriptor) func() proto.Message {
	// The protobuf wire contract is independent of generated Go code. File-based
	// routes always take this path so their messages cannot disagree with the
	// descriptor snapshot used to validate their HTTP bindings.
	return func() proto.Message { return dynamicpb.NewMessage(descriptor) }
}
