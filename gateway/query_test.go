package gateway

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	_ "google.golang.org/protobuf/types/known/durationpb"
	_ "google.golang.org/protobuf/types/known/fieldmaskpb"
	_ "google.golang.org/protobuf/types/known/structpb"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	_ "google.golang.org/protobuf/types/known/wrapperspb"
)

func mappingTestMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()
	str := func(value string) *string { return &value }
	i32 := func(value int32) *int32 { return &value }
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	typeString := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	typeInt32 := descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	typeInt64 := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	typeBytes := descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
	typeMessage := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	typeEnum := descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum()

	field := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: str(name), Number: i32(number), Label: labelOptional, Type: &kind}
	}
	messageField := func(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
		f := field(name, number, *typeMessage)
		f.TypeName = str(typeName)
		return f
	}
	mapEntry := &descriptorpb.DescriptorProto{
		Name: str("LabelsEntry"),
		Field: []*descriptorpb.FieldDescriptorProto{
			field("key", 1, *typeString), field("value", 2, *typeString),
		},
		Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
	}
	labels := messageField("labels", 3, ".gateway.test.Sample.LabelsEntry")
	labels.Label = labelRepeated
	counts := field("counts", 2, *typeInt32)
	counts.Label = labelRepeated
	names := field("names", 12, *typeString)
	names.Label = labelRepeated
	state := field("state", 11, *typeEnum)
	state.TypeName = str(".gateway.test.Sample.State")
	displayName := field("display_name", 13, *typeString)
	displayName.JsonName = str("displayName")

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: str("gateway_query_test.proto"), Package: str("gateway.test"), Syntax: str("proto3"),
		Dependency: []string{
			"google/protobuf/timestamp.proto", "google/protobuf/duration.proto",
			"google/protobuf/field_mask.proto", "google/protobuf/wrappers.proto", "google/protobuf/struct.proto",
		},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: str("Inner"), Field: []*descriptorpb.FieldDescriptorProto{field("value", 1, *typeString)},
		}, {
			Name: str("Sample"),
			Field: []*descriptorpb.FieldDescriptorProto{
				field("name", 1, *typeString), counts, labels, messageField("inner", 4, ".gateway.test.Inner"),
				messageField("when", 5, ".google.protobuf.Timestamp"),
				messageField("timeout", 6, ".google.protobuf.Duration"),
				messageField("mask", 7, ".google.protobuf.FieldMask"),
				messageField("note", 8, ".google.protobuf.StringValue"),
				messageField("attrs", 9, ".google.protobuf.Struct"),
				field("big", 10, *typeInt64), state, names, displayName, field("data", 14, *typeBytes),
			},
			NestedType: []*descriptorpb.DescriptorProto{mapEntry},
			EnumType: []*descriptorpb.EnumDescriptorProto{{
				Name: str("State"), Value: []*descriptorpb.EnumValueDescriptorProto{
					{Name: str("UNKNOWN"), Number: i32(0)}, {Name: str("ACTIVE"), Number: i32(1)},
				},
			}},
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatalf("build test descriptor: %v", err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName("Sample"))
}

func TestPopulateQueryScalarCollectionsAndWellKnownTypes(t *testing.T) {
	msg := mappingTestMessage(t)
	values := url.Values{}
	values.Add("counts", "1")
	values.Add("counts", "2")
	values.Add("labels[x]", "y")
	values.Add("inner.value", "nested")
	values.Add("when", "2024-01-02T03:04:05Z")
	values.Add("timeout", "1.5s")
	values.Add("mask", "fooBar,baz")
	values.Add("note", "wrapped")
	values.Add("attrs", `{"depth":2}`)
	values.Add("big", "9007199254740993")
	values.Add("state", "ACTIVE")
	values.Add("names", "a")
	values.Add("names", "b")
	values.Add("data", "aGk=")
	if err := populateQuery(msg, values, nil, false); err != nil {
		t.Fatalf("populateQuery: %v", err)
	}
	fields := msg.Descriptor().Fields()
	if got := msg.Get(fields.ByName("counts")).List().Len(); got != 2 {
		t.Errorf("counts length = %d", got)
	}
	labels := msg.Get(fields.ByName("labels")).Map()
	if got := labels.Get(protoreflect.ValueOfString("x").MapKey()).String(); got != "y" {
		t.Errorf("labels[x] = %q", got)
	}
	inner := msg.Get(fields.ByName("inner")).Message()
	if got := inner.Get(inner.Descriptor().Fields().ByName("value")).String(); got != "nested" {
		t.Errorf("inner.value = %q", got)
	}
	when := msg.Get(fields.ByName("when")).Message()
	if got := when.Get(when.Descriptor().Fields().ByName("seconds")).Int(); got != 1704164645 {
		t.Errorf("timestamp seconds = %d", got)
	}
	timeout := msg.Get(fields.ByName("timeout")).Message()
	if got := timeout.Get(timeout.Descriptor().Fields().ByName("nanos")).Int(); got != 500000000 {
		t.Errorf("duration nanos = %d", got)
	}
	mask := msg.Get(fields.ByName("mask")).Message()
	if got := mask.Get(mask.Descriptor().Fields().ByName("paths")); got.List().Len() != 2 {
		t.Errorf("field mask paths = %v", got)
	}
	note := msg.Get(fields.ByName("note")).Message()
	if got := note.Get(note.Descriptor().Fields().ByName("value")).String(); got != "wrapped" {
		t.Errorf("wrapper = %q", got)
	}
	if got := msg.Get(fields.ByName("big")).Int(); got != 9007199254740993 {
		t.Errorf("big = %d", got)
	}
	if got := msg.Get(fields.ByName("state")).Enum(); got != 1 {
		t.Errorf("state = %d", got)
	}
	if got := msg.Get(fields.ByName("data")).Bytes(); string(got) != "hi" {
		t.Errorf("data = %q", got)
	}
}

func TestQueryRejectsProtoAndJSONAliasesTogether(t *testing.T) {
	msg := mappingTestMessage(t)
	values := url.Values{"display_name": {"a"}, "displayName": {"b"}}
	if err := populateQuery(msg, values, nil, false); err == nil {
		t.Fatal("duplicate proto/JSON field aliases were accepted")
	}
}

func TestBodyCollectionsNestedPathAndResponseInt64(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		body  string
		check func(*dynamicpb.Message) bool
	}{
		{"list", "counts", `[1,2]`, func(m *dynamicpb.Message) bool {
			return m.Get(m.Descriptor().Fields().ByName("counts")).List().Len() == 2
		}},
		{"map", "labels", `{"x":"y"}`, func(m *dynamicpb.Message) bool {
			return m.Get(m.Descriptor().Fields().ByName("labels")).Map().Len() == 1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := mappingTestMessage(t)
			rt := Route{Method: http.MethodPost, Path: "/x", Service: "s", FullMethod: "/gateway.test.Sample/Call",
				Body: stringPointer(tc.field), NewRequest: func() proto.Message { return dynamicpb.NewMessage(msg.Descriptor()) },
				NewReply: func() proto.Message { return dynamicpb.NewMessage(msg.Descriptor()) }}
			if err := rt.validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(tc.body))
			if err := decodeRequest(req, rt, nil, msg, false); err != nil {
				t.Fatalf("decodeRequest: %v", err)
			}
			if !tc.check(msg) {
				t.Fatalf("body did not populate %s: %v", tc.field, msg)
			}
		})
	}

	t.Run("nested path", func(t *testing.T) {
		msg := mappingTestMessage(t)
		rt := Route{Method: http.MethodGet, Path: "/x/{inner.value}", Service: "s", FullMethod: "/gateway.test.Sample/Call",
			Body: stringPointer(""), NewRequest: func() proto.Message { return dynamicpb.NewMessage(msg.Descriptor()) },
			NewReply: func() proto.Message { return dynamicpb.NewMessage(msg.Descriptor()) }}
		if err := rt.validate(); err != nil {
			t.Fatalf("validate: %v", err)
		}
		values, ok := rt.pattern.match("/x/nested")
		if !ok {
			t.Fatal("nested path did not match")
		}
		req := httptest.NewRequest(http.MethodGet, "/x/nested", nil)
		if err := decodeRequest(req, rt, values, msg, false); err != nil {
			t.Fatalf("decodeRequest: %v", err)
		}
		inner := msg.Get(msg.Descriptor().Fields().ByName("inner")).Message()
		if got := inner.Get(inner.Descriptor().Fields().ByName("value")).String(); got != "nested" {
			t.Fatalf("inner.value = %q", got)
		}
	})

	t.Run("response int64 stays a string", func(t *testing.T) {
		msg := mappingTestMessage(t)
		fd := msg.Descriptor().Fields().ByName("big")
		msg.Set(fd, protoreflect.ValueOfInt64(9007199254740993))
		fields, err := resolveFieldPath(msg.Descriptor(), "big")
		if err != nil {
			t.Fatal(err)
		}
		data, err := marshalResponse(msg, fields)
		if err != nil {
			t.Fatalf("marshalResponse: %v", err)
		}
		if string(data) != `"9007199254740993"` {
			t.Fatalf("responseBody = %s", data)
		}
	})
}
