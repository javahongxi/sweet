package gateway

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type fieldPath []protoreflect.FieldDescriptor

func resolveFieldPath(md protoreflect.MessageDescriptor, name string) (fieldPath, error) {
	if name == "" {
		return nil, fmt.Errorf("field path is empty")
	}
	parts := strings.Split(name, ".")
	fields := make(fieldPath, 0, len(parts))
	current := md
	for i, part := range parts {
		if current == nil || part == "" {
			return nil, fmt.Errorf("invalid field path %q", name)
		}
		fd := current.Fields().ByName(protoreflect.Name(part))
		if fd == nil {
			fd = current.Fields().ByJSONName(part)
		}
		if fd == nil {
			return nil, fmt.Errorf("message %s has no field %q", current.FullName(), part)
		}
		fields = append(fields, fd)
		if i == len(parts)-1 {
			break
		}
		if fd.IsList() || fd.IsMap() || fd.Kind() != protoreflect.MessageKind {
			return nil, fmt.Errorf("intermediate field %q is not a singular message", part)
		}
		current = fd.Message()
	}
	return fields, nil
}

func mutableFieldMessage(msg protoreflect.Message, fd protoreflect.FieldDescriptor) protoreflect.Message {
	return msg.Mutable(fd).Message()
}

func setFieldPath(msg protoreflect.Message, fields fieldPath, raw string) error {
	current := msg
	for _, fd := range fields[:len(fields)-1] {
		current = mutableFieldMessage(current, fd)
	}
	last := fields[len(fields)-1]
	value, err := parseScalar(last, raw)
	if err != nil {
		return err
	}
	current.Set(last, value)
	return nil
}

func parseScalar(fd protoreflect.FieldDescriptor, raw string) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		v, err := strconv.ParseBool(raw)
		return protoreflect.ValueOfBool(v), err
	case protoreflect.EnumKind:
		if ev := fd.Enum().Values().ByName(protoreflect.Name(raw)); ev != nil {
			return protoreflect.ValueOfEnum(ev.Number()), nil
		}
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("unknown enum value %q for %s", raw, fd.FullName())
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(n)), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		v, err := strconv.ParseInt(raw, 10, 32)
		return protoreflect.ValueOfInt32(int32(v)), err
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		v, err := strconv.ParseInt(raw, 10, 64)
		return protoreflect.ValueOfInt64(v), err
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		v, err := strconv.ParseUint(raw, 10, 32)
		return protoreflect.ValueOfUint32(uint32(v)), err
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		v, err := strconv.ParseUint(raw, 10, 64)
		return protoreflect.ValueOfUint64(v), err
	case protoreflect.FloatKind:
		v, err := strconv.ParseFloat(raw, 32)
		return protoreflect.ValueOfFloat32(float32(v)), err
	case protoreflect.DoubleKind:
		v, err := strconv.ParseFloat(raw, 64)
		return protoreflect.ValueOfFloat64(v), err
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(raw), nil
	case protoreflect.BytesKind:
		v, err := decodeBytes(raw)
		return protoreflect.ValueOfBytes(v), err
	default:
		return protoreflect.Value{}, fmt.Errorf("field %s is not a scalar", fd.FullName())
	}
}

func decodeBytes(raw string) ([]byte, error) {
	encodings := []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	}
	var err error
	for _, encoding := range encodings {
		var decoded []byte
		decoded, err = encoding.DecodeString(raw)
		if err == nil {
			return decoded, nil
		}
	}
	return nil, err
}

func fieldPathKey(fields fieldPath) string {
	parts := make([]string, len(fields))
	for i, fd := range fields {
		parts[i] = strconv.Itoa(int(fd.Number()))
	}
	return strings.Join(parts, ".")
}

func sameFieldPath(a, b fieldPath) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].FullName() != b[i].FullName() {
			return false
		}
	}
	return true
}

func fieldPathOccupied(candidate fieldPath, occupied []fieldPath) bool {
	for _, bound := range occupied {
		if len(candidate) < len(bound) {
			continue
		}
		if sameFieldPath(candidate[:len(bound)], bound) {
			return true
		}
	}
	return false
}
