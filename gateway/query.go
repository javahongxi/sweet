package gateway

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// populateQuery fills only fields not already claimed by a path or body
// binding. A singular query field follows the usual last-value-wins rule;
// repeated fields consume every value in their original order.
func populateQuery(msg protoreflect.Message, values url.Values, occupied []fieldPath, allowUnknown bool) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	seenFields := make(map[string]string)
	seenOneofs := make(map[string]string)

	for _, key := range keys {
		expression, mapKey, hasMapKey, err := splitQueryKey(key)
		if err != nil {
			return err
		}
		fields, err := resolveFieldPath(msg.Descriptor(), expression)
		if err != nil {
			if allowUnknown {
				continue
			}
			return fmt.Errorf("query parameter %q: %w", key, err)
		}
		if fieldPathOccupied(fields, occupied) {
			continue
		}
		fd := fields[len(fields)-1]
		if fd.IsMap() && !hasMapKey {
			return fmt.Errorf("query parameter %q needs a map key, for example %s[key]", key, expression)
		}
		if !fd.IsMap() && hasMapKey {
			return fmt.Errorf("query parameter %q has a map key but is not a map", key)
		}

		canonical := fieldPathKey(fields)
		if !fd.IsMap() {
			if previous, ok := seenFields[canonical]; ok && previous != key {
				return fmt.Errorf("query parameters %q and %q address the same field", previous, key)
			}
			seenFields[canonical] = key
		}
		if oneof := fd.ContainingOneof(); oneof != nil {
			oneofKey := fieldPathKey(fields[:len(fields)-1]) + ":" + string(oneof.FullName())
			if previous, ok := seenOneofs[oneofKey]; ok && previous != canonical {
				return fmt.Errorf("query parameter %q conflicts with another field in oneof %s", key, oneof.FullName())
			}
			seenOneofs[oneofKey] = canonical
		}

		current := msg
		for _, parent := range fields[:len(fields)-1] {
			current = mutableFieldMessage(current, parent)
		}
		vals := values[key]
		if len(vals) == 0 {
			continue
		}
		switch {
		case fd.IsMap():
			if err := setQueryMap(current, fd, mapKey, vals[len(vals)-1]); err != nil {
				return fmt.Errorf("query parameter %q: %w", key, err)
			}
		case fd.IsList():
			if err := setQueryList(current, fd, vals); err != nil {
				return fmt.Errorf("query parameter %q: %w", key, err)
			}
		default:
			if err := setQuerySingular(current, fd, vals[len(vals)-1]); err != nil {
				return fmt.Errorf("query parameter %q: %w", key, err)
			}
		}
	}
	return nil
}

func splitQueryKey(key string) (expression, mapKey string, hasMapKey bool, err error) {
	open := strings.IndexByte(key, '[')
	if open < 0 {
		if strings.Contains(key, "]") {
			return "", "", false, fmt.Errorf("query parameter %q has an unmatched ]", key)
		}
		return key, "", false, nil
	}
	if !strings.HasSuffix(key, "]") || strings.Count(key, "[") != 1 || strings.Count(key, "]") != 1 || open == 0 {
		return "", "", false, fmt.Errorf("query parameter %q has an invalid map key", key)
	}
	return key[:open], key[open+1 : len(key)-1], true, nil
}

func setQuerySingular(msg protoreflect.Message, fd protoreflect.FieldDescriptor, raw string) error {
	if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
		target := msg.Mutable(fd).Message()
		return unmarshalQueryMessage(target, fd.Message().FullName(), raw)
	}
	value, err := parseScalar(fd, raw)
	if err != nil {
		return err
	}
	msg.Set(fd, value)
	return nil
}

func setQueryList(msg protoreflect.Message, fd protoreflect.FieldDescriptor, raws []string) error {
	list := msg.Mutable(fd).List()
	for _, raw := range raws {
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			element := dynamicpb.NewMessage(fd.Message())
			if err := unmarshalQueryMessage(element, fd.Message().FullName(), raw); err != nil {
				return err
			}
			list.Append(protoreflect.ValueOfMessage(element))
			continue
		}
		value, err := parseScalar(fd, raw)
		if err != nil {
			return err
		}
		list.Append(value)
	}
	return nil
}

func setQueryMap(msg protoreflect.Message, fd protoreflect.FieldDescriptor, rawKey, rawValue string) error {
	key, err := parseScalar(fd.MapKey(), rawKey)
	if err != nil {
		return fmt.Errorf("invalid map key: %w", err)
	}
	var value protoreflect.Value
	valueField := fd.MapValue()
	if valueField.Kind() == protoreflect.MessageKind || valueField.Kind() == protoreflect.GroupKind {
		element := dynamicpb.NewMessage(valueField.Message())
		if err := unmarshalQueryMessage(element, valueField.Message().FullName(), rawValue); err != nil {
			return err
		}
		value = protoreflect.ValueOfMessage(element)
	} else {
		value, err = parseScalar(valueField, rawValue)
		if err != nil {
			return err
		}
	}
	msg.Mutable(fd).Map().Set(key.MapKey(), value)
	return nil
}

func unmarshalQueryMessage(msg protoreflect.Message, name protoreflect.FullName, raw string) error {
	var data []byte
	switch name {
	case "google.protobuf.Struct", "google.protobuf.Value", "google.protobuf.ListValue":
		data = []byte(raw)
	case "google.protobuf.Timestamp", "google.protobuf.Duration", "google.protobuf.FieldMask",
		"google.protobuf.DoubleValue", "google.protobuf.FloatValue",
		"google.protobuf.Int64Value", "google.protobuf.UInt64Value",
		"google.protobuf.Int32Value", "google.protobuf.UInt32Value",
		"google.protobuf.BoolValue", "google.protobuf.StringValue", "google.protobuf.BytesValue":
		data = []byte(strconv.Quote(raw))
	default:
		return fmt.Errorf("message field %s must be populated through a nested field", name)
	}
	return protojson.Unmarshal(data, msg.Interface())
}
