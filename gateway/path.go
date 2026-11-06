package gateway

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// pathTemplate is the deliberately small subset of google.api.http templates
// that Sweet serves: a variable occupies one complete, non-empty path segment.
// Wildcards and custom verbs need a different routing model and are not hidden
// inside this parser as a half-supported special case.
type pathTemplate struct {
	raw      string
	segments []pathSegment
	literals int
}

type pathSegment struct {
	literal string
	field   string
	fields  fieldPath
}

func parsePathTemplate(raw string) (*pathTemplate, error) {
	t := &pathTemplate{raw: raw}
	if raw == "/" {
		return t, nil
	}
	seen := make(map[string]struct{})
	for _, part := range strings.Split(strings.TrimPrefix(raw, "/"), "/") {
		segment := pathSegment{literal: part}
		if strings.ContainsAny(part, "{}") {
			if len(part) < 3 || part[0] != '{' || part[len(part)-1] != '}' ||
				strings.Count(part, "{") != 1 || strings.Count(part, "}") != 1 {
				return nil, fmt.Errorf("path variable must occupy one whole segment")
			}
			segment.field = part[1 : len(part)-1]
			segment.literal = ""
			if segment.field == "" || strings.Contains(segment.field, "*") || strings.Contains(segment.field, "=") {
				return nil, fmt.Errorf("unsupported path variable %q", segment.field)
			}
			if _, ok := seen[segment.field]; ok {
				return nil, fmt.Errorf("path field %q is bound more than once", segment.field)
			}
			seen[segment.field] = struct{}{}
		} else {
			t.literals++
		}
		t.segments = append(t.segments, segment)
	}
	return t, nil
}

func (t *pathTemplate) templated() bool {
	if t == nil {
		return false
	}
	for _, segment := range t.segments {
		if segment.field != "" {
			return true
		}
	}
	return false
}

func (t *pathTemplate) key() string {
	parts := make([]string, 0, len(t.segments))
	for _, segment := range t.segments {
		if segment.field != "" {
			parts = append(parts, "{}")
		} else {
			parts = append(parts, segment.literal)
		}
	}
	return "/" + strings.Join(parts, "/")
}

func (t *pathTemplate) bind(md protoreflect.MessageDescriptor) error {
	for i := range t.segments {
		segment := &t.segments[i]
		if segment.field == "" {
			continue
		}
		fields, err := resolveFieldPath(md, segment.field)
		if err != nil {
			return fmt.Errorf("path field %q: %w", segment.field, err)
		}
		last := fields[len(fields)-1]
		if last.IsList() || last.IsMap() || last.Kind() == protoreflect.MessageKind || last.Kind() == protoreflect.GroupKind {
			return fmt.Errorf("path field %q must be a singular scalar", segment.field)
		}
		segment.fields = fields
	}
	return nil
}

func (t *pathTemplate) match(canonicalPath string) (map[string]string, bool) {
	if t == nil {
		return nil, false
	}
	var parts []string
	if canonicalPath != "/" {
		parts = strings.Split(strings.TrimPrefix(canonicalPath, "/"), "/")
	}
	if len(parts) != len(t.segments) {
		return nil, false
	}
	values := make(map[string]string)
	for i, segment := range t.segments {
		if segment.field == "" {
			if parts[i] != segment.literal {
				return nil, false
			}
			continue
		}
		if parts[i] == "" {
			return nil, false
		}
		values[segment.field] = parts[i]
	}
	return values, true
}

func (t *pathTemplate) occupied() []fieldPath {
	if t == nil {
		return nil
	}
	var paths []fieldPath
	for _, segment := range t.segments {
		if segment.field != "" {
			paths = append(paths, segment.fields)
		}
	}
	return paths
}
