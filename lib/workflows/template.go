package workflows

import (
	"fmt"
	"regexp"
	"strings"
)

// placeholderPattern matches ${a.b.c} anywhere in a string.
var placeholderPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// wholePlaceholderPattern matches a string that is exactly one placeholder,
// nothing else.
var wholePlaceholderPattern = regexp.MustCompile(`^\$\{([^}]+)\}$`)

// ResolveValue recursively resolves ${...} placeholders in v against vars.
// Strings that are exactly one placeholder resolve to the underlying value
// as-is (so a job:push input can carry a number, list, etc., not just
// strings); placeholders embedded in a larger string are stringified.
// Unknown paths are an error, not an empty string, so config typos surface
// immediately instead of silently writing "" into a Job input.
func ResolveValue(v any, vars map[string]any) (any, error) {
	switch val := v.(type) {
	case string:
		return resolveString(val, vars)
	case map[string]any:
		out := make(map[string]any, len(val))
		for k, item := range val {
			resolved, err := ResolveValue(item, vars)
			if err != nil {
				return nil, err
			}
			out[k] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(val))
		for i, item := range val {
			resolved, err := ResolveValue(item, vars)
			if err != nil {
				return nil, err
			}
			out[i] = resolved
		}
		return out, nil
	default:
		return val, nil
	}
}

func resolveString(s string, vars map[string]any) (any, error) {
	if m := wholePlaceholderPattern.FindStringSubmatch(s); m != nil {
		return lookupPath(m[1], vars)
	}

	var resolveErr error
	result := placeholderPattern.ReplaceAllStringFunc(s, func(match string) string {
		path := match[2 : len(match)-1] // strip "${" and "}"
		value, err := lookupPath(path, vars)
		if err != nil {
			resolveErr = err
			return match
		}
		return fmt.Sprintf("%v", value)
	})
	if resolveErr != nil {
		return nil, resolveErr
	}
	return result, nil
}

// lookupPath resolves a dotted path (e.g. "outputs.input.path" or
// "packages.bar.url") against vars, one map lookup per segment.
func lookupPath(path string, vars map[string]any) (any, error) {
	segments := strings.Split(path, ".")
	var current any = vars

	for i, seg := range segments {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("cannot resolve %q: %q is not an object", path, strings.Join(segments[:i], "."))
		}
		value, ok := m[seg]
		if !ok {
			return nil, fmt.Errorf("cannot resolve %q: no such key %q", path, seg)
		}
		current = value
	}

	return current, nil
}
