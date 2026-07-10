package workflows

import (
	"fmt"
	"strconv"
	"strings"
)

// ResolveParams merges CLI-provided overrides with each declared Param's
// default, coercing raw CLI strings to the declared Type. A Param that ends
// up with neither an override nor a default is simply left out of the
// result if it isn't Required - referencing ${params.<name>} for it later
// is then a normal "unknown path" template error, consistent with how
// outputs/packages already behave (s. template.go).
func ResolveParams(wf Workflow, overrides map[string]string) (map[string]any, error) {
	result := make(map[string]any, len(wf.Params))

	for _, p := range wf.Params {
		if raw, ok := overrides[p.Name]; ok {
			value, err := coerce(raw, p.Type)
			if err != nil {
				return nil, fmt.Errorf("parameter %q: %w", p.Name, err)
			}
			result[p.Name] = value
			continue
		}
		if p.Default != nil {
			result[p.Name] = p.Default
			continue
		}
		if p.Required {
			return nil, fmt.Errorf("missing required parameter %q", p.Name)
		}
	}

	return result, nil
}

// coerce converts a raw CLI-provided string to the declared param type.
// "" and "string" require no conversion; "int"/"integer" and "bool" get
// basic coercion. Any other declared type is an error - most likely a typo
// in the workflow config, better caught here than silently ignored.
func coerce(raw, typ string) (any, error) {
	switch strings.ToLower(strings.TrimSpace(typ)) {
	case "", "string":
		return raw, nil
	case "int", "integer":
		v, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("expected an integer, got %q", raw)
		}
		return v, nil
	case "bool", "boolean":
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, fmt.Errorf("expected a bool, got %q", raw)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported param type %q (supported: string, int, bool)", typ)
	}
}
