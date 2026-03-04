package pkg

import (
	"fmt"
	"strings"
)

// PackageInspector provides inspect behavior for a specific package config type (e.g., LDP, xtraserver).
type PackageInspector interface {
	Name() string
	Inspect(root string) (*InspectResult, error)
}

type ldpInspector struct{}

func (i ldpInspector) Name() string {
	return "ldp"
}

func (i ldpInspector) Inspect(root string) (*InspectResult, error) {
	entities, err := inspectEntities(root)
	if err != nil {
		return nil, err
	}

	substitutions, err := inspectSubstitutions(root)
	if err != nil {
		return nil, err
	}

	dataSources, err := inspectDataSources(root, substitutions)
	if err != nil {
		return nil, err
	}

	return &InspectResult{
		Entities:      entities,
		Substitutions: substitutions,
		DataSources:   dataSources,
	}, nil
}

func resolveInspector(configType string) (PackageInspector, error) {
	// Future extension point:
	// add additional inspectors here (e.g., "xtraserver") once their
	// inspect implementation is available and configType is provided by package metadata.
	switch strings.ToLower(strings.TrimSpace(configType)) {
	case "ldp":
		return ldpInspector{}, nil
	default:
		return nil, fmt.Errorf("unsupported inspect config type %q", configType)
	}
}
