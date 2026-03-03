package pkg

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

func inspectEntities(root string) (InspectEntities, error) {
	services := map[string]int{}
	providers := map[string]int{}

	servicesDir := filepath.Join(root, "entities", "instances", "services")
	providersDir := filepath.Join(root, "entities", "instances", "providers")

	if err := countEntityTypes(servicesDir, func(doc map[string]any) {
		if v := asString(doc["serviceType"]); v != "" {
			services[v]++
		}
	}); err != nil {
		return InspectEntities{}, err
	}

	if err := countEntityTypes(providersDir, func(doc map[string]any) {
		base := asString(doc["providerType"])
		if base == "" {
			return
		}
		sub := detectProviderSubType(doc)
		key := base
		if sub != "" {
			key = base + "/" + sub
		}
		providers[key]++
	}); err != nil {
		return InspectEntities{}, err
	}

	return InspectEntities{Services: services, Providers: providers}, nil
}

func countEntityTypes(dir string, apply func(map[string]any)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("could not read directory %s: %w", dir, err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := strings.ToLower(e.Name())
		if !strings.HasSuffix(n, ".yml") && !strings.HasSuffix(n, ".yaml") {
			continue
		}

		fullPath := filepath.Join(dir, e.Name())
		raw, err := os.ReadFile(fullPath)
		if err != nil {
			return fmt.Errorf("could not read %s: %w", fullPath, err)
		}

		var doc map[string]any
		if err := yaml.Unmarshal(raw, &doc); err != nil {
			return fmt.Errorf("could not parse yaml (%s): %w", fullPath, err)
		}
		apply(doc)
	}

	return nil
}

func detectProviderSubType(doc map[string]any) string {
	if v := asString(doc["providerSubType"]); v != "" {
		return v
	}
	if v := asString(doc["featureProviderType"]); v != "" {
		return v
	}
	if v := asString(doc["tileProviderType"]); v != "" {
		return v
	}

	keys := make([]string, 0, len(doc))
	for k := range doc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if strings.EqualFold(k, "providerType") {
			continue
		}
		if strings.EqualFold(k, "providerSubType") {
			if v := asString(doc[k]); v != "" {
				return v
			}
			continue
		}
		if strings.HasSuffix(k, "ProviderType") {
			if v := asString(doc[k]); v != "" {
				return v
			}
		}
	}

	return ""
}

func asString(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}
