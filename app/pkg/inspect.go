package pkg

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/drivers"
	"gopkg.in/yaml.v3"
)

type InspectResult struct {
	Entities      InspectEntities  `json:"entities"`
	Substitutions []map[string]any `json:"substitutions"`
	DataSources   []map[string]any `json:"data-sources"`
}

type InspectEntities struct {
	Services  map[string]int `json:"services"`
	Providers map[string]int `json:"providers"`
}

func Inspect(appCtx *app.AppContext, pkgID string) (*InspectResult, error) {
	if strings.TrimSpace(pkgID) == "" {
		return nil, fmt.Errorf("package id is empty")
	}
	if appCtx == nil || appCtx.Settings == nil {
		return nil, fmt.Errorf("settings is nil")
	}

	pkgCfg, err := findPackageByID(appCtx.Settings, pkgID)
	if err != nil {
		return nil, err
	}

	inspectRoot, cleanup, err := syncPackageToInspectTemp(appCtx, *pkgCfg)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	entities, err := inspectEntities(inspectRoot)
	if err != nil {
		return nil, err
	}

	return &InspectResult{
		Entities:      entities,
		Substitutions: []map[string]any{},
		DataSources:   []map[string]any{},
	}, nil
}

func syncPackageToInspectTemp(appCtx *app.AppContext, p app.Package) (string, func(), error) {
	if appCtx.Drivers == nil {
		return "", nil, fmt.Errorf("drivers factory is nil")
	}

	tmpDir, err := os.MkdirTemp("", "xtrasync-inspect-")
	if err != nil {
		return "", nil, fmt.Errorf("could not create inspect temp directory: %w", err)
	}

	cleanup := func() {
		_ = os.RemoveAll(tmpDir)
	}

	inspectRoot := filepath.Join(tmpDir, p.Id)

	driver, err := appCtx.Drivers.For(p.Type)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("package driver resolution failed: %w", err)
	}

	remote := drivers.Remote{
		Type:              p.Type,
		ID:                p.Id,
		URL:               p.URL,
		Tag:               p.Tag,
		User:              p.User,
		Password:          p.Password,
		Path:              p.Path,
		ResolvedLocalPath: inspectRoot,
	}

	if err := driver.Sync(remote); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("package fetch failed: %w", err)
	}

	return inspectRoot, cleanup, nil
}

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

func findPackageByID(settings *app.Settings, id string) (*app.Package, error) {
	if settings == nil {
		return nil, fmt.Errorf("settings is nil")
	}
	target := strings.TrimSpace(id)
	if target == "" {
		return nil, fmt.Errorf("package id is empty")
	}

	for i := range settings.Packages {
		if strings.TrimSpace(settings.Packages[i].Id) == target {
			return &settings.Packages[i], nil
		}
	}

	return nil, fmt.Errorf("package with id %q not found", id)
}
