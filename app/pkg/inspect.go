package pkg

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/drivers"
)

type InspectResult struct {
	Entities      InspectEntities       `json:"entities"`
	Substitutions []InspectSubstitution `json:"substitutions"`
	DataSources   []InspectDataSource   `json:"data-sources"`
}

type InspectEntities struct {
	Services  map[string]int `json:"services"`
	Providers map[string]int `json:"providers"`
}

type InspectSubstitution struct {
	File    string  `json:"file"`
	Path    string  `json:"path"`
	Name    string  `json:"name"`
	Default *string `json:"default,omitempty"`
}

type InspectDataSource struct {
	Type       string                 `json:"type"`
	Name       string                 `json:"name"`
	Paths      []string               `json:"paths,omitempty"`
	Connection *InspectDataSourceConn `json:"connection,omitempty"`
	Usages     []string               `json:"usages"`
}

type InspectDataSourceConn struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Password string `json:"password"`
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

	substitutions, err := inspectSubstitutions(inspectRoot)
	if err != nil {
		return nil, err
	}

	dataSources, err := inspectDataSources(inspectRoot, substitutions)
	if err != nil {
		return nil, err
	}

	return &InspectResult{
		Entities:      entities,
		Substitutions: substitutions,
		DataSources:   dataSources,
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
