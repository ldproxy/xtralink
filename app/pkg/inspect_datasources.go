package pkg

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type dataSourceAcc struct {
	Type       string
	Name       string
	Paths      map[string]struct{}
	Usages     map[string]struct{}
	Connection *InspectDataSourceConn
}

func inspectDataSources(root string, substitutions []InspectSubstitution) ([]InspectDataSource, error) {
	defaultByLocation := substitutionDefaultsByLocation(substitutions)
	acc := map[string]*dataSourceAcc{}

	if err := collectFileSystemDataSources(root, acc); err != nil {
		return nil, err
	}
	if err := collectProviderDataSourceUsages(root, defaultByLocation, acc); err != nil {
		return nil, err
	}

	result := make([]InspectDataSource, 0, len(acc))
	for _, ds := range acc {
		result = append(result, InspectDataSource{
			Type:       ds.Type,
			Name:       ds.Name,
			Paths:      setToSortedSlice(ds.Paths),
			Connection: ds.Connection,
			Usages:     setToSortedSlice(ds.Usages),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Type != result[j].Type {
			return result[i].Type < result[j].Type
		}
		return result[i].Name < result[j].Name
	})

	return result, nil
}

func substitutionDefaultsByLocation(substitutions []InspectSubstitution) map[string]string {
	m := map[string]string{}
	for _, s := range substitutions {
		if s.Default == nil {
			continue
		}
		m[s.File+"|"+s.Path] = *s.Default
	}
	return m
}

func collectFileSystemDataSources(root string, acc map[string]*dataSourceAcc) error {
	featuresRoot := filepath.Join(root, "resources", "features")
	if err := walkDirIfExists(featuresRoot, func(absPath string) error {
		ext := strings.ToLower(filepath.Ext(absPath))
		if ext != ".gpkg" {
			return nil
		}

		rel, err := filepath.Rel(root, absPath)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		name := strings.TrimSuffix(filepath.Base(absPath), filepath.Ext(absPath))

		ds := ensureDataSource(acc, "GPKG", name)
		ds.Paths[rel] = struct{}{}
		return nil
	}); err != nil {
		return fmt.Errorf("could not scan resources/features: %w", err)
	}

	dbRoot := filepath.Join(root, "db")
	if err := walkDirIfExists(dbRoot, func(absPath string) error {
		rel, err := filepath.Rel(root, absPath)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		name := deriveDumpNameFromPath(filepath.Base(absPath))
		if name == "" {
			return nil
		}

		ds := ensureDataSource(acc, "PGIS/DUMP", name)
		ds.Paths[rel] = struct{}{}
		return nil
	}); err != nil {
		return fmt.Errorf("could not scan db: %w", err)
	}

	return nil
}

func collectProviderDataSourceUsages(root string, defaultByLocation map[string]string, acc map[string]*dataSourceAcc) error {
	providersRoot := filepath.Join(root, "entities", "instances", "providers")
	return walkDirIfExists(providersRoot, func(absPath string) error {
		n := strings.ToLower(filepath.Base(absPath))
		if !strings.HasSuffix(n, ".yml") && !strings.HasSuffix(n, ".yaml") {
			return nil
		}

		rel, err := filepath.Rel(root, absPath)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		doc, err := readYAMLDocumentAsMap(absPath)
		if err != nil {
			return err
		}

		id := effectiveString(rel, "id", getStringAtPath(doc, "id"), defaultByLocation)
		dialect := strings.ToUpper(effectiveString(rel, "connectionInfo.dialect", getStringAtPath(doc, "connectionInfo.dialect"), defaultByLocation))
		database := effectiveString(rel, "connectionInfo.database", getStringAtPath(doc, "connectionInfo.database"), defaultByLocation)
		host := effectiveString(rel, "connectionInfo.host", getStringAtPath(doc, "connectionInfo.host"), defaultByLocation)
		user := effectiveString(rel, "connectionInfo.user", getStringAtPath(doc, "connectionInfo.user"), defaultByLocation)
		password := effectiveString(rel, "connectionInfo.password", getStringAtPath(doc, "connectionInfo.password"), defaultByLocation)

		matchedDump := findMatchingDumpDataSource(acc, id, database)
		if matchedDump != nil {
			matchedDump.Usages[rel] = struct{}{}
		}

		if dialect == "GPKG" {
			gpkgName := strings.TrimSuffix(path.Base(database), path.Ext(database))
			if gpkgName != "" {
				ds := ensureDataSource(acc, "GPKG", gpkgName)
				ds.Usages[rel] = struct{}{}
			}
		}

		if isPGISDialect(dialect) || ((host != "" || user != "" || password != "") && dialect != "GPKG") {
			name := strings.TrimSpace(database)
			if name == "" {
				name = strings.TrimSpace(id)
			}
			if name != "" {
				if matchedDump == nil || matchedDump.Name != name {
					ds := ensureDataSource(acc, "PGIS/REF", name)
					ds.Connection = &InspectDataSourceConn{Host: host, User: user, Password: password}
					ds.Usages[rel] = struct{}{}
				}
			}
		}

		return nil
	})
}

func walkDirIfExists(root string, onFile func(absPath string) error) error {
	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		return onFile(p)
	})
}

func readYAMLDocumentAsMap(absPath string) (map[string]any, error) {
	raw, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("could not read yaml file (%s): %w", absPath, err)
	}

	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("could not parse yaml (%s): %w", absPath, err)
		}
		if len(doc) > 0 {
			return doc, nil
		}
	}

	return map[string]any{}, nil
}

func getStringAtPath(doc map[string]any, dottedPath string) string {
	current := any(doc)
	for _, k := range strings.Split(dottedPath, ".") {
		m, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = m[k]
		if !ok {
			return ""
		}
	}
	if s, ok := current.(string); ok {
		return strings.TrimSpace(s)
	}
	return ""
}

func effectiveString(file, yamlPath, raw string, defaults map[string]string) string {
	if d, ok := defaults[file+"|"+yamlPath]; ok {
		return d
	}
	return strings.TrimSpace(raw)
}

func deriveDumpNameFromPath(base string) string {
	b := strings.TrimSpace(base)
	lower := strings.ToLower(b)
	switch {
	case strings.HasSuffix(lower, ".ddl.sql"):
		return b[:len(b)-len(".ddl.sql")]
	case strings.HasSuffix(lower, ".dml.sql"):
		return b[:len(b)-len(".dml.sql")]
	case strings.HasSuffix(lower, ".sql.gz"):
		return b[:len(b)-len(".sql.gz")]
	case strings.HasSuffix(lower, ".sql"):
		return b[:len(b)-len(".sql")]
	default:
		return ""
	}
}

func isPGISDialect(dialect string) bool {
	d := strings.ToUpper(strings.TrimSpace(dialect))
	return strings.Contains(d, "PGIS") || strings.Contains(d, "POSTGRES") || strings.Contains(d, "POSTGIS")
}

func ensureDataSource(acc map[string]*dataSourceAcc, typ, name string) *dataSourceAcc {
	key := typ + "|" + name
	if ds, ok := acc[key]; ok {
		return ds
	}
	ds := &dataSourceAcc{
		Type:   typ,
		Name:   name,
		Paths:  map[string]struct{}{},
		Usages: map[string]struct{}{},
	}
	acc[key] = ds
	return ds
}

func getDataSource(acc map[string]*dataSourceAcc, typ, name string) *dataSourceAcc {
	return acc[typ+"|"+name]
}

func setToSortedSlice(set map[string]struct{}) []string {
	if len(set) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func findMatchingDumpDataSource(acc map[string]*dataSourceAcc, id, database string) *dataSourceAcc {
	candidates := make([]string, 0, 3)
	if v := strings.TrimSpace(database); v != "" {
		candidates = append(candidates, v)
		if derived := deriveDumpNameFromPath(path.Base(v)); derived != "" {
			candidates = append(candidates, derived)
		}
	} else if v := strings.TrimSpace(id); v != "" {
		// Fallback only if no database is configured.
		candidates = append(candidates, v)
	}

	seen := map[string]struct{}{}
	for _, c := range candidates {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		if ds := getDataSource(acc, "PGIS/DUMP", c); ds != nil {
			return ds
		}
	}
	return nil
}
