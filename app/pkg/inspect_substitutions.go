package pkg

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

var substitutionPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_.-]*)(?::-([^}]*))?\}`)

func inspectSubstitutions(root string) ([]InspectSubstitution, error) {
	results := make([]InspectSubstitution, 0)
	seen := map[string]struct{}{}

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		name := strings.ToLower(d.Name())
		if !strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml") {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return fmt.Errorf("could not make path relative (%s): %w", path, err)
		}
		rel = filepath.ToSlash(rel)

		if err := collectSubstitutionsFromFile(path, rel, &results, seen); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("could not inspect substitutions: %w", err)
	}

	sort.Slice(results, func(i, j int) bool {
		a, b := results[i], results[j]
		if a.File != b.File {
			return a.File < b.File
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Name < b.Name
	})

	return results, nil
}

func collectSubstitutionsFromFile(absPath, relPath string, out *[]InspectSubstitution, seen map[string]struct{}) error {
	raw, err := os.ReadFile(absPath)
	if err != nil {
		return fmt.Errorf("could not read yaml file (%s): %w", absPath, err)
	}

	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	for {
		var doc yaml.Node
		err := dec.Decode(&doc)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("could not parse yaml (%s): %w", absPath, err)
		}

		for _, c := range doc.Content {
			walkSubstitutionNode(c, "", relPath, out, seen)
		}
	}

	return nil
}

func walkSubstitutionNode(node *yaml.Node, path string, relFile string, out *[]InspectSubstitution, seen map[string]struct{}) {
	if node == nil {
		return
	}

	switch node.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			k := node.Content[i]
			v := node.Content[i+1]
			walkSubstitutionNode(v, joinYAMLPath(path, k.Value), relFile, out, seen)
		}
	case yaml.SequenceNode:
		for i, c := range node.Content {
			idxPath := fmt.Sprintf("%s[%d]", path, i)
			if path == "" {
				idxPath = fmt.Sprintf("[%d]", i)
			}
			walkSubstitutionNode(c, idxPath, relFile, out, seen)
		}
	case yaml.ScalarNode:
		if node.Tag != "!!str" {
			return
		}
		matches := substitutionPattern.FindAllStringSubmatch(node.Value, -1)
		for _, m := range matches {
			if len(m) < 2 {
				continue
			}

			name := strings.TrimSpace(m[1])
			if name == "" {
				continue
			}

			var def *string
			if len(m) >= 3 && m[2] != "" {
				d := m[2]
				def = &d
			}

			dedupKey := relFile + "|" + path + "|" + name + "|"
			if def != nil {
				dedupKey += *def
			}
			if _, exists := seen[dedupKey]; exists {
				continue
			}
			seen[dedupKey] = struct{}{}

			*out = append(*out, InspectSubstitution{
				File:    relFile,
				Path:    path,
				Name:    name,
				Default: def,
			})
		}
	}
}

func joinYAMLPath(base, next string) string {
	if base == "" {
		return next
	}
	if next == "" {
		return base
	}
	return base + "." + next
}
