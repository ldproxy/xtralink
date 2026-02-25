package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

func LoadSettings(path string) (*Settings, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("config path is empty")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("could not read config (%s): %w", path, err)
	}

	var settings Settings
	if err := yaml.Unmarshal(raw, &settings); err != nil {
		return nil, fmt.Errorf("could not parse yaml: %w", err)
	}

	if err := validateAndNormalize(&settings); err != nil {
		return nil, err
	}

	return &settings, nil
}

func validateAndNormalize(settings *Settings) error {
	if settings == nil {
		return errors.New("settings is nil")
	}

	if len(settings.Remotes) == 0 {
		return errors.New("at least one remote is required")
	}

	settings.TargetDir = strings.TrimSpace(settings.TargetDir)
	if settings.TargetDir == "" {
		settings.TargetDir = "."
	}

	for i := range settings.Remotes {
		r := &settings.Remotes[i]

		r.Type = strings.ToUpper(strings.TrimSpace(r.Type))
		r.Id = strings.TrimSpace(r.Id)
		r.URL = strings.TrimSpace(r.URL)
		r.Tag = strings.TrimSpace(r.Tag)
		r.User = strings.TrimSpace(r.User)
		r.Password = strings.TrimSpace(r.Password)
		r.Path = strings.TrimSpace(r.Path)
		r.LocalPath = strings.TrimSpace(r.LocalPath)

		if r.Type == "" {
			return fmt.Errorf("remotes[%d].type is required", i)
		}
		switch r.Type {
		case "GIT", "OCI", "S3":
		default:
			return fmt.Errorf("remotes[%d].type=%q is invalid (allowed: GIT, OCI, S3)", i, r.Type)
		}

		if r.URL == "" {
			return fmt.Errorf("remotes[%d].url is required", i)
		}
		if r.LocalPath == "" {
			return fmt.Errorf("remotes[%d].localPath is required", i)
		}

		if r.Type == "GIT" && r.Tag == "" {
			r.Tag = "main"
		}

		r.ResolvedLocalPath = filepath.Join(settings.TargetDir, r.LocalPath)
	}

	return nil
}
