package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

type Settings struct {
	TargetDir string    `yaml:"targetDir,omitempty"`
	Packages  []Package `yaml:"packages"`
}

type Package struct {
	Type      string `yaml:"type"`
	Id        string `yaml:"id"`
	URL       string `yaml:"url"`
	Tag       string `yaml:"tag,omitempty"`
	User      string `yaml:"user,omitempty"`
	Password  string `yaml:"password,omitempty"`
	Path      string `yaml:"path,omitempty"`
	LocalPath string `yaml:"localPath,omitempty"`

	ResolvedLocalPath string `yaml:"-"`
}

func (s *Settings) HasPackage(id string) bool {
	for _, r := range s.Packages {
		if r.Id == id {
			return true
		}
	}
	return false
}

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

	if len(settings.Packages) == 0 {
		return errors.New("at least one package is required")
	}

	settings.TargetDir = strings.TrimSpace(settings.TargetDir)
	if settings.TargetDir == "" {
		settings.TargetDir = "."
	}

	for i := range settings.Packages {
		r := &settings.Packages[i]

		r.Type = strings.ToUpper(strings.TrimSpace(r.Type))
		r.Id = strings.TrimSpace(r.Id)
		r.URL = strings.TrimSpace(r.URL)
		r.Tag = strings.TrimSpace(r.Tag)
		r.User = strings.TrimSpace(r.User)
		r.Password = strings.TrimSpace(r.Password)
		r.Path = strings.TrimSpace(r.Path)
		r.LocalPath = strings.TrimSpace(r.LocalPath)

		if r.Type == "" {
			return fmt.Errorf("packages[%d].type is required", i)
		}
		switch r.Type {
		case "GIT", "OCI", "S3":
		default:
			return fmt.Errorf("packages[%d].type=%q is invalid (allowed: GIT, OCI, S3)", i, r.Type)
		}

		if r.URL == "" {
			return fmt.Errorf("packages[%d].url is required", i)
		}
		if r.Id == "" {
			return fmt.Errorf("packages[%d].id is required", i)
		}
		if r.LocalPath == "" {
			r.LocalPath = r.Id
		}

		if r.Type == "GIT" && r.Tag == "" {
			r.Tag = "main"
		}

		if r.User == "" {
			r.User = envByRemoteID(r.Id, "user")
		}
		if r.Password == "" {
			r.Password = envByRemoteID(r.Id, "password")
		}

		r.ResolvedLocalPath = filepath.Join(settings.TargetDir, r.LocalPath)
	}

	return nil
}

func envByRemoteID(remoteID, base string) string {
	id := strings.ToUpper(strings.TrimSpace(remoteID))
	b := strings.ToUpper(strings.TrimSpace(base))
	if id == "" || b == "" {
		return ""
	}
	return strings.TrimSpace(os.Getenv(b + "_" + id))
}
