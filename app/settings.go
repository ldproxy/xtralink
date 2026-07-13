package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ldproxy/xtralink/lib/workflows"
)

var nonAlphanumeric = regexp.MustCompile(`[^A-Z0-9]`)

type Settings struct {
	TargetDir string               `yaml:"targetDir,omitempty"`
	Packages  []Package            `yaml:"packages"`
	Workflows []workflows.Workflow `yaml:"workflows,omitempty"`
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

func (s *Settings) GetPackage(id string) (*Package, error) {
	for i := range s.Packages {
		if s.Packages[i].Id == id {
			return &s.Packages[i], nil
		}
	}
	return nil, fmt.Errorf("package with id %q not found", id)
}

func (s *Settings) HasWorkflow(id string) bool {
	for _, w := range s.Workflows {
		if w.Id == id {
			return true
		}
	}
	return false
}

func (s *Settings) GetWorkflow(id string) (*workflows.Workflow, error) {
	for i := range s.Workflows {
		if s.Workflows[i].Id == id {
			return &s.Workflows[i], nil
		}
	}
	return nil, fmt.Errorf("workflow with id %q not found", id)
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
		case "GIT", "OCI", "S3", "FS":
		default:
			return fmt.Errorf("packages[%d].type=%q is invalid (allowed: GIT, OCI, S3, FS)", i, r.Type)
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

	if err := validateWorkflows(settings.Workflows); err != nil {
		return err
	}

	return nil
}

// validateWorkflows checks only what the generic Workflow/Step model itself
// can verify (id uniqueness) - action-aware checks (does this action type
// exist, do its pkg/from/to params reference real packages of the right
// type) need the Action registry, which is only built once app.AppContext
// exists (Drivers/Jobs wiring). Settings is loaded before that, and
// app/workflows can't be imported from here without an import cycle
// (app/workflows needs *app.AppContext). Those checks run instead right
// before a workflow executes, in app/workflows.Validate.
func validateWorkflows(wfs []workflows.Workflow) error {
	seenWorkflowIds := map[string]bool{}

	for wi, wf := range wfs {
		if wf.Id == "" {
			return fmt.Errorf("workflows[%d].id is required", wi)
		}
		if seenWorkflowIds[wf.Id] {
			return fmt.Errorf("workflows[%d]: duplicate workflow id %q", wi, wf.Id)
		}
		seenWorkflowIds[wf.Id] = true

		seenParamNames := map[string]bool{}
		for pi, p := range wf.Params {
			if p.Name == "" {
				return fmt.Errorf("workflows[%d] (%s): params[%d].name is required", wi, wf.Id, pi)
			}
			if seenParamNames[p.Name] {
				return fmt.Errorf("workflows[%d] (%s): duplicate param name %q", wi, wf.Id, p.Name)
			}
			seenParamNames[p.Name] = true
		}

		seenStepIds := map[string]bool{}
		for si, step := range wf.Steps {
			id := step.EffectiveId(si)
			if seenStepIds[id] {
				return fmt.Errorf("workflows[%d] (%s): duplicate step id %q", wi, wf.Id, id)
			}
			seenStepIds[id] = true

			if step.Action == "" {
				return fmt.Errorf("workflows[%d].steps[%d].action is required", wi, si)
			}
		}
	}

	return nil
}

func envByRemoteID(remoteID, base string) string {
	id := strings.ToUpper(strings.TrimSpace(remoteID))
	b := strings.ToUpper(strings.TrimSpace(base))
	if id == "" || b == "" {
		return ""
	}
	id = nonAlphanumeric.ReplaceAllString(id, "_")
	return strings.TrimSpace(os.Getenv(b + "_" + id))
}
