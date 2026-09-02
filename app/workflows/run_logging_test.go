package workflows

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/drivers"
	"github.com/ldproxy/xtralink/lib/lock"
)

// runCapturingLogs runs a workflow against a throwaway config and returns
// every log line it produced, decoded.
func runCapturingLogs(t *testing.T, config, workflowId string, level zerolog.Level) ([]map[string]any, error) {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), ".xtrasync.yml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatalf("WriteFile config: %v", err)
	}
	settings, err := app.LoadSettings(configPath)
	if err != nil {
		t.Fatalf("LoadSettings: %v", err)
	}

	var buf bytes.Buffer
	appCtx := &app.AppContext{
		Logger:   zerolog.New(&buf).Level(level),
		Settings: settings,
		Drivers:  drivers.NewFactory(),
		Jobs:     &fakeBackend{},
		Locks:    lock.NoopLocker{},
	}

	runErr := Run(appCtx, workflowId, nil)

	var entries []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		if unmarshalErr := json.Unmarshal([]byte(line), &entry); unmarshalErr != nil {
			t.Fatalf("log line %q is not JSON: %v", line, unmarshalErr)
		}
		entries = append(entries, entry)
	}
	return entries, runErr
}

// echoWorkflowConfig is a two-step workflow that touches nothing but the
// commands it runs, so these tests stay about the logging.
func echoWorkflowConfig(t *testing.T) string {
	t.Helper()

	return `
settings:
  targetDir: ` + t.TempDir() + `
packages:
  - id: unused
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: noisy
    steps:
      - id: greet
        action: cmd:exec
        cmd: echo hello
      - id: greet-again
        action: cmd:exec
        cmd: echo hello again
`
}

func TestRun_LogsOneInfoLineOnSuccess(t *testing.T) {
	entries, err := runCapturingLogs(t, echoWorkflowConfig(t), "noisy", zerolog.InfoLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected exactly 1 log line at info level, got %+v", entries)
	}
	entry := entries[0]
	if entry["message"] != "workflow completed" {
		t.Errorf("message = %v, want \"workflow completed\"", entry["message"])
	}
	if entry["workflow"] != "noisy" {
		t.Errorf("workflow = %v, want \"noisy\"", entry["workflow"])
	}
	if entry["steps"] != float64(2) {
		t.Errorf("steps = %v, want 2", entry["steps"])
	}
	if _, ok := entry["duration"]; !ok {
		t.Errorf("expected a duration field, got %+v", entry)
	}
}

func TestRun_LogsNoSuccessLineWhenAStepFails(t *testing.T) {
	config := `
settings:
  targetDir: ` + t.TempDir() + `
packages:
  - id: unused
    type: FS
    url: ` + t.TempDir() + `

workflows:
  - id: doomed
    steps:
      - id: fine
        action: cmd:exec
        cmd: echo fine
      - id: broken
        action: cmd:exec
        cmd: false
`
	entries, err := runCapturingLogs(t, config, "doomed", zerolog.DebugLevel)
	if err == nil {
		t.Fatal("expected the run to fail")
	}

	// The failure is logged once at the CLI boundary (s. cli/flow.go), not
	// here - what matters is that the steps leading up to it are visible and
	// that nothing claims success.
	var messages []string
	for _, entry := range entries {
		message, _ := entry["message"].(string)
		messages = append(messages, message)
		if message == "workflow completed" {
			t.Errorf("a failed run must not log a completion line, got %+v", entries)
		}
	}
	if !strings.Contains(strings.Join(messages, "\n"), "step finished") {
		t.Errorf("expected the successful step to be visible, got %v", messages)
	}
}

func TestRun_StepLogLinesCarryTheWorkflowId(t *testing.T) {
	entries, err := runCapturingLogs(t, echoWorkflowConfig(t), "noisy", zerolog.DebugLevel)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, entry := range entries {
		if entry["workflow"] != "noisy" {
			t.Fatalf("log line %+v is missing workflow=noisy", entry)
		}
	}
}
