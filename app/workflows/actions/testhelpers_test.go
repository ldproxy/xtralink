package actions

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/drivers"
	"github.com/ldproxy/xtrasync/lib/jobs"
)

// fakeBackend is a minimal in-memory jobs.Backend stub, mirroring
// app/jobs/testhelpers_test.go's - these tests only ever exercise
// PushJobSet (via JobPushAction), everything else is unused.
type fakeBackend struct {
	pushedJobSet *jobs.JobSet
}

func (f *fakeBackend) IsEnabled() bool { return true }
func (f *fakeBackend) PushJobSet(js *jobs.JobSet) error {
	f.pushedJobSet = js
	return nil
}
func (f *fakeBackend) PushJob(job *jobs.Job, untake bool) error         { return nil }
func (f *fakeBackend) Take(jobType, executor string) (*jobs.Job, error) { return nil, nil }
func (f *fakeBackend) Done(jobID string) error                          { return nil }
func (f *fakeBackend) Error(jobID, message string, retry bool) error    { return nil }
func (f *fakeBackend) GetSets() ([]*jobs.JobSet, error)                 { return nil, nil }
func (f *fakeBackend) GetSet(id string) (*jobs.JobSet, error)           { return nil, nil }
func (f *fakeBackend) GetOpen(jobType string) ([]*jobs.Job, error)      { return nil, nil }
func (f *fakeBackend) GetTaken() ([]*jobs.Job, error)                   { return nil, nil }
func (f *fakeBackend) GetFailed() ([]*jobs.Job, error)                  { return nil, nil }
func (f *fakeBackend) StartJobSet(jobSetID string) error                { return nil }
func (f *fakeBackend) SetProgressDetails(jobSetID string, details any) error {
	return nil
}
func (f *fakeBackend) SetOutput(jobSetID, key string, value jobs.OutputValue) error {
	return nil
}
func (f *fakeBackend) InitJobSet(jobSetID string, totalDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdateJobSet(jobSetID string, currentDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdateJob(jobID string, currentDelta int) error { return nil }

// newTestAppContext builds a real *app.AppContext around the given
// packages - no Redis, no network, matching the FS package type's whole
// point (testing without real S3).
func newTestAppContext(t *testing.T, targetDir string, pkgs ...app.Package) (*app.AppContext, *fakeBackend) {
	t.Helper()
	backend := &fakeBackend{}
	return &app.AppContext{
		Logger:   zerolog.Nop(),
		Settings: &app.Settings{TargetDir: targetDir, Packages: pkgs},
		Drivers:  drivers.NewFactory(),
		Jobs:     backend,
	}, backend
}

// fsPackage returns an FS package backed by a fresh temp directory (the
// "remote"), with ResolvedLocalPath computed exactly like
// app/settings.go's validateAndNormalize would.
func fsPackage(t *testing.T, id, targetDir string) app.Package {
	t.Helper()
	return app.Package{
		Id:                id,
		Type:              "FS",
		URL:               t.TempDir(),
		LocalPath:         id,
		ResolvedLocalPath: filepath.Join(targetDir, id),
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if string(got) != want {
		t.Errorf("content of %s = %q, want %q", path, got, want)
	}
}

func assertFileMissing(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected %s to not exist, stat err = %v", path, err)
	}
}
