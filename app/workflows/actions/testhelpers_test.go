package actions

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/drivers"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/model"
)

// fakeBackend is a minimal in-memory jobs.Backend stub, mirroring
// app/jobs/testhelpers_test.go's - these tests only ever exercise PushJob
// (via JobPushAction), everything else is unused.
type fakeBackend struct {
	pushedJob *model.Job
}

func (f *fakeBackend) IsEnabled() bool { return true }
func (f *fakeBackend) PushJob(job *model.Job) error {
	return f.PushJobListen(job, jobs.NoopJobListener{})
}
func (f *fakeBackend) PushJobListen(job *model.Job, onProgress jobs.JobListener) error {
	f.pushedJob = job
	return nil
}
func (f *fakeBackend) PushPartialJob(partialJob *model.PartialJob, untake bool) error { return nil }
func (f *fakeBackend) Take(partialJobType, executor string) (*model.PartialJob, error) {
	return nil, nil
}
func (f *fakeBackend) Done(partialJobID string) error                       { return nil }
func (f *fakeBackend) Error(partialJobID, message string, retry bool) error { return nil }
func (f *fakeBackend) GetJobs() ([]*model.Job, error)                       { return nil, nil }
func (f *fakeBackend) GetJob(id string) (*model.Job, error)                 { return nil, nil }
func (f *fakeBackend) GetPartialJob(id string) (*model.PartialJob, error) { return nil, nil }
func (f *fakeBackend) GetOpen(partialJobType string) ([]*model.PartialJob, error) {
	return nil, nil
}
func (f *fakeBackend) GetTaken() ([]*model.PartialJob, error)  { return nil, nil }
func (f *fakeBackend) GetFailed() ([]*model.PartialJob, error) { return nil, nil }
func (f *fakeBackend) StartJob(jobID string) error             { return nil }
func (f *fakeBackend) SetProgressDetails(jobID string, details map[string]any) error {
	return nil
}
func (f *fakeBackend) SetOutput(jobID, key string, value model.OutputValue) error {
	return nil
}
func (f *fakeBackend) InitJob(jobID string, totalDelta int, updates []model.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdateJob(jobID string, currentDelta int, updates []model.ProgressUpdate) error {
	return nil
}
func (f *fakeBackend) UpdatePartialJob(partialJobID string, currentDelta int) error { return nil }

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
