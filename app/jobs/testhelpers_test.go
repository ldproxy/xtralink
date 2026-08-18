package jobs

import (
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/model"
)

// fakeBackend is a minimal in-memory jobs.Backend stub used to test this
// adapter layer (input validation, field mapping, error wrapping) in
// isolation. The real Redis-backed implementation already has its own
// dedicated integration tests in lib/jobs; these tests are not about the
// queue itself, only about the thin CLI-facing functions on top of it.
type fakeBackend struct {
	pushedJob  *model.Job
	pushJobErr error

	getJobResult *model.Job
	getJobErr    error

	getJobsResult []*model.Job
	getJobsErr    error
}

func (f *fakeBackend) IsEnabled() bool { return true }

func (f *fakeBackend) PushJob(job *model.Job) error {
	return f.PushJobListen(job, jobs.NoopJobListener{})
}
func (f *fakeBackend) PushJobListen(job *model.Job, onProgress jobs.JobListener) error {
	f.pushedJob = job
	return f.pushJobErr
}

func (f *fakeBackend) PushPartialJob(partialJob *model.PartialJob, untake bool) error { return nil }

func (f *fakeBackend) Take(partialJobType, executor string) (*model.PartialJob, error) {
	return nil, nil
}

func (f *fakeBackend) Done(partialJobID string) error { return nil }

func (f *fakeBackend) Error(partialJobID, message string, retry bool) error { return nil }

func (f *fakeBackend) GetJobs() ([]*model.Job, error) { return f.getJobsResult, f.getJobsErr }

func (f *fakeBackend) GetJob(id string) (*model.Job, error) { return f.getJobResult, f.getJobErr }

func (f *fakeBackend) GetPartialJob(id string) (*model.PartialJob, error) { return nil, nil }

func (f *fakeBackend) GetOpen(partialJobType string) ([]*model.PartialJob, error) {
	return nil, nil
}

func (f *fakeBackend) GetTaken() ([]*model.PartialJob, error) { return nil, nil }

func (f *fakeBackend) GetFailed() ([]*model.PartialJob, error) { return nil, nil }

func (f *fakeBackend) StartJob(jobID string) error { return nil }

func (f *fakeBackend) SetProgressDetails(jobID string, details map[string]any) error { return nil }

func (f *fakeBackend) SetOutput(jobID, key string, value model.OutputValue) error { return nil }
func (f *fakeBackend) SetOutputs(jobID string, outputs map[string]any) error {
	return nil
}

func (f *fakeBackend) InitJob(jobID string, totalDelta int, updates []model.ProgressUpdate) error {
	return nil
}

func (f *fakeBackend) UpdateJob(jobID string, currentDelta int, updates []model.ProgressUpdate) error {
	return nil
}

func (f *fakeBackend) UpdatePartialJob(partialJobID string, currentDelta int) error { return nil }
