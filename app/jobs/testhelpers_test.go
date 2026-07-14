package jobs

import "github.com/ldproxy/xtralink/lib/jobs"

// fakeBackend is a minimal in-memory jobs.Backend stub used to test this
// adapter layer (input validation, field mapping, error wrapping) in
// isolation. The real Redis-backed implementation already has its own
// dedicated integration tests in lib/jobs; these tests are not about the
// queue itself, only about the thin CLI-facing functions on top of it.
type fakeBackend struct {
	pushedJob  *jobs.Job
	pushJobErr error

	getJobResult *jobs.Job
	getJobErr    error

	getJobsResult []*jobs.Job
	getJobsErr    error
}

func (f *fakeBackend) IsEnabled() bool { return true }

func (f *fakeBackend) PushJob(job *jobs.Job) error {
	f.pushedJob = job
	return f.pushJobErr
}

func (f *fakeBackend) PushPartialJob(partialJob *jobs.PartialJob, untake bool) error { return nil }

func (f *fakeBackend) Take(partialJobType, executor string) (*jobs.PartialJob, error) {
	return nil, nil
}

func (f *fakeBackend) Done(partialJobID string) error { return nil }

func (f *fakeBackend) Error(partialJobID, message string, retry bool) error { return nil }

func (f *fakeBackend) GetJobs() ([]*jobs.Job, error) { return f.getJobsResult, f.getJobsErr }

func (f *fakeBackend) GetJob(id string) (*jobs.Job, error) { return f.getJobResult, f.getJobErr }

func (f *fakeBackend) GetOpen(partialJobType string) ([]*jobs.PartialJob, error) { return nil, nil }

func (f *fakeBackend) GetTaken() ([]*jobs.PartialJob, error) { return nil, nil }

func (f *fakeBackend) GetFailed() ([]*jobs.PartialJob, error) { return nil, nil }

func (f *fakeBackend) StartJob(jobID string) error { return nil }

func (f *fakeBackend) SetProgressDetails(jobID string, details any) error { return nil }

func (f *fakeBackend) SetOutput(jobID, key string, value jobs.OutputValue) error { return nil }

func (f *fakeBackend) InitJob(jobID string, totalDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}

func (f *fakeBackend) UpdateJob(jobID string, currentDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}

func (f *fakeBackend) UpdatePartialJob(partialJobID string, currentDelta int) error { return nil }
