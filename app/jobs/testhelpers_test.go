package jobs

import "github.com/ldproxy/xtralink/lib/jobs"

// fakeBackend is a minimal in-memory jobs.Backend stub used to test this
// adapter layer (input validation, field mapping, error wrapping) in
// isolation. The real Redis-backed implementation already has its own
// dedicated integration tests in lib/jobs; these tests are not about the
// queue itself, only about the thin CLI-facing functions on top of it.
type fakeBackend struct {
	pushedJobSet  *jobs.JobSet
	pushJobSetErr error

	getSetResult *jobs.JobSet
	getSetErr    error

	getSetsResult []*jobs.JobSet
	getSetsErr    error
}

func (f *fakeBackend) IsEnabled() bool { return true }

func (f *fakeBackend) PushJobSet(js *jobs.JobSet) error {
	f.pushedJobSet = js
	return f.pushJobSetErr
}

func (f *fakeBackend) PushJob(job *jobs.Job, untake bool) error { return nil }

func (f *fakeBackend) Take(jobType, executor string) (*jobs.Job, error) { return nil, nil }

func (f *fakeBackend) Done(jobID string) error { return nil }

func (f *fakeBackend) Error(jobID, message string, retry bool) error { return nil }

func (f *fakeBackend) GetSets() ([]*jobs.JobSet, error) { return f.getSetsResult, f.getSetsErr }

func (f *fakeBackend) GetSet(id string) (*jobs.JobSet, error) { return f.getSetResult, f.getSetErr }

func (f *fakeBackend) GetOpen(jobType string) ([]*jobs.Job, error) { return nil, nil }

func (f *fakeBackend) GetTaken() ([]*jobs.Job, error) { return nil, nil }

func (f *fakeBackend) GetFailed() ([]*jobs.Job, error) { return nil, nil }

func (f *fakeBackend) StartJobSet(jobSetID string) error { return nil }

func (f *fakeBackend) SetProgressDetails(jobSetID string, details any) error { return nil }

func (f *fakeBackend) SetOutput(jobSetID, key string, value jobs.OutputValue) error { return nil }

func (f *fakeBackend) InitJobSet(jobSetID string, totalDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}

func (f *fakeBackend) UpdateJobSet(jobSetID string, currentDelta int, updates []jobs.ProgressUpdate) error {
	return nil
}

func (f *fakeBackend) UpdateJob(jobID string, currentDelta int) error { return nil }
