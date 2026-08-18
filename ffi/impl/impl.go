package impl

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	api "github.com/ldproxy/xtralink/ffi/api"
	"github.com/ldproxy/xtralink/lib/jobs"
	model "github.com/ldproxy/xtralink/model"
)

//=== JobQueue ===

type JobQueue struct {
	backend    jobs.Backend
	runner     *jobs.Runner
	stopRunner context.CancelFunc
	started    bool
}

// The  runner is built here, not in Start, so that Register works
// in either order: the natural FFI lifecycle is register-then-start, but Start
// may equally come first (Runner picks up processors registered while it runs).
func NewJobQueue() *JobQueue {
	runner := jobs.NewRunner2()
	runner.OnError = func(err error) {
		fmt.Printf("xtralink - Runner error: %v\n", err)
	}

	return &JobQueue{backend: nil, runner: runner, stopRunner: nil, started: false}
}

func (s *JobQueue) Start(cfg model.QueueConfiguration) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error occurred: %v\n%s", r, debug.Stack())
		}
	}()

	if s.started {
		return nil
	}

	if cfg.Queue == model.QueueREDIS {
		cluster := ""
		if cfg.Cluster != nil {
			cluster = *cfg.Cluster
		}
		s.backend = jobs.NewRedisBackend(cfg.Redis, cluster)
	} else {
		s.backend = jobs.NewMemoryBackend()
	}

	s.runner.Backend = s.backend
	s.runner.Concurrency = cfg.Concurrency
	s.runner.Executor = cfg.Executor

	ctx, cancel := context.WithCancel(context.Background())
	s.stopRunner = cancel
	go func() { s.runner.Run(ctx) }()
	s.started = true

	return nil
}

// Stops the dispatch loop but keeps runner and registrations, so a later Start
// resumes with the same processors.
func (s *JobQueue) Stop() {
	defer func() { recover() }()

	if s.stopRunner == nil {
		return
	}

	s.stopRunner()
	s.stopRunner = nil
	s.started = false
}

func (s *JobQueue) Register(jobType string, priority int32, processor api.JobProcessor) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error occurred: %v\n%s", r, debug.Stack())
		}
	}()

	s.runner.Register(&jobs.JobProcessor{Kind: jobType, Priority: int(priority), Process: func(partialJob *model.PartialJob, job *model.Job, backend jobs.Backend) model.JobResult {
		result, err := processor.Process(*partialJob, *job)
		if err != nil {
			//fmt.Printf("JOBS: Processor for %s returned error: %v\n%s", partialJob.Kind, err, debug.Stack())
			return model.JobResult{Status: model.ResultFAILURE, Messages: append(result.Messages, err.Error())}
		}
		//fmt.Printf("JOBS: Processor for %s returned result: %v\n", partialJob.Kind, result)
		return result
	}})

	return nil
}

func (s *JobQueue) Push(cfg model.JobConfiguration, onProgress api.JobListener) model.Job {
	job := Create(cfg)

	//TODO: pass listener to backend and have it call back on progress updates, not just on job creation

	err := s.backend.PushJobListen(&job, onProgress)
	if err != nil {
		job.Errors = append(job.Errors, err.Error())
		job.StartedAt = time.Now().UnixMilli()
		job.FinishedAt = job.StartedAt
		job.BaseJob.Status = model.StatusFAILED

		return job
	}

	return job
}

func (s *JobQueue) PushPartial(cfg model.PartialJobConfiguration) model.PartialJob {
	job := CreatePartial(cfg)

	return s.pushPartial(&job, false)
}

func (s *JobQueue) RepushPartial(id string) model.PartialJob {
	job, err := s.backend.GetPartialJob(id)

	if err != nil || job == nil {
		return model.PartialJob{}
	}

	return s.pushPartial(job, true)
}

func (s *JobQueue) pushPartial(job *model.PartialJob, untake bool) model.PartialJob {
	err := s.backend.PushPartialJob(job, untake)
	if err != nil {
		job.Errors = append(job.Errors, err.Error())
		job.StartedAt = time.Now().UnixMilli()
		job.FinishedAt = job.StartedAt
		job.BaseJob.Status = model.StatusFAILED

		return *job
	}

	return *job
}

func (s *JobQueue) WaitFor(id string) model.Job {
	job, _ := s.backend.GetJob(id)

	for job != nil && job.FinishedAt <= 0 {
		//fmt.Printf("JOBS: waiting for job %s to finish, current status: %v\n", job2.Id, job2.Status())
		time.Sleep(1000 * time.Millisecond)
		job, _ = s.backend.GetJob(id)
	}

	return *job
}

func (s *JobQueue) WaitForPartial(id string) model.PartialJob {
	job2, _ := s.backend.GetPartialJob(id)

	for job2 != nil && job2.FinishedAt <= 0 {
		//fmt.Printf("JOBS: waiting for partial job %s to finish, current status: %v\n", job2.Id, job2.Status)
		time.Sleep(1000 * time.Millisecond)
		job3, _ := s.backend.GetPartialJob(id)
		if job3 == nil {
			break
		}
		job2 = job3
	}

	return *job2
}

func (s *JobQueue) Init(jobId string, progress model.InitProgress) {
	s.backend.InitJob(jobId, progress.Total, []model.ProgressUpdate{})
	s.backend.SetProgressDetails(jobId, progress.Details)
}

func (s *JobQueue) UpdatePartial(id string, delta int32) {
	s.backend.UpdatePartialJob(id, int(delta))
}

func (s *JobQueue) Outputs(id string, outputs model.SetOutputs) {
	s.backend.SetOutputs(id, outputs.Outputs)
}

func (s *JobQueue) Cancel(id string) bool {
	return false
}

func (s *JobQueue) Get(id string) (model.Job, bool) {
	job, err := s.backend.GetJob(id)
	if err != nil || job == nil {
		return model.Job{}, false
	}
	return *job, true
}

func (s *JobQueue) GetPartial(id string) (model.PartialJob, bool) {
	job, err := s.backend.GetPartialJob(id)
	if err != nil || job == nil {
		return model.PartialJob{}, false
	}
	return *job, true
}

func Create(cfg model.JobConfiguration) model.Job {
	job := *jobs.NewJob(
		uuid.NewString(),
		cfg.Kind,
		cfg.Priority,
		cfg.Label,
		map[string]any{},
	)
	job.Description = cfg.Description
	job.Inputs = cfg.Inputs
	job.Context = cfg.Context
	job.Progress = cfg.Progress

	//TODO: could be booleans, create and push in runner
	if cfg.Setup {
		job.Setup = jobs.NewPartialJob(
			uuid.NewString(),
			fmt.Sprintf("%v:setup", cfg.Kind),
			cfg.Priority,
			job.Id,
		)
	}
	if cfg.Cleanup {
		job.Cleanup = jobs.NewPartialJob(
			uuid.NewString(),
			fmt.Sprintf("%v:cleanup", cfg.Kind),
			cfg.Priority,
			job.Id,
		)
	}

	job.FollowUps = make([]model.Job, len(cfg.FollowUps))
	for i, followUpCfg := range cfg.FollowUps {
		job.FollowUps[i] = Create(followUpCfg)
	}

	return job
}

func CreatePartial(cfg model.PartialJobConfiguration) model.PartialJob {
	job := *jobs.NewPartialJob(
		uuid.NewString(),
		cfg.Kind,
		cfg.Priority,
		cfg.PartOf,
	)
	job.Progress = cfg.Progress
	job.ProgressUpdates = cfg.ProgressUpdates
	job.Context = cfg.Context
	job.Sequence = cfg.Sequence

	return job
}
