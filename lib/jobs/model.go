// Package jobs implements the internal Job/JobSet queue model
// (inputs/outputs/progressDetails), analogous to xtraplatform-jobs in Java.
package jobs

import (
	"encoding/json"
	"time"
)

// Status is the derived lifecycle status of a JobSet.
type Status string

const (
	StatusAccepted   Status = "accepted"
	StatusRunning    Status = "running"
	StatusSuccessful Status = "successful"
	StatusFailed     Status = "failed"
)

// ProgressOp is the operation applied to a progressDetails JSON path when a
// sub-Job reports progress.
type ProgressOp string

const (
	ProgressOpAdd      ProgressOp = "add"
	ProgressOpSubtract ProgressOp = "subtract"
)

// ProgressUpdate declares, relative to JobSet.progressDetails, a JSON path
// and operation that a Job.current delta should also be applied to - it lets
// the queue core update progressDetails without knowing the job type.
type ProgressUpdate struct {
	Path string     `json:"path"`
	Op   ProgressOp `json:"op"`
}

// BaseJob holds the properties shared by Job and JobSet.
type BaseJob struct {
	ID         string   `json:"id"`
	Type       string   `json:"type"`
	Priority   int      `json:"priority"`
	Total      int      `json:"total"`
	Current    int      `json:"current"`
	StartedAt  int64    `json:"startedAt"`
	UpdatedAt  int64    `json:"updatedAt"`
	FinishedAt int64    `json:"finishedAt"`
	Errors     []string `json:"errors"`
}

// NewBaseJob returns a BaseJob in the not-yet-started ("accepted") state.
func NewBaseJob(id, jobType string, priority int) BaseJob {
	return BaseJob{
		ID:         id,
		Type:       jobType,
		Priority:   priority,
		Errors:     []string{},
		StartedAt:  -1,
		UpdatedAt:  nowMillis(),
		FinishedAt: -1,
	}
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}

// Percent is derived from current/total on read, not stored in the model.
func (b *BaseJob) Percent() int {
	if b.Total <= 0 {
		if b.StartedAt <= 0 {
			return 0
		}
		return 100
	}
	if b.Current >= b.Total {
		return 100
	}
	if b.Current <= 0 {
		return 0
	}
	return b.Current * 100 / b.Total
}

func (b *BaseJob) IsStarted() bool {
	return b.StartedAt > 0
}

func (b *BaseJob) IsDone() bool {
	return b.IsStarted() && b.Current == b.Total
}

func (b *BaseJob) HasErrors() bool {
	return len(b.Errors) > 0
}

// Init grows the expected scope of the job (BaseJob.init in Java).
func (b *BaseJob) Init(delta int) {
	b.Total += delta
	b.UpdatedAt = nowMillis()
}

// Update reports progress (BaseJob.update in Java).
func (b *BaseJob) Update(delta int) {
	b.Current += delta
	b.UpdatedAt = nowMillis()
}

// Job is a single unit of work a worker actually executes.
type Job struct {
	BaseJob
	PartOf   string  `json:"partOf,omitempty"`
	Executor *string `json:"executor,omitempty"`
	OnHold   bool    `json:"onHold,omitempty"`

	// Details is opaque and process-specific - sub-job structure is internal
	// sharding and must not leak into the public contract.
	Details json.RawMessage `json:"details,omitempty"`

	// UpdateTargets is the declarative progress-update descriptor attached
	// to a sub-Job when it is created in setup.
	UpdateTargets []ProgressUpdate `json:"updateTargets,omitempty"`
}

func NewJob(id, jobType string, priority int, partOf string) *Job {
	return &Job{
		BaseJob: NewBaseJob(id, jobType, priority),
		PartOf:  partOf,
	}
}

// OutputValue is a JobSet output: either a literal value, a by-reference
// href, or both.
type OutputValue struct {
	Value any    `json:"value,omitempty"`
	Href  string `json:"href,omitempty"`
	Type  string `json:"type,omitempty"`
}

// JobSet is the order a caller pushes; it orchestrates Jobs and carries
// metadata/progress.
type JobSet struct {
	BaseJob
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
	Entity      string `json:"entity,omitempty"`

	// Inputs mirrors the OGC Execute request body 1:1.
	Inputs json.RawMessage `json:"inputs,omitempty"`
	// Outputs is populated by the last lifecycle step (e.g. cleanup).
	Outputs map[string]OutputValue `json:"outputs"`
	// ProgressDetails is the process-specific progress extension (formerly
	// JobSetDetails); it is never part of the external OGC wire format.
	ProgressDetails json.RawMessage `json:"progressDetails,omitempty"`

	Setup     *Job      `json:"setup"`
	Cleanup   *Job      `json:"cleanup"`
	FollowUps []*JobSet `json:"followUps"`
}

// NewJobSet creates a JobSet in the "accepted" state.
func NewJobSet(id, jobType string, priority int, label, entity string, inputs json.RawMessage) *JobSet {
	return &JobSet{
		BaseJob:   NewBaseJob(id, jobType, priority),
		Label:     label,
		Entity:    entity,
		Inputs:    inputs,
		Outputs:   map[string]OutputValue{},
		FollowUps: []*JobSet{},
	}
}

// Status derives the OGC-facing lifecycle status.
func (j *JobSet) Status() Status {
	switch {
	case j.FinishedAt > 0:
		// Checked first, ahead of StartedAt: a permanently failed setup Job
		// (RedisBackend.forceFail) can finish a JobSet that was never
		// formally "started" (no sub-Job was ever taken). Finished always
		// wins, regardless of whether it was ever running.
		if j.HasErrors() {
			return StatusFailed
		}
		return StatusSuccessful
	case j.StartedAt <= 0:
		return StatusAccepted
	default:
		return StatusRunning
	}
}

// Message returns a short human-readable status text. This is a generic
// placeholder per status; phase-specific messages (e.g. naming the tileset
// currently being seeded) would need the JobSet to carry some notion of
// "current phase", which nothing in the model does yet.
func (j *JobSet) Message() string {
	switch j.Status() {
	case StatusAccepted:
		return "Job accepted"
	case StatusRunning:
		return "Job running"
	case StatusSuccessful:
		return "Job completed successfully"
	case StatusFailed:
		if n := len(j.Errors); n > 0 {
			return j.Errors[n-1]
		}
		return "Job failed"
	default:
		return ""
	}
}
