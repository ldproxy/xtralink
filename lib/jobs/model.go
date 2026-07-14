// Package jobs implements the internal Job/PartialJob queue model
// (inputs/outputs/progressDetails), analogous to xtraplatform-jobs in Java.
package jobs

import (
	"encoding/json"
	"time"
)

// Status is the derived lifecycle status of a Job.
type Status string

const (
	StatusAccepted   Status = "accepted"
	StatusRunning    Status = "running"
	StatusSuccessful Status = "successful"
	StatusFailed     Status = "failed"
)

// ProgressOp is the operation applied to a progressDetails JSON path when a
// PartialJob reports progress.
type ProgressOp string

const (
	ProgressOpAdd      ProgressOp = "add"
	ProgressOpSubtract ProgressOp = "subtract"
)

// ProgressUpdate declares, relative to Job.progressDetails, a JSON path
// and operation that a PartialJob.current delta should also be applied to -
// it lets the queue core update progressDetails without knowing the job type.
type ProgressUpdate struct {
	Path string     `json:"path"`
	Op   ProgressOp `json:"op"`
}

// BaseJob holds the properties shared by PartialJob and Job.
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

// PartialJob is a single unit of work a worker actually executes (formerly
// named Job - renamed to stop it colliding with Job, formerly JobSet).
type PartialJob struct {
	BaseJob
	PartOf   string  `json:"partOf,omitempty"`
	Executor *string `json:"executor,omitempty"`
	OnHold   bool    `json:"onHold,omitempty"`

	// Details is opaque and process-specific - sub-job structure is internal
	// sharding and must not leak into the public contract.
	Details json.RawMessage `json:"details,omitempty"`

	// UpdateTargets is the declarative progress-update descriptor attached
	// to a PartialJob when it is created in setup.
	UpdateTargets []ProgressUpdate `json:"updateTargets,omitempty"`

	// Sequence orders this PartialJob relative to its siblings when its
	// parent Job has Parallel=false - it only becomes eligible to run once
	// Job.CurrentSequence reaches this value. Ignored when Parallel is
	// true (the default). Typically the index of this PartialJob within
	// whatever ordered list created it, not chosen by hand.
	Sequence int `json:"sequence,omitempty"`
}

func NewPartialJob(id, jobType string, priority int, partOf string) *PartialJob {
	return &PartialJob{
		BaseJob: NewBaseJob(id, jobType, priority),
		PartOf:  partOf,
	}
}

// OutputValue is a Job output: either a literal value, a by-reference href,
// or both.
type OutputValue struct {
	Value any    `json:"value,omitempty"`
	Href  string `json:"href,omitempty"`
	Type  string `json:"type,omitempty"`
}

// Job is the order a caller pushes; it orchestrates PartialJobs and carries
// metadata/progress (formerly named JobSet - renamed).
type Job struct {
	BaseJob
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`

	// Inputs mirrors the OGC Execute request body 1:1.
	Inputs json.RawMessage `json:"inputs,omitempty"`
	// Outputs is populated by the last lifecycle step (e.g. cleanup).
	Outputs map[string]OutputValue `json:"outputs"`
	// ProgressDetails is the process-specific progress extension (formerly
	// JobSetDetails); it is never part of the external OGC wire format.
	ProgressDetails json.RawMessage `json:"progressDetails,omitempty"`

	Setup     *PartialJob `json:"setup"`
	Cleanup   *PartialJob `json:"cleanup"`
	FollowUps []*Job      `json:"followUps"`

	// Parallel controls whether this Job's (non-setup/cleanup) PartialJobs
	// may run in any order (true, the default - plain sharding) or must
	// run one Sequence at a time (false).
	Parallel bool `json:"parallel"`
	// CurrentSequence is the PartialJob.Sequence value currently allowed
	// to run, when Parallel is false. Ignored when Parallel is true.
	CurrentSequence int `json:"currentSequence,omitempty"`
	// SequenceRemaining counts, per Sequence value, how many PartialJobs
	// pushed at that Sequence have not yet finished (successfully or
	// permanently failed) - only meaningful when Parallel is false.
	// Internal bookkeeping, analogous to BaseJob.Total/Current but
	// partitioned per Sequence instead of aggregated; never part of the
	// external OGC wire format. Deliberately not omitempty: the backends
	// need this object to already exist in the stored JSON document (even
	// empty) so a later per-sequence key can be added to it.
	SequenceRemaining map[int]int `json:"sequenceRemaining"`
}

// NewJob creates a Job in the "accepted" state.
func NewJob(id, jobType string, priority int, label string, inputs json.RawMessage) *Job {
	return &Job{
		BaseJob:           NewBaseJob(id, jobType, priority),
		Label:             label,
		Inputs:            inputs,
		Outputs:           map[string]OutputValue{},
		FollowUps:         []*Job{},
		Parallel:          true,
		SequenceRemaining: map[int]int{},
	}
}

// Status derives the OGC-facing lifecycle status.
func (j *Job) Status() Status {
	switch {
	case j.FinishedAt > 0:
		// Checked first, ahead of StartedAt: a permanently failed setup
		// PartialJob (RedisBackend.forceFail) can finish a Job that was
		// never formally "started" (no PartialJob was ever taken). Finished
		// always wins, regardless of whether it was ever running.
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
// currently being seeded) would need the Job to carry some notion of
// "current phase", which nothing in the model does yet.
func (j *Job) Message() string {
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
