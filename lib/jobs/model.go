// Package jobs implements the internal Job/JobSet model and queue backend
// described in Job-Queue-Example-Diagram.md, §5 (the generalized model with
// inputs/outputs/progressDetails), analogous to xtraplatform-jobs in Java.
package jobs

import (
	"encoding/json"
	"time"
)

// Status is the derived lifecycle status of a JobSet (Diagram §5.1).
type Status string

const (
	StatusAccepted   Status = "accepted"
	StatusRunning    Status = "running"
	StatusSuccessful Status = "successful"
	StatusFailed     Status = "failed"
)

// ProgressOp is the operation applied to a progressDetails JSON path when a
// sub-Job reports progress (Diagram §4).
type ProgressOp string

const (
	ProgressOpAdd      ProgressOp = "add"
	ProgressOpSubtract ProgressOp = "subtract"
)

// ProgressUpdate declares, relative to JobSet.progressDetails, a JSON path
// and operation that a Job.current delta should also be applied to. This is
// the generic update descriptor from Diagram §4: it lets the queue core
// update progressDetails without knowing the job type.
type ProgressUpdate struct {
	Path string     `json:"path"`
	Op   ProgressOp `json:"op"`
}

// BaseJob holds the properties shared by Job and JobSet (Diagram §1).
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

// Percent returns BaseJob.percent (Diagram §3 read notes): derived from
// current/total, not stored in the model.
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

// Job is a single unit of work a worker actually executes (Diagram §1: "Job").
type Job struct {
	BaseJob
	PartOf   string  `json:"partOf,omitempty"`
	Executor *string `json:"executor,omitempty"`
	OnHold   bool    `json:"onHold,omitempty"`

	// Details is opaque and process-specific (Diagram §4: sub-job structure
	// is internal sharding and must not leak into the public contract).
	Details json.RawMessage `json:"details,omitempty"`

	// UpdateTargets is the declarative progress-update descriptor (Diagram
	// §4) attached to a sub-Job when it is created in setup.
	UpdateTargets []ProgressUpdate `json:"updateTargets,omitempty"`
}

func NewJob(id, jobType string, priority int, partOf string) *Job {
	return &Job{
		BaseJob: NewBaseJob(id, jobType, priority),
		PartOf:  partOf,
	}
}

// OutputValue is a JobSet output (Diagram §4/§5): either a literal value, a
// by-reference href, or both.
type OutputValue struct {
	Value any    `json:"value,omitempty"`
	Href  string `json:"href,omitempty"`
	Type  string `json:"type,omitempty"`
}

// JobSet is the order a caller pushes; it orchestrates Jobs and carries
// metadata/progress (Diagram §1: "jobset").
type JobSet struct {
	BaseJob
	Label       string `json:"label,omitempty"`
	Description string `json:"description,omitempty"`
	Entity      string `json:"entity,omitempty"`

	// Inputs mirrors the OGC Execute request body 1:1 (Diagram §4/§5).
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

// NewJobSet creates a JobSet in the "accepted" state (Diagram §5.2).
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

// Status derives the OGC-facing lifecycle status (Diagram §5.1).
func (j *JobSet) Status() Status {
	switch {
	case j.StartedAt <= 0:
		return StatusAccepted
	case j.FinishedAt <= 0:
		return StatusRunning
	case j.HasErrors():
		return StatusFailed
	default:
		return StatusSuccessful
	}
}

// Message returns a short human-readable status text (Diagram §5.1/§5.3).
// Phase-specific messages (e.g. naming the tileset currently being seeded)
// require the JobRunner/JobProcessor machinery and are not yet implemented;
// this is a generic placeholder per status.
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
