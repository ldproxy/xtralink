package model

import "time"

// Init grows the expected scope of the job (BaseJob.init in Java).
func (b *Job) Init(delta int) {
	b.Progress.Total += delta
	b.BaseJob.UpdatedAt = nowMillis()
}

// Update reports progress (BaseJob.update in Java).
func (b *Job) Update(delta int) {
	b.Progress.Current += delta
	b.BaseJob.UpdatedAt = nowMillis()
}

// Percent is derived from current/total on read, not stored in the model.
func (b *Job) Percent() int {
	if b.Progress.Total <= 0 {
		if b.StartedAt <= 0 {
			return 0
		}
		return 100
	}
	if b.Progress.Current >= b.Progress.Total {
		return 100
	}
	if b.Progress.Current <= 0 {
		return 0
	}
	return b.Progress.Current * 100 / b.Progress.Total
}

func (b *BaseJob) IsStarted() bool {
	return b.StartedAt > 0
}

func (b *Job) IsDone() bool {
	return b.IsStarted() && b.Progress.Current == b.Progress.Total
}

func (b *BaseJob) HasErrors() bool {
	return len(b.Errors) > 0
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
			return StatusFAILED
		}
		return StatusSUCCESSFUL
	case j.StartedAt <= 0:
		return StatusACCEPTED
	default:
		return StatusRUNNING
	}
}

// Message returns a short human-readable status text. This is a generic
// placeholder per status; phase-specific messages (e.g. naming the tileset
// currently being seeded) would need the Job to carry some notion of
// "current phase", which nothing in the model does yet.
func (j *Job) Message() string {
	switch j.Status() {
	case StatusACCEPTED:
		return "Job accepted"
	case StatusRUNNING:
		return "Job running"
	case StatusSUCCESSFUL:
		return "Job completed successfully"
	case StatusFAILED:
		if n := len(j.Errors); n > 0 {
			return j.Errors[n-1]
		}
		return "Job failed"
	default:
		return ""
	}
}

func nowMillis() int64 {
	return time.Now().UnixMilli()
}
