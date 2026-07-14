package jobs

import (
	"encoding/json"
	"testing"
)

func TestBaseJobPercent(t *testing.T) {
	tests := []struct {
		name    string
		total   int
		current int
		started int64
		want    int
	}{
		{"not started, zero total", 0, 0, -1, 0},
		{"started, zero total", 0, 0, 1, 100},
		{"zero current", 10, 0, 1, 0},
		{"negative current", 10, -5, 1, 0},
		{"current equals total", 10, 10, 1, 100},
		{"current exceeds total", 10, 15, 1, 100},
		{"partial progress", 10, 4, 1, 40},
		{"partial progress rounds down", 3, 1, 1, 33},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BaseJob{Total: tt.total, Current: tt.current, StartedAt: tt.started}
			if got := b.Percent(); got != tt.want {
				t.Errorf("Percent() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestBaseJobIsStarted(t *testing.T) {
	if (&BaseJob{StartedAt: -1}).IsStarted() {
		t.Error("expected not started")
	}
	if !(&BaseJob{StartedAt: 1}).IsStarted() {
		t.Error("expected started")
	}
}

func TestBaseJobIsDone(t *testing.T) {
	tests := []struct {
		name    string
		started int64
		current int
		total   int
		want    bool
	}{
		{"not started", -1, 5, 5, false},
		{"started, not equal", 1, 3, 5, false},
		{"started, equal", 1, 5, 5, true},
		{"started, both zero", 1, 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := BaseJob{StartedAt: tt.started, Current: tt.current, Total: tt.total}
			if got := b.IsDone(); got != tt.want {
				t.Errorf("IsDone() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBaseJobHasErrors(t *testing.T) {
	if (&BaseJob{Errors: nil}).HasErrors() {
		t.Error("expected no errors for nil slice")
	}
	if (&BaseJob{Errors: []string{}}).HasErrors() {
		t.Error("expected no errors for empty slice")
	}
	if !(&BaseJob{Errors: []string{"boom"}}).HasErrors() {
		t.Error("expected errors")
	}
}

func TestBaseJobInit(t *testing.T) {
	b := BaseJob{Total: 5}
	b.Init(3)
	if b.Total != 8 {
		t.Errorf("Total = %d, want 8", b.Total)
	}
	if b.UpdatedAt == 0 {
		t.Error("expected UpdatedAt to be set")
	}
}

func TestBaseJobUpdate(t *testing.T) {
	b := BaseJob{Current: 2}
	b.Update(3)
	if b.Current != 5 {
		t.Errorf("Current = %d, want 5", b.Current)
	}
	if b.UpdatedAt == 0 {
		t.Error("expected UpdatedAt to be set")
	}
}

func TestNewBaseJob(t *testing.T) {
	b := NewBaseJob("id-1", "test-type", 500)
	if b.ID != "id-1" || b.Type != "test-type" || b.Priority != 500 {
		t.Errorf("unexpected fields: %+v", b)
	}
	if b.StartedAt != -1 || b.FinishedAt != -1 {
		t.Errorf("expected StartedAt/FinishedAt = -1, got %d/%d", b.StartedAt, b.FinishedAt)
	}
	if b.Errors == nil {
		t.Error("expected Errors to be initialized to a non-nil empty slice")
	}
}

func TestNewPartialJob(t *testing.T) {
	j := NewPartialJob("job-1", "worker", 1000, "set-1")
	if j.PartOf != "set-1" {
		t.Errorf("PartOf = %q, want set-1", j.PartOf)
	}
	if j.ID != "job-1" || j.Type != "worker" || j.Priority != 1000 {
		t.Errorf("unexpected fields: %+v", j)
	}
}

func TestNewJob(t *testing.T) {
	inputs := json.RawMessage(`{"foo":"bar"}`)
	job := NewJob("set-1", "demo", 1000, "Label", inputs)
	if job.Label != "Label" {
		t.Errorf("unexpected fields: %+v", job)
	}
	if job.Outputs == nil {
		t.Error("expected Outputs to be initialized")
	}
	if job.FollowUps == nil {
		t.Error("expected FollowUps to be initialized")
	}
	if job.Setup != nil || job.Cleanup != nil {
		t.Error("expected Setup/Cleanup to be nil by default")
	}
}

func TestJobStatus(t *testing.T) {
	tests := []struct {
		name       string
		startedAt  int64
		finishedAt int64
		errors     []string
		want       Status
	}{
		{"accepted", -1, -1, nil, StatusAccepted},
		{"running", 1, -1, nil, StatusRunning},
		{"running with errors mid-flight", 1, -1, []string{"transient"}, StatusRunning},
		{"successful", 1, 2, nil, StatusSuccessful},
		{"failed", 1, 2, []string{"boom"}, StatusFailed},
		// Regression: a permanently failed setup PartialJob can finish a
		// Job that was never started (RedisBackend.forceFail) - finished
		// must win over the "never started -> accepted" rule, or the Job
		// would incorrectly show "accepted" forever.
		{"finished without ever starting, no errors", -1, 5, nil, StatusSuccessful},
		{"finished without ever starting, with errors", -1, 5, []string{"setup failed"}, StatusFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{BaseJob: BaseJob{StartedAt: tt.startedAt, FinishedAt: tt.finishedAt, Errors: tt.errors}}
			if got := job.Status(); got != tt.want {
				t.Errorf("Status() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestJobMessage(t *testing.T) {
	tests := []struct {
		name string
		job  *Job
		want string
	}{
		{"accepted", &Job{BaseJob: BaseJob{StartedAt: -1, FinishedAt: -1}}, "Job accepted"},
		{"running", &Job{BaseJob: BaseJob{StartedAt: 1, FinishedAt: -1}}, "Job running"},
		{"successful", &Job{BaseJob: BaseJob{StartedAt: 1, FinishedAt: 2}}, "Job completed successfully"},
		{
			"failed uses last error",
			&Job{BaseJob: BaseJob{StartedAt: 1, FinishedAt: 2, Errors: []string{"first", "last"}}},
			"last",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.job.Message(); got != tt.want {
				t.Errorf("Message() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestJobResultConstructors(t *testing.T) {
	if !Success().IsSuccess() {
		t.Error("Success() should be IsSuccess()")
	}
	if Success().IsFailure() {
		t.Error("Success() should not be IsFailure()")
	}

	oh := OnHold()
	if oh.IsSuccess() {
		t.Error("OnHold() should not be IsSuccess()")
	}
	if oh.IsFailure() {
		t.Error("OnHold() should not be IsFailure() (no Error set)")
	}
	if !oh.OnHold {
		t.Error("OnHold() should set OnHold=true")
	}

	r := Retry("transient")
	if !r.IsFailure() {
		t.Error("Retry() should be IsFailure()")
	}
	if !r.Retry {
		t.Error("Retry() should set Retry=true")
	}
	if r.Error != "transient" {
		t.Errorf("Retry().Error = %q, want transient", r.Error)
	}

	e := Error("permanent")
	if !e.IsFailure() {
		t.Error("Error() should be IsFailure()")
	}
	if e.Retry {
		t.Error("Error() should not set Retry")
	}
}
