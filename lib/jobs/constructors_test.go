package jobs

import (
	"testing"
)

func TestNewBaseJob(t *testing.T) {
	b := NewBaseJob("id-1", "test-type", 500)
	if b.Id != "id-1" || b.Kind != "test-type" || b.Priority != 500 {
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
	if j.Id != "job-1" || j.Kind != "worker" || j.Priority != 1000 {
		t.Errorf("unexpected fields: %+v", j)
	}
}

func TestNewJob(t *testing.T) {
	inputs := map[string]any{"foo": "bar"}
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
