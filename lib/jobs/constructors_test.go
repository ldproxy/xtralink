package jobs

import (
	"testing"

	"github.com/ldproxy/xtralink/model"
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
	if !model.Success().IsSuccess() {
		t.Error("Success() should be IsSuccess()")
	}
	if model.Success().IsFailure() {
		t.Error("Success() should not be IsFailure()")
	}

	oh := model.OnHold()
	if oh.IsSuccess() {
		t.Error("OnHold() should not be IsSuccess()")
	}
	if oh.IsFailure() {
		t.Error("OnHold() should not be IsFailure() (no Error set)")
	}
	if !oh.IsOnHold() {
		t.Error("OnHold() should set OnHold=true")
	}

	r := model.Retry("transient")
	if !r.IsFailure() {
		t.Error("Retry() should be IsFailure()")
	}
	if r.Status != model.ResultRETRY {
		t.Error("Retry() should set Retry=true")
	}
	if r.Message() != "transient" {
		t.Errorf("Retry().Error = %q, want transient", r.Message())
	}

	e := model.Error("permanent")
	if !e.IsFailure() {
		t.Error("Error() should be IsFailure()")
	}
	if e.Status == model.ResultRETRY {
		t.Error("Error() should not set Retry")
	}
}
