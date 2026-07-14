package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

func TestStatus_MapsJobFields(t *testing.T) {
	job := jobs.NewJob("id-1", "demo", 1000, "label", nil)
	backend := &fakeBackend{getJobResult: job}
	appCtx := &app.AppContext{Jobs: backend}

	got, err := Status(appCtx, "id-1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.ID != job.ID || got.Type != job.Type || got.Status != job.Status() || got.Percent != job.Percent() || got.Message != job.Message() {
		t.Errorf("unexpected StatusView: %+v", got)
	}
}

func TestStatus_NotFound(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Status(appCtx, "missing"); err == nil {
		t.Fatal("expected an error for an unknown Job id")
	}
}

func TestStatus_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{getJobErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Status(appCtx, "id-1"); err == nil {
		t.Fatal("expected an error to be returned")
	}
}
