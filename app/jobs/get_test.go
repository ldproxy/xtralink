package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

func TestGet_ReturnsJob(t *testing.T) {
	want := jobs.NewJob("id-1", "demo", 1000, "label", nil)
	backend := &fakeBackend{getJobResult: want}
	appCtx := &app.AppContext{Jobs: backend}

	got, err := Get(appCtx, "id-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != want {
		t.Error("expected the Job returned by the backend")
	}
}

func TestGet_NotFound(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Get(appCtx, "missing"); err == nil {
		t.Fatal("expected an error for an unknown Job id")
	}
}

func TestGet_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{getJobErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Get(appCtx, "id-1"); err == nil {
		t.Fatal("expected an error to be returned")
	}
}
