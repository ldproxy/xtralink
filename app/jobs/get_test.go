package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

func TestGet_ReturnsJobSet(t *testing.T) {
	want := jobs.NewJobSet("id-1", "demo", 1000, "label", "entity", nil)
	backend := &fakeBackend{getSetResult: want}
	appCtx := &app.AppContext{Jobs: backend}

	got, err := Get(appCtx, "id-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != want {
		t.Error("expected the JobSet returned by the backend")
	}
}

func TestGet_NotFound(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Get(appCtx, "missing"); err == nil {
		t.Fatal("expected an error for an unknown JobSet id")
	}
}

func TestGet_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{getSetErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Get(appCtx, "id-1"); err == nil {
		t.Fatal("expected an error to be returned")
	}
}
