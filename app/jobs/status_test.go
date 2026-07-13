package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

func TestStatus_MapsJobSetFields(t *testing.T) {
	js := jobs.NewJobSet("id-1", "demo", 1000, "label", "entity", nil)
	backend := &fakeBackend{getSetResult: js}
	appCtx := &app.AppContext{Jobs: backend}

	got, err := Status(appCtx, "id-1")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.ID != js.ID || got.Type != js.Type || got.Status != js.Status() || got.Percent != js.Percent() || got.Message != js.Message() {
		t.Errorf("unexpected StatusView: %+v", got)
	}
}

func TestStatus_NotFound(t *testing.T) {
	backend := &fakeBackend{}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Status(appCtx, "missing"); err == nil {
		t.Fatal("expected an error for an unknown JobSet id")
	}
}

func TestStatus_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{getSetErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := Status(appCtx, "id-1"); err == nil {
		t.Fatal("expected an error to be returned")
	}
}
