package jobs

import (
	"errors"
	"testing"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/jobs"
)

func TestList_MapsAllJobs(t *testing.T) {
	a := jobs.NewJob("id-a", "demo", 1000, "", nil)
	b := jobs.NewJob("id-b", "demo", 1000, "", nil)
	backend := &fakeBackend{getJobsResult: []*jobs.Job{a, b}}
	appCtx := &app.AppContext{Jobs: backend}

	got, err := List(appCtx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 || got[0].ID != "id-a" || got[1].ID != "id-b" {
		t.Errorf("unexpected views: %+v", got)
	}
}

func TestList_EmptyReturnsEmptyNotNilSlice(t *testing.T) {
	backend := &fakeBackend{getJobsResult: []*jobs.Job{}}
	appCtx := &app.AppContext{Jobs: backend}

	got, err := List(appCtx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got == nil {
		t.Error("expected a non-nil empty slice")
	}
	if len(got) != 0 {
		t.Errorf("expected 0 views, got %d", len(got))
	}
}

func TestList_WrapsBackendError(t *testing.T) {
	backend := &fakeBackend{getJobsErr: errors.New("boom")}
	appCtx := &app.AppContext{Jobs: backend}

	if _, err := List(appCtx); err == nil {
		t.Fatal("expected an error to be returned")
	}
}
