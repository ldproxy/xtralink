package jobs

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ldproxy/xtralink/model"
)

func remoteOf(t *testing.T, owner Backend) *RemoteBackend {
	t.Helper()
	server := httptest.NewServer(NewRPCHandler(owner))
	t.Cleanup(server.Close)
	return NewRemoteBackend(server.URL)
}

// A Runner on a RemoteBackend works the queue of the process that owns it:
// setup, partials and cleanup run remotely, and the owner sees the Job
// finish with the progress and outputs the remote processors reported.
func TestRPC_RemoteRunnerCompletesJob(t *testing.T) {
	owner := NewMemoryBackend()
	remote := remoteOf(t, owner)

	kind, worker := uniqueType("job"), uniqueType("worker")
	job := NewJobFromConfiguration(model.JobConfiguration{
		Kind: kind, Priority: 1, Partials: []string{worker}, Setup: true, Cleanup: true,
	})

	r := fastRunner(remote)
	r.Register(&JobProcessor{Kind: SetupKind(kind), Priority: 1, Process: succeed})
	r.Register(&JobProcessor{Kind: worker, Priority: 1, Process: complete})
	r.Register(&JobProcessor{Kind: CleanupKind(kind), Priority: 1, Process: func(_ *model.PartialJob, job *model.Job, backend Backend) model.JobResult {
		if err := backend.SetOutput(job.Id, "result", model.OutputValue{Value: "done"}); err != nil {
			return model.Error(err.Error())
		}
		return model.Success()
	}})

	if err := owner.PushJob(&job); err != nil {
		t.Fatalf("PushJob: %v", err)
	}
	runUntil(t, r, 3*time.Second, "the job to settle", func() bool {
		got, _ := owner.GetJob(job.Id)
		return got != nil && IsSettled(got)
	})

	got, _ := owner.GetJob(job.Id)
	if got.Status != model.StatusSUCCESSFUL {
		t.Fatalf("Status = %s, want SUCCESSFUL (errors: %v)", got.Status, got.Errors)
	}
	if got.Progress.Current != got.Progress.Total {
		t.Errorf("Progress = %d/%d, want complete", got.Progress.Current, got.Progress.Total)
	}
	if output, ok := got.Outputs["result"]; !ok {
		t.Errorf("Outputs = %v, want result", got.Outputs)
	} else if value, _ := output.(map[string]any); value["value"] != "done" {
		t.Errorf("Outputs[result] = %v, want value done", output)
	}
}

func TestRPC_TakeOnEmptyQueueReturnsNil(t *testing.T) {
	remote := remoteOf(t, NewMemoryBackend())

	partialJob, err := remote.Take(uniqueType("none"), "executor")
	if err != nil || partialJob != nil {
		t.Fatalf("Take = %v, %v; want nil, nil", partialJob, err)
	}
}

// Filling or dismissing the queue stays with its owner.
func TestRPC_ProducerMethodsAreNotExposed(t *testing.T) {
	owner := NewMemoryBackend()
	remote := remoteOf(t, owner)

	job := NewJob("job-1", uniqueType("job"), 1, "", nil)
	if err := remote.PushJob(job); !isRPCError(err, rpcMethodNotFound) {
		t.Errorf("PushJob error = %v, want method not found", err)
	}
	if _, err := remote.Cancel(job.Id); !isRPCError(err, rpcMethodNotFound) {
		t.Errorf("Cancel error = %v, want method not found", err)
	}
	if got, _ := owner.GetJob(job.Id); got != nil {
		t.Errorf("job %s was pushed despite the rejection", job.Id)
	}
}

type failingDone struct{ *MemoryBackend }

func (failingDone) Done(string) error { return errors.New("store unavailable") }

func TestRPC_BackendErrorReachesCaller(t *testing.T) {
	remote := remoteOf(t, failingDone{NewMemoryBackend()})

	err := remote.Done("partial-1")
	if !isRPCError(err, rpcBackendError) || !strings.Contains(err.Error(), "store unavailable") {
		t.Errorf("Done error = %v, want the backend's error", err)
	}
}

func TestRPCHandler_RejectsMalformedRequests(t *testing.T) {
	handler := NewRPCHandler(NewMemoryBackend())

	cases := map[string]struct {
		request string
		code    int
	}{
		"not json":       {`{`, rpcParseError},
		"no version":     {`{"method":"GetJobs","id":1}`, rpcInvalidRequest},
		"wrong arity":    {`{"jsonrpc":"2.0","method":"Done","params":[],"id":1}`, rpcInvalidParams},
		"wrong type":     {`{"jsonrpc":"2.0","method":"Done","params":[1],"id":1}`, rpcInvalidParams},
		"unknown method": {`{"jsonrpc":"2.0","method":"Listen","id":1}`, rpcMethodNotFound},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			response := handler.handle([]byte(c.request))
			if response.Error == nil || response.Error.Code != c.code {
				t.Errorf("error = %v, want code %d", response.Error, c.code)
			}
			if response.Result != nil {
				t.Errorf("result = %s, want none alongside an error", response.Result)
			}
		})
	}
}

func isRPCError(err error, code int) bool {
	var rpcErr *RPCError
	return errors.As(err, &rpcErr) && rpcErr.Code == code
}
