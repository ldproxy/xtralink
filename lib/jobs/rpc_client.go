package jobs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/ldproxy/xtralink/model"
)

// RemoteBackend is a Backend that calls an RPCHandler at URL, so a Runner
// can execute PartialJobs of a queue owned by another process. Calls the
// handler does not expose (s. rpcMethods) fail with "method not found".
type RemoteBackend struct {
	URL    string
	Client *http.Client

	nextID atomic.Int64
}

func NewRemoteBackend(url string) *RemoteBackend {
	return &RemoteBackend{URL: url, Client: &http.Client{Timeout: 30 * time.Second}}
}

func (b *RemoteBackend) IsEnabled() bool { return true }

// Listen subscribes to nothing: events do not travel over RPC, so the
// Runner falls back to polling, and cancellations reach running processors
// only through their own checks.
func (b *RemoteBackend) Listen(ctx context.Context, n Notifications) error { return nil }

func (b *RemoteBackend) PushJob(job *model.Job) error {
	return b.call("PushJob", nil, job)
}

func (b *RemoteBackend) PushJobListen(job *model.Job, onProgress JobListener) error {
	return errors.New("jobs rpc: PushJobListen is not available over RPC")
}

func (b *RemoteBackend) PushPartialJob(partialJob *model.PartialJob, untake bool) error {
	return b.call("PushPartialJob", nil, partialJob, untake)
}

func (b *RemoteBackend) Take(partialJobType, executor string) (*model.PartialJob, error) {
	var partialJob *model.PartialJob
	return partialJob, b.call("Take", &partialJob, partialJobType, executor)
}

func (b *RemoteBackend) Done(partialJobID string) error {
	return b.call("Done", nil, partialJobID)
}

func (b *RemoteBackend) Error(partialJobID, message string, retry bool) error {
	return b.call("Error", nil, partialJobID, message, retry)
}

func (b *RemoteBackend) DoneJob(jobID string) error {
	return b.call("DoneJob", nil, jobID)
}

func (b *RemoteBackend) Cancel(jobID string) (bool, error) {
	var cancelled bool
	return cancelled, b.call("Cancel", &cancelled, jobID)
}

func (b *RemoteBackend) GetJobs() ([]*model.Job, error) {
	var jobs []*model.Job
	return jobs, b.call("GetJobs", &jobs)
}

func (b *RemoteBackend) GetJob(id string) (*model.Job, error) {
	var job *model.Job
	return job, b.call("GetJob", &job, id)
}

func (b *RemoteBackend) GetPartialJob(id string) (*model.PartialJob, error) {
	var partialJob *model.PartialJob
	return partialJob, b.call("GetPartialJob", &partialJob, id)
}

func (b *RemoteBackend) GetOpen(partialJobType string) ([]*model.PartialJob, error) {
	var partialJobs []*model.PartialJob
	return partialJobs, b.call("GetOpen", &partialJobs, partialJobType)
}

func (b *RemoteBackend) GetTaken() ([]*model.PartialJob, error) {
	var partialJobs []*model.PartialJob
	return partialJobs, b.call("GetTaken", &partialJobs)
}

func (b *RemoteBackend) GetFailed() ([]*model.PartialJob, error) {
	var partialJobs []*model.PartialJob
	return partialJobs, b.call("GetFailed", &partialJobs)
}

func (b *RemoteBackend) Heartbeat(partialJobIDs []string) error {
	return b.call("Heartbeat", nil, partialJobIDs)
}

func (b *RemoteBackend) StartJob(jobID string) error {
	return b.call("StartJob", nil, jobID)
}

func (b *RemoteBackend) SetProgressDetails(jobID string, details map[string]any) error {
	return b.call("SetProgressDetails", nil, jobID, details)
}

func (b *RemoteBackend) SetOutput(jobID, key string, value model.OutputValue) error {
	return b.call("SetOutput", nil, jobID, key, value)
}

func (b *RemoteBackend) SetOutputs(jobID string, outputs map[string]any) error {
	return b.call("SetOutputs", nil, jobID, outputs)
}

func (b *RemoteBackend) InitJob(jobID string, totalDelta int, updates []model.ProgressUpdate) error {
	return b.call("InitJob", nil, jobID, totalDelta, updates)
}

func (b *RemoteBackend) UpdateJob(jobID string, currentDelta int, updates []model.ProgressUpdate) error {
	return b.call("UpdateJob", nil, jobID, currentDelta, updates)
}

func (b *RemoteBackend) UpdatePartialJob(partialJobID string, currentDelta int) error {
	return b.call("UpdatePartialJob", nil, partialJobID, currentDelta)
}

// call sends method with params and decodes the result into result, unless
// result is nil.
func (b *RemoteBackend) call(method string, result any, params ...any) error {
	encodedParams := make([]json.RawMessage, len(params))
	for i, param := range params {
		encoded, err := json.Marshal(param)
		if err != nil {
			return fmt.Errorf("jobs rpc %s: encoding param %d: %w", method, i, err)
		}
		encodedParams[i] = encoded
	}

	id, _ := json.Marshal(b.nextID.Add(1))
	request, err := json.Marshal(rpcRequest{JSONRPC: rpcVersion, Method: method, Params: encodedParams, ID: id})
	if err != nil {
		return fmt.Errorf("jobs rpc %s: %w", method, err)
	}

	httpResponse, err := b.Client.Post(b.URL, "application/json", bytes.NewReader(request))
	if err != nil {
		return fmt.Errorf("jobs rpc %s: %w", method, err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		return fmt.Errorf("jobs rpc %s: %s returned %s", method, b.URL, httpResponse.Status)
	}

	var response rpcResponse
	if err := json.NewDecoder(httpResponse.Body).Decode(&response); err != nil {
		return fmt.Errorf("jobs rpc %s: decoding response: %w", method, err)
	}
	if response.Error != nil {
		return response.Error
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(response.Result, result); err != nil {
		return fmt.Errorf("jobs rpc %s: decoding result: %w", method, err)
	}
	return nil
}
