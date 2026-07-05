package jobs

// JobResult is the outcome of a JobProcessor.Process call, analogous to
// JobResult.java (success/onHold/retry/error).
type JobResult struct {
	Error  string
	Retry  bool
	OnHold bool
}

func Success() JobResult { return JobResult{} }
func OnHold() JobResult  { return JobResult{OnHold: true} }
func Retry(err string) JobResult {
	return JobResult{Error: err, Retry: true}
}
func Error(err string) JobResult { return JobResult{Error: err} }

func (r JobResult) IsSuccess() bool {
	return r.Error == "" && !r.OnHold
}

func (r JobResult) IsFailure() bool {
	return r.Error != ""
}

// JobProcessor is the worker-plugin contract, analogous to JobProcessor.java:
// a processor declares which job type it handles and does the actual work.
// Unlike Java's JobProcessor<T,U>, there is no generic details type - a
// processor parses Job.Details/JobSet.Inputs itself, since both are already
// opaque json.RawMessage in this model.
type JobProcessor interface {
	JobType() string
	Priority() int
	Process(job *Job, jobSet *JobSet, backend Backend) JobResult
}
