package jobs

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// MemoryBackend implements Backend entirely in memory, guarded by a single
// mutex - the "local" queue from JobsConfig, recommended only for
// single-node setups (state is lost on restart, and separate processes
// never see each other's jobs). It shares no code with RedisBackend (s. its
// doc comment): the two have nothing in common beyond the Backend interface
// itself, so there is no template-method base class between them.
//
// Because every method below serializes on b.mu for its entire body, the
// SETNX-style "claim the right to finalize" dance RedisBackend needs in
// finalizeIfDone (guarding against two different *processes* racing) has no
// equivalent here - two goroutines can never both observe "not yet
// finished" before either writes, since the mutex already makes that
// impossible.
type MemoryBackend struct {
	mu sync.Mutex

	partial map[string]*PartialJob
	jobs    map[string]*Job

	// queues[type][priority] holds partial job IDs waiting to run. Index 0
	// is the oldest fresh push (LPush-equivalent: prepend), the last index
	// is the next one Take() pops (LMove RIGHT-equivalent: remove from the
	// end) - mirrors RedisBackend's per-(type,priority) list semantics
	// exactly, including untake's "goes straight to the front of the line"
	// behavior (RPush-equivalent: append at the end, i.e. next to be
	// popped).
	queues map[string]map[int][]string

	taken  map[string]bool
	failed []string
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		partial: map[string]*PartialJob{},
		jobs:    map[string]*Job{},
		queues:  map[string]map[int][]string{},
		taken:   map[string]bool{},
	}
}

func (b *MemoryBackend) IsEnabled() bool { return true }

// cloneJob/clonePartialJob round-trip through JSON, the same serialize/
// deserialize boundary RedisBackend gets for free from actually being a
// separate process - every value stored in or read out of the maps is a
// full copy, so a caller mutating what Get.../Take returns can never
// corrupt this backend's internal state (or vice versa) without going
// through its methods. Marshal/Unmarshal of a well-formed Job/PartialJob
// value can't fail, so the errors are deliberately swallowed here.
func cloneJob(j *Job) *Job {
	if j == nil {
		return nil
	}
	raw, _ := json.Marshal(j)
	var out Job
	_ = json.Unmarshal(raw, &out)
	return &out
}

func clonePartialJob(p *PartialJob) *PartialJob {
	if p == nil {
		return nil
	}
	raw, _ := json.Marshal(p)
	var out PartialJob
	_ = json.Unmarshal(raw, &out)
	return &out
}

func (b *MemoryBackend) PushJob(job *Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pushJobLocked(job)
}

func (b *MemoryBackend) pushJobLocked(job *Job) error {
	b.jobs[job.ID] = cloneJob(job)
	if job.Setup != nil {
		return b.pushPartialJobLocked(job.Setup, false)
	}
	return nil
}

func (b *MemoryBackend) PushPartialJob(partialJob *PartialJob, untake bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pushPartialJobLocked(partialJob, untake)
}

func (b *MemoryBackend) pushPartialJobLocked(partialJob *PartialJob, untake bool) error {
	stored := clonePartialJob(partialJob)
	b.partial[stored.ID] = stored

	byPriority := b.queues[stored.Type]
	if byPriority == nil {
		byPriority = map[int][]string{}
		b.queues[stored.Type] = byPriority
	}

	if untake {
		delete(b.taken, stored.ID)
		byPriority[stored.Priority] = append(byPriority[stored.Priority], stored.ID)
	} else {
		byPriority[stored.Priority] = append([]string{stored.ID}, byPriority[stored.Priority]...)
	}
	return nil
}

func (b *MemoryBackend) Take(partialJobType, executor string) (*PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	byPriority := b.queues[partialJobType]
	if byPriority == nil {
		return nil, nil
	}

	for _, p := range descendingPriorities(byPriority) {
		ids := byPriority[p]
		if len(ids) == 0 {
			continue
		}
		id := ids[len(ids)-1]
		byPriority[p] = ids[:len(ids)-1]

		partialJob := b.partial[id]
		if partialJob == nil {
			continue
		}

		b.taken[id] = true
		now := nowMillis()
		partialJob.Executor = &executor
		partialJob.StartedAt = now
		partialJob.UpdatedAt = now

		return clonePartialJob(partialJob), nil
	}

	return nil, nil
}

func descendingPriorities(byPriority map[int][]string) []int {
	priorities := make([]int, 0, len(byPriority))
	for p := range byPriority {
		priorities = append(priorities, p)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(priorities)))
	return priorities
}

func (b *MemoryBackend) Done(partialJobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.taken[partialJobID] {
		return nil
	}
	delete(b.taken, partialJobID)

	partialJob := b.partial[partialJobID]
	if partialJob != nil && partialJob.PartOf != "" {
		if err := b.onPartialJobDoneLocked(partialJob); err != nil {
			return err
		}
	}

	delete(b.partial, partialJobID)
	return nil
}

// onPartialJobDoneLocked mirrors RedisBackend.onPartialJobDone.
func (b *MemoryBackend) onPartialJobDoneLocked(partialJob *PartialJob) error {
	job := b.jobs[partialJob.PartOf]
	if job == nil {
		return nil
	}

	if job.Setup != nil && job.Setup.ID == partialJob.ID {
		b.syncEmbeddedPartialJobLocked(job, "setup", partialJob)
		return nil
	}
	if job.Cleanup != nil && job.Cleanup.ID == partialJob.ID {
		b.syncEmbeddedPartialJobLocked(job, "cleanup", partialJob)
		b.clearProgressDetailsOnSuccessLocked(job)
		return b.pushFollowUpsLocked(job)
	}

	job.UpdatedAt = nowMillis()
	return b.finalizeIfDoneLocked(job)
}

func (b *MemoryBackend) syncEmbeddedPartialJobLocked(job *Job, field string, partialJob *PartialJob) {
	done := clonePartialJob(partialJob)
	done.UpdatedAt = nowMillis()
	if done.FinishedAt <= 0 {
		done.FinishedAt = done.UpdatedAt
	}
	switch field {
	case "setup":
		job.Setup = done
	case "cleanup":
		job.Cleanup = done
	}
}

// onPartialJobPermanentlyFailedLocked mirrors
// RedisBackend.onPartialJobPermanentlyFailed.
func (b *MemoryBackend) onPartialJobPermanentlyFailedLocked(partialJob *PartialJob) error {
	job := b.jobs[partialJob.PartOf]
	if job == nil {
		return nil
	}
	if job.Setup != nil && job.Setup.ID == partialJob.ID {
		b.syncEmbeddedPartialJobLocked(job, "setup", partialJob)
		b.forceFailLocked(job, partialJob.Errors)
		return nil
	}
	if job.Cleanup != nil && job.Cleanup.ID == partialJob.ID {
		b.syncEmbeddedPartialJobLocked(job, "cleanup", partialJob)
		job.Errors = append(job.Errors, partialJob.Errors...)
		return nil
	}

	if remaining := partialJob.Total - partialJob.Current; remaining > 0 {
		job.Total -= remaining
	}
	job.Errors = append(job.Errors, partialJob.Errors...)
	job.UpdatedAt = nowMillis()

	return b.finalizeIfDoneLocked(job)
}

// finalizeIfDoneLocked mirrors RedisBackend.finalizeIfDone, minus the SETNX
// claim (s. MemoryBackend's doc comment - the mutex already makes it
// impossible for two calls to both see "not yet finished" here).
func (b *MemoryBackend) finalizeIfDoneLocked(job *Job) error {
	if !job.IsDone() || job.FinishedAt > 0 {
		return nil
	}

	job.FinishedAt = nowMillis()
	if job.Cleanup != nil {
		return b.pushPartialJobLocked(job.Cleanup, false)
	}
	b.clearProgressDetailsOnSuccessLocked(job)
	return b.pushFollowUpsLocked(job)
}

// forceFailLocked mirrors RedisBackend.forceFail.
func (b *MemoryBackend) forceFailLocked(job *Job, errors []string) {
	job.Errors = append(job.Errors, errors...)
	if job.FinishedAt <= 0 {
		job.FinishedAt = nowMillis()
	}
}

// clearProgressDetailsOnSuccessLocked mirrors
// RedisBackend.clearProgressDetailsOnSuccess - the literal JSON "null" (not
// a Go nil/empty json.RawMessage) matches what a JSON.SET ...
// $.progressDetails null round-trip through Redis actually produces.
func (b *MemoryBackend) clearProgressDetailsOnSuccessLocked(job *Job) {
	if job.HasErrors() {
		return
	}
	job.ProgressDetails = json.RawMessage("null")
}

func (b *MemoryBackend) pushFollowUpsLocked(job *Job) error {
	for _, followUp := range job.FollowUps {
		if err := b.pushJobLocked(followUp); err != nil {
			return err
		}
	}
	return nil
}

func (b *MemoryBackend) StartJob(jobID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	job.StartedAt = nowMillis()
	return nil
}

func (b *MemoryBackend) SetProgressDetails(jobID string, details any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return err
	}
	job.ProgressDetails = raw
	return nil
}

func (b *MemoryBackend) SetOutput(jobID, key string, value OutputValue) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	if job.Outputs == nil {
		job.Outputs = map[string]OutputValue{}
	}
	job.Outputs[key] = value
	return nil
}

func (b *MemoryBackend) Error(partialJobID, message string, retry bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.taken[partialJobID] {
		return nil
	}
	delete(b.taken, partialJobID)

	partialJob := b.partial[partialJobID]
	if partialJob == nil {
		return nil
	}

	partialJob.Errors = append(partialJob.Errors, message)
	partialJob.UpdatedAt = nowMillis()

	if retry && len(partialJob.Errors) <= maxRetries {
		return b.pushPartialJobLocked(partialJob, true)
	}

	b.failed = append(b.failed, partialJobID)

	if partialJob.PartOf != "" {
		return b.onPartialJobPermanentlyFailedLocked(partialJob)
	}
	return nil
}

func (b *MemoryBackend) GetJobs() ([]*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*Job, 0, len(b.jobs))
	for _, job := range b.jobs {
		result = append(result, cloneJob(job))
	}
	return result, nil
}

func (b *MemoryBackend) GetJob(id string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneJob(b.jobs[id]), nil
}

func (b *MemoryBackend) GetOpen(partialJobType string) ([]*PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*PartialJob, 0)
	byPriority := b.queues[partialJobType]
	if byPriority == nil {
		return result, nil
	}

	for _, p := range descendingPriorities(byPriority) {
		ids := byPriority[p]
		// ids[len-1] is next up (s. Take) - report in that order.
		for i := len(ids) - 1; i >= 0; i-- {
			if pj := b.partial[ids[i]]; pj != nil {
				result = append(result, clonePartialJob(pj))
			}
		}
	}
	return result, nil
}

func (b *MemoryBackend) GetTaken() ([]*PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*PartialJob, 0, len(b.taken))
	for id := range b.taken {
		if pj := b.partial[id]; pj != nil {
			result = append(result, clonePartialJob(pj))
		}
	}
	return result, nil
}

func (b *MemoryBackend) GetFailed() ([]*PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*PartialJob, 0, len(b.failed))
	for _, id := range b.failed {
		if pj := b.partial[id]; pj != nil {
			result = append(result, clonePartialJob(pj))
		}
	}
	return result, nil
}

func (b *MemoryBackend) InitJob(jobID string, totalDelta int, updates []ProgressUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	job.Total += totalDelta
	job.UpdatedAt = nowMillis()
	return applyProgressUpdatesToJob(job, totalDelta, updates)
}

func (b *MemoryBackend) UpdateJob(jobID string, currentDelta int, updates []ProgressUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	job.Current += currentDelta
	job.UpdatedAt = nowMillis()
	return applyProgressUpdatesToJob(job, currentDelta, updates)
}

func (b *MemoryBackend) UpdatePartialJob(partialJobID string, currentDelta int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	partialJob := b.partial[partialJobID]
	if partialJob == nil {
		return fmt.Errorf("partial job not found: %s", partialJobID)
	}

	partialJob.Current += currentDelta
	partialJob.UpdatedAt = nowMillis()

	if partialJob.PartOf == "" || len(partialJob.UpdateTargets) == 0 {
		return nil
	}

	job := b.jobs[partialJob.PartOf]
	if job == nil {
		return nil
	}
	job.Current += currentDelta
	job.UpdatedAt = nowMillis()
	return applyProgressUpdatesToJob(job, currentDelta, partialJob.UpdateTargets)
}

// applyProgressUpdatesToJob is the in-memory equivalent of RedisBackend's
// applyProgressUpdates: it applies delta to each declared progressDetails
// path via incrJSONPath, the same generic, job-type-agnostic mechanism
// (paths must already exist, initialized by a type-specific setup step via
// SetProgressDetails).
func applyProgressUpdatesToJob(job *Job, delta int, updates []ProgressUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	if len(job.ProgressDetails) == 0 {
		return fmt.Errorf("progressDetails is empty, cannot apply update targets")
	}

	var root map[string]any
	if err := json.Unmarshal(job.ProgressDetails, &root); err != nil {
		return fmt.Errorf("invalid progressDetails: %w", err)
	}

	for _, u := range updates {
		d := delta
		if u.Op == ProgressOpSubtract {
			d = -d
		}
		if err := incrJSONPath(root, u.Path, d); err != nil {
			return err
		}
	}

	raw, err := json.Marshal(root)
	if err != nil {
		return err
	}
	job.ProgressDetails = raw
	return nil
}

// jsonPathSegment is one dot-separated part of a RedisJSON-style dot path
// (e.g. "levels" or "demo[5]" in "tileSets.vineyards.progress.levels.demo[5]"),
// split into its map key and optional trailing array index.
type jsonPathSegment struct {
	key   string
	index int // -1 if this segment has no "[N]" suffix
}

func parseJSONPath(path string) ([]jsonPathSegment, error) {
	parts := strings.Split(path, ".")
	segments := make([]jsonPathSegment, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("invalid progressDetails path %q: empty segment", path)
		}
		i := strings.IndexByte(part, '[')
		if i < 0 {
			segments = append(segments, jsonPathSegment{key: part, index: -1})
			continue
		}
		if !strings.HasSuffix(part, "]") {
			return nil, fmt.Errorf("invalid progressDetails path %q: malformed index in %q", path, part)
		}
		idx, err := strconv.Atoi(part[i+1 : len(part)-1])
		if err != nil {
			return nil, fmt.Errorf("invalid progressDetails path %q: bad index in %q", path, part)
		}
		segments = append(segments, jsonPathSegment{key: part[:i], index: idx})
	}
	return segments, nil
}

// incrJSONPath walks root (an unmarshaled JSON object) along path - the
// same dot-path convention RedisJSON's JSON.NUMINCRBY takes, e.g.
// "tileSets.vineyards.progress.levels.demo[5]" - and adds delta to the
// numeric leaf it finds there, in place.
func incrJSONPath(root map[string]any, path string, delta int) error {
	segments, err := parseJSONPath(path)
	if err != nil {
		return err
	}

	var current any = root
	for i, seg := range segments {
		m, ok := current.(map[string]any)
		if !ok {
			return fmt.Errorf("progressDetails path %q: expected an object at segment %d", path, i)
		}
		val, ok := m[seg.key]
		if !ok {
			return fmt.Errorf("progressDetails path %q: missing key %q", path, seg.key)
		}
		last := i == len(segments)-1

		if seg.index < 0 {
			if last {
				n, ok := val.(float64)
				if !ok {
					return fmt.Errorf("progressDetails path %q: value at %q is not a number", path, seg.key)
				}
				m[seg.key] = n + float64(delta)
				return nil
			}
			current = val
			continue
		}

		arr, ok := val.([]any)
		if !ok {
			return fmt.Errorf("progressDetails path %q: value at %q is not an array", path, seg.key)
		}
		if seg.index >= len(arr) {
			return fmt.Errorf("progressDetails path %q: index %d out of range (len %d)", path, seg.index, len(arr))
		}
		if last {
			n, ok := arr[seg.index].(float64)
			if !ok {
				return fmt.Errorf("progressDetails path %q: value at index %d is not a number", path, seg.index)
			}
			arr[seg.index] = n + float64(delta)
			return nil
		}
		current = arr[seg.index]
	}
	return nil
}
