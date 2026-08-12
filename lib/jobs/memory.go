package jobs

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ldproxy/xtralink/model"
)

// MemoryBackend implements Backend entirely in memory, guarded by a single
// mutex - the "local" queue from app.JobQueueConfig, recommended only for
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

	partial map[string]*model.PartialJob
	jobs    map[string]*model.Job

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
		partial: map[string]*model.PartialJob{},
		jobs:    map[string]*model.Job{},
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
func cloneJob(j *model.Job) *model.Job {
	if j == nil {
		return nil
	}
	raw, _ := json.Marshal(j)
	var out model.Job
	_ = json.Unmarshal(raw, &out)
	return &out
}

func clonePartialJob(p *model.PartialJob) *model.PartialJob {
	if p == nil {
		return nil
	}
	raw, _ := json.Marshal(p)
	var out model.PartialJob
	_ = json.Unmarshal(raw, &out)
	return &out
}

func (b *MemoryBackend) PushJob(job *model.Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pushJobLocked(job)
}

func (b *MemoryBackend) pushJobLocked(job *model.Job) error {
	b.jobs[job.Id] = cloneJob(job)
	if job.Setup != nil {
		return b.pushPartialJobLocked(job.Setup, false)
	}
	return nil
}

func (b *MemoryBackend) PushPartialJob(partialJob *model.PartialJob, untake bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pushPartialJobLocked(partialJob, untake)
}

func (b *MemoryBackend) pushPartialJobLocked(partialJob *model.PartialJob, untake bool) error {
	stored := clonePartialJob(partialJob)
	b.partial[stored.Id] = stored

	// Only a fresh push registers a Sequence slot - untake is a re-queue of
	// a PartialJob already counted once at its original push, incrementing
	// again would double-count it.
	if !untake && stored.PartOf != "" {
		if job := b.jobs[stored.PartOf]; job != nil && job.Sequence != nil {
			next := job.Sequence.Remaining
			stored.Sequence = &next
			job.Sequence.Remaining++
		}
	}

	byPriority := b.queues[stored.Kind]
	if byPriority == nil {
		byPriority = map[int][]string{}
		b.queues[stored.Kind] = byPriority
	}

	if untake {
		delete(b.taken, stored.Id)
		byPriority[stored.Priority] = append(byPriority[stored.Priority], stored.Id)
	} else {
		byPriority[stored.Priority] = append([]string{stored.Id}, byPriority[stored.Priority]...)
	}
	return nil
}

// Take scans partialJobType's queue, highest priority first, for the first
// PartialJob whose sequence gate is currently open (s.
// sequenceReadyLocked) - almost always just the very next one, unless its
// parent Job has Parallel=false and it isn't its Sequence's turn yet, in
// which case it is skipped in favor of a later (but eligible) one, leaving
// the rest of the queue's relative order untouched.
func (b *MemoryBackend) Take(partialJobType, executor string) (*model.PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	byPriority := b.queues[partialJobType]
	if byPriority == nil {
		return nil, nil
	}

	for _, p := range descendingPriorities(byPriority) {
		ids := byPriority[p]
		for i := len(ids) - 1; i >= 0; i-- {
			id := ids[i]
			partialJob := b.partial[id]
			if partialJob == nil || !b.sequenceReadyLocked(partialJob) {
				continue
			}

			remaining := make([]string, 0, len(ids)-1)
			remaining = append(remaining, ids[:i]...)
			remaining = append(remaining, ids[i+1:]...)
			byPriority[p] = remaining

			b.taken[id] = true
			now := nowMillis()
			partialJob.Executor = executor
			partialJob.StartedAt = now
			partialJob.UpdatedAt = now

			return clonePartialJob(partialJob), nil
		}
	}

	return nil, nil
}

// sequenceReadyLocked reports whether partialJob may run right now: always
// true for a standalone PartialJob (no parent Job) or one whose parent Job
// has Parallel=true (the default, plain sharding - no ordering
// constraint); otherwise only once its parent's CurrentSequence has
// reached its own Sequence.
func (b *MemoryBackend) sequenceReadyLocked(partialJob *model.PartialJob) bool {
	if partialJob.PartOf == "" {
		return true
	}
	job := b.jobs[partialJob.PartOf]
	if job == nil || job.Sequence == nil {
		return true
	}
	return partialJob.Sequence != nil && *partialJob.Sequence == job.Sequence.Current
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
func (b *MemoryBackend) onPartialJobDoneLocked(partialJob *model.PartialJob) error {
	job := b.jobs[partialJob.PartOf]
	if job == nil {
		return nil
	}

	if job.Setup != nil && job.Setup.Id == partialJob.Id {
		b.syncEmbeddedPartialJobLocked(job, "setup", partialJob)
		return nil
	}
	if job.Cleanup != nil && job.Cleanup.Id == partialJob.Id {
		b.syncEmbeddedPartialJobLocked(job, "cleanup", partialJob)
		return b.pushFollowUpsLocked(job)
	}

	job.UpdatedAt = nowMillis()
	if job.Sequence != nil {
		b.advanceSequenceLocked(job)
	}
	return b.finalizeIfDoneLocked(job)
}

func (b *MemoryBackend) syncEmbeddedPartialJobLocked(job *model.Job, field string, partialJob *model.PartialJob) {
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
func (b *MemoryBackend) onPartialJobPermanentlyFailedLocked(partialJob *model.PartialJob) error {
	job := b.jobs[partialJob.PartOf]
	if job == nil {
		return nil
	}
	if job.Setup != nil && job.Setup.Id == partialJob.Id {
		b.syncEmbeddedPartialJobLocked(job, "setup", partialJob)
		b.forceFailLocked(job, partialJob.Errors)
		return nil
	}
	if job.Cleanup != nil && job.Cleanup.Id == partialJob.Id {
		b.syncEmbeddedPartialJobLocked(job, "cleanup", partialJob)
		job.Errors = append(job.Errors, partialJob.Errors...)
		return nil
	}

	job.Progress.Current += (partialJob.Progress.Total - partialJob.Progress.Current)
	job.Errors = append(job.Errors, partialJob.Errors...)
	job.UpdatedAt = nowMillis()
	if job.Sequence != nil {
		b.advanceSequenceLocked(job)
	}

	return b.finalizeIfDoneLocked(job)
}

// advanceSequenceLocked mirrors RedisBackend.advanceSequence: decrements
// Job.SequenceRemaining[sequence] (the PartialJob that just finished or
// permanently failed) and, once a Sequence's count reaches zero, advances
// Job.CurrentSequence to the next one already known (registered by an
// earlier PushPartialJob covering all Sequences up front) - repeating in
// case that next Sequence turns out to already be empty too.
func (b *MemoryBackend) advanceSequenceLocked(job *model.Job) {
	job.Sequence.Remaining--

	if job.Sequence.Remaining <= 0 {
		job.Sequence.Current = -1
	} else {
		job.Sequence.Current++
	}
}

// finalizeIfDoneLocked mirrors RedisBackend.finalizeIfDone, minus the SETNX
// claim (s. MemoryBackend's doc comment - the mutex already makes it
// impossible for two calls to both see "not yet finished" here).
func (b *MemoryBackend) finalizeIfDoneLocked(job *model.Job) error {
	if !job.IsDone() || job.FinishedAt > 0 {
		return nil
	}

	job.FinishedAt = nowMillis()
	if job.Cleanup != nil {
		return b.pushPartialJobLocked(job.Cleanup, false)
	}
	return b.pushFollowUpsLocked(job)
}

// forceFailLocked mirrors RedisBackend.forceFail.
func (b *MemoryBackend) forceFailLocked(job *model.Job, errors []string) {
	job.Errors = append(job.Errors, errors...)
	if job.FinishedAt <= 0 {
		job.FinishedAt = nowMillis()
	}
}

func (b *MemoryBackend) pushFollowUpsLocked(job *model.Job) error {
	for _, followUp := range job.FollowUps {
		if err := b.pushJobLocked(&followUp); err != nil {
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

func (b *MemoryBackend) SetProgressDetails(jobID string, details map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	job.Progress.Details = details

	return nil
}

func (b *MemoryBackend) SetOutput(jobID, key string, value model.OutputValue) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	if job.Outputs == nil {
		job.Outputs = map[string]any{}
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

func (b *MemoryBackend) GetJobs() ([]*model.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*model.Job, 0, len(b.jobs))
	for _, job := range b.jobs {
		result = append(result, cloneJob(job))
	}
	return result, nil
}

func (b *MemoryBackend) GetJob(id string) (*model.Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return cloneJob(b.jobs[id]), nil
}

func (b *MemoryBackend) GetOpen(partialJobType string) ([]*model.PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*model.PartialJob, 0)
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

func (b *MemoryBackend) GetTaken() ([]*model.PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*model.PartialJob, 0, len(b.taken))
	for id := range b.taken {
		if pj := b.partial[id]; pj != nil {
			result = append(result, clonePartialJob(pj))
		}
	}
	return result, nil
}

func (b *MemoryBackend) GetFailed() ([]*model.PartialJob, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	result := make([]*model.PartialJob, 0, len(b.failed))
	for _, id := range b.failed {
		if pj := b.partial[id]; pj != nil {
			result = append(result, clonePartialJob(pj))
		}
	}
	return result, nil
}

func (b *MemoryBackend) InitJob(jobID string, totalDelta int, updates []model.ProgressUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	job.Progress.Total += totalDelta
	job.UpdatedAt = nowMillis()
	return applyProgressUpdatesToJob(job, totalDelta, updates)
}

func (b *MemoryBackend) UpdateJob(jobID string, currentDelta int, updates []model.ProgressUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	job := b.jobs[jobID]
	if job == nil {
		return nil
	}
	job.Progress.Current += currentDelta
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

	partialJob.Progress.Current += currentDelta
	partialJob.UpdatedAt = nowMillis()

	if partialJob.PartOf == "" {
		return nil
	}

	job := b.jobs[partialJob.PartOf]
	if job == nil {
		return nil
	}
	job.Progress.Current += currentDelta
	job.UpdatedAt = nowMillis()

	if len(partialJob.ProgressUpdates) == 0 {
		return nil
	}

	return applyProgressUpdatesToJob(job, currentDelta, partialJob.ProgressUpdates)
}

// applyProgressUpdatesToJob is the in-memory equivalent of RedisBackend's
// applyProgressUpdates: it applies delta to each declared progressDetails
// path via incrJSONPath, the same generic, job-type-agnostic mechanism
// (paths must already exist, initialized by a type-specific setup step via
// SetProgressDetails).
func applyProgressUpdatesToJob(job *model.Job, delta int, updates []model.ProgressUpdate) error {
	if len(updates) == 0 {
		return nil
	}
	if len(job.Progress.Details) == 0 {
		return fmt.Errorf("progressDetails is empty, cannot apply update targets")
	}

	for _, u := range updates {
		d := delta
		if u.Operation == model.ProgressOperationSUBTRACT {
			d = -d
		}
		if err := incrJSONPath(job.Progress.Details, u.Path, d); err != nil {
			return err
		}
	}

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
