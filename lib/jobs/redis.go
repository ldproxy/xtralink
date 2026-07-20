package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// maxRetries mirrors AbstractJobQueueBackend's retry cap. The current
// xtraplatform-redis Java backend has an unfinished stub for error()
// (always returns false, no retry) - this reimplements the intended
// retry-then-fail behavior from the abstract base class.
const maxRetries = 3

// RedisBackend implements Backend directly against Redis (RedisJSON module
// required), with no AbstractJobQueueBackend-style base class: MemoryBackend
// is the only other implementation, and the two share nothing beyond the
// Backend interface itself, so the extra layer of Java's template-method
// abstraction is not reproduced.
type RedisBackend struct {
	client redis.UniversalClient

	// Redis key schema, analogous to JobQueueBackendRedis.java but
	// namespaced under xtrasync (not xtraplatform) to avoid colliding with a
	// real xtraplatform instance sharing the same Redis. Each key is scoped
	// by cluster (s. NewRedisBackend), so only instances sharing the same
	// configuration - and thus the same cluster - share a queue.
	keyPriorities string
	keyQueue      string
	keyPartial    string
	keyJob        string
	keyTaken      string
	keyFailed     string
	// keyFinalized must NOT share the keyJob prefix: GetJobs() lists every
	// key matching keyJob+"*" and JSON.GETs it, so a plain SETNX string key
	// under that same prefix breaks it ("wrong Redis type"). Own prefix.
	keyFinalized string
}

// NewRedisBackend connects lazily (go-redis does not dial until the first
// command), so constructing this at startup never blocks or fails other
// commands when Redis is unavailable. nodes is host:port entries; a single
// entry connects to one Redis/Valkey node, more than one switches to
// cluster mode - go-redis's UniversalClient already picks the right client
// type for either case, mirroring RedisImpl.java's manual
// RedisClient/RedisClusterClient switch without having to reproduce it.
//
// cluster scopes every key this backend uses, so only instances sharing the
// same configuration share a queue; if empty, it falls back to the
// hostname (best-effort - left empty if even that fails).
func NewRedisBackend(nodes []string, cluster string) *RedisBackend {
	if cluster == "" {
		if host, err := os.Hostname(); err == nil {
			cluster = host
		}
	}

	prefix := "xtrasync:jobs:"
	if cluster != "" {
		prefix += cluster + ":"
	}

	return &RedisBackend{
		client:        redis.NewUniversalClient(&redis.UniversalOptions{Addrs: nodes}),
		keyPriorities: prefix + "priorities:",
		keyQueue:      prefix + "queue:",
		keyPartial:    prefix + "partial:",
		keyJob:        prefix + "job:",
		keyTaken:      prefix + "taken",
		keyFailed:     prefix + "failed",
		keyFinalized:  prefix + "finalized:",
	}
}

func (b *RedisBackend) IsEnabled() bool {
	return true
}

func (b *RedisBackend) queueKey(partialJobType string, priority int) string {
	return b.keyQueue + partialJobType + ":" + strconv.Itoa(priority)
}

// go-redis/v9 ships typed methods for the RedisJSON module (JSONSet,
// JSONGet, JSONDel, JSONNumIncrBy, ...) - these thin wrappers just fix the
// path convention ("$" for whole-document ops) used throughout this file,
// no raw command dispatch via Do() is needed.
func (b *RedisBackend) jsonSet(ctx context.Context, key, path string, value any) error {
	return b.client.JSONSet(ctx, key, path, value).Err()
}

// jsonGet reads the document at $ and unmarshals it into out. It returns
// (false, nil) if the key does not exist.
func (b *RedisBackend) jsonGet(ctx context.Context, key string, out any) (bool, error) {
	res, err := b.client.JSONGet(ctx, key, "$").Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	var wrapped []json.RawMessage
	if err := json.Unmarshal([]byte(res), &wrapped); err != nil {
		return false, err
	}
	if len(wrapped) == 0 {
		return false, nil
	}
	if err := json.Unmarshal(wrapped[0], out); err != nil {
		return false, err
	}
	return true, nil
}

func (b *RedisBackend) jsonNumIncrBy(ctx context.Context, key, path string, delta int) error {
	return b.client.JSONNumIncrBy(ctx, key, path, float64(delta)).Err()
}

func (b *RedisBackend) jsonDel(ctx context.Context, key string) error {
	return b.client.JSONDel(ctx, key, "$").Err()
}

func (b *RedisBackend) registerPriority(ctx context.Context, partialJobType string, priority int) error {
	return b.client.ZAdd(ctx, b.keyPriorities+partialJobType, redis.Z{
		Score:  float64(priority),
		Member: strconv.Itoa(priority),
	}).Err()
}

func (b *RedisBackend) priorities(ctx context.Context, partialJobType string) ([]int, error) {
	vals, err := b.client.ZRevRange(ctx, b.keyPriorities+partialJobType, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	result := make([]int, 0, len(vals))
	for _, v := range vals {
		p, err := strconv.Atoi(v)
		if err != nil {
			continue
		}
		result = append(result, p)
	}
	return result, nil
}

func (b *RedisBackend) putPartialJob(ctx context.Context, partialJob *PartialJob) error {
	return b.jsonSet(ctx, b.keyPartial+partialJob.ID, "$", partialJob)
}

func (b *RedisBackend) getPartialJob(ctx context.Context, id string) (*PartialJob, error) {
	var partialJob PartialJob
	ok, err := b.jsonGet(ctx, b.keyPartial+id, &partialJob)
	if err != nil || !ok {
		return nil, err
	}
	return &partialJob, nil
}

func (b *RedisBackend) putJob(ctx context.Context, job *Job) error {
	return b.jsonSet(ctx, b.keyJob+job.ID, "$", job)
}

func (b *RedisBackend) getJob(ctx context.Context, id string) (*Job, error) {
	var job Job
	ok, err := b.jsonGet(ctx, b.keyJob+id, &job)
	if err != nil || !ok {
		return nil, err
	}
	return &job, nil
}

func (b *RedisBackend) PushJob(job *Job) error {
	ctx := context.Background()
	if err := b.putJob(ctx, job); err != nil {
		return err
	}
	if job.Setup != nil {
		return b.PushPartialJob(job.Setup, false)
	}
	return nil
}

func (b *RedisBackend) PushPartialJob(partialJob *PartialJob, untake bool) error {
	ctx := context.Background()
	queue := b.queueKey(partialJob.Type, partialJob.Priority)

	// Only a fresh push registers a Sequence slot - untake is a re-queue of
	// a PartialJob already counted once at its original push, incrementing
	// again would double-count it.
	if !untake {
		if err := b.registerSequence(ctx, partialJob.PartOf, partialJob.Sequence, 1); err != nil {
			return err
		}
	}

	if err := b.registerPriority(ctx, partialJob.Type, partialJob.Priority); err != nil {
		return err
	}
	if err := b.putPartialJob(ctx, partialJob); err != nil {
		return err
	}

	if untake {
		if err := b.client.LRem(ctx, b.keyTaken, 1, partialJob.ID).Err(); err != nil {
			return err
		}
		return b.client.RPush(ctx, queue, partialJob.ID).Err()
	}
	return b.client.LPush(ctx, queue, partialJob.ID).Err()
}

// Take scans partialJobType's queue, highest priority first, for the first
// PartialJob whose sequence gate is currently open (s. sequenceReady) -
// almost always just the very next one, unless its parent Job has
// Parallel=false and it isn't its Sequence's turn yet, in which case it is
// skipped in favor of a later (but eligible) one.
func (b *RedisBackend) Take(partialJobType, executor string) (*PartialJob, error) {
	ctx := context.Background()

	priorities, err := b.priorities(ctx, partialJobType)
	if err != nil {
		return nil, err
	}

	for _, p := range priorities {
		queue := b.queueKey(partialJobType, p)
		partialJob, err := b.takeEligibleFromQueue(ctx, queue, executor)
		if err != nil {
			return nil, err
		}
		if partialJob != nil {
			return partialJob, nil
		}
	}

	return nil, nil
}

// takeEligibleFromQueue scans queue in pop order (tail first, the order
// LMove RIGHT used before this method existed) for the first PartialJob
// that is currently eligible (s. sequenceReady), claims it via LREM, and
// returns nil if nothing in queue is eligible right now - not necessarily
// because the queue is empty, but because everything left is waiting its
// turn. LRANGE-then-LREM (rather than the single atomic LMove this
// replaces) leaves a narrow window where a concurrent Take() could claim
// the same id first; LREM reporting 0 removed detects that and simply
// re-scans.
func (b *RedisBackend) takeEligibleFromQueue(ctx context.Context, queue, executor string) (*PartialJob, error) {
	for {
		ids, err := b.client.LRange(ctx, queue, 0, -1).Result()
		if err != nil {
			return nil, err
		}

		claimedID := ""
		for i := len(ids) - 1; i >= 0; i-- {
			partialJob, err := b.getPartialJob(ctx, ids[i])
			if err != nil {
				return nil, err
			}
			if partialJob == nil {
				continue
			}
			ready, err := b.sequenceReady(ctx, partialJob)
			if err != nil {
				return nil, err
			}
			if ready {
				claimedID = ids[i]
				break
			}
		}
		if claimedID == "" {
			return nil, nil
		}

		n, err := b.client.LRem(ctx, queue, 1, claimedID).Result()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			continue // someone else claimed it between LRange and LRem - rescan
		}
		if err := b.client.LPush(ctx, b.keyTaken, claimedID).Err(); err != nil {
			return nil, err
		}

		partialJob, err := b.getPartialJob(ctx, claimedID)
		if err != nil || partialJob == nil {
			return nil, err
		}
		now := nowMillis()
		partialJob.Executor = &executor
		partialJob.StartedAt = now
		partialJob.UpdatedAt = now
		if err := b.putPartialJob(ctx, partialJob); err != nil {
			return nil, err
		}
		return partialJob, nil
	}
}

// sequenceReady reports whether partialJob may run right now: always true
// for a standalone PartialJob (no parent Job) or one whose parent Job has
// Parallel=true (the default, plain sharding - no ordering constraint);
// otherwise only once its parent's CurrentSequence has reached its own
// Sequence.
func (b *RedisBackend) sequenceReady(ctx context.Context, partialJob *PartialJob) (bool, error) {
	if partialJob.PartOf == "" {
		return true, nil
	}
	job, err := b.getJob(ctx, partialJob.PartOf)
	if err != nil {
		return false, err
	}
	if job == nil || job.Parallel {
		return true, nil
	}
	return partialJob.Sequence == job.CurrentSequence, nil
}

// Done removes partialJobID from the taken list, runs the setup/cleanup/
// followUps decision if the PartialJob belongs to a Job (onPartialJobDone),
// and discards the PartialJob's document - the finished PartialJob state
// itself is not persisted, matching JobQueueBackendRedis.doneJob.
func (b *RedisBackend) Done(partialJobID string) error {
	ctx := context.Background()

	n, err := b.client.LRem(ctx, b.keyTaken, 1, partialJobID).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	partialJob, err := b.getPartialJob(ctx, partialJobID)
	if err != nil {
		return err
	}
	if partialJob != nil && partialJob.PartOf != "" {
		if err := b.onPartialJobDone(ctx, partialJob); err != nil {
			return err
		}
	}

	return b.jsonDel(ctx, b.keyPartial+partialJobID)
}

// onPartialJobDone mirrors JobSet.done(job) + AbstractJobQueueBackend.
// onJobFinished in Java: a finishing setup PartialJob just syncs its
// embedded snapshot (it already pushed its PartialJobs itself); a finishing
// cleanup PartialJob syncs its snapshot and pushes followUps; any other
// PartialJob finishing hands off to finalizeIfDone.
//
// partialJob.Errors is deliberately NOT merged into the Job here: Error(...,
// retry=true) appends a message on every retry attempt, so a PartialJob that
// fails twice and then succeeds still carries those messages - merging them
// here would mark the Job failed despite every PartialJob having succeeded.
// Only a PartialJob's permanent failure (onPartialJobPermanentlyFailed)
// should surface errors on the Job.
func (b *RedisBackend) onPartialJobDone(ctx context.Context, partialJob *PartialJob) error {
	job, err := b.getJob(ctx, partialJob.PartOf)
	if err != nil || job == nil {
		return err
	}

	if job.Setup != nil && job.Setup.ID == partialJob.ID {
		return b.syncEmbeddedPartialJob(ctx, job.ID, "setup", partialJob)
	}
	if job.Cleanup != nil && job.Cleanup.ID == partialJob.ID {
		if err := b.syncEmbeddedPartialJob(ctx, job.ID, "cleanup", partialJob); err != nil {
			return err
		}
		if err := b.clearProgressDetailsOnSuccess(ctx, job.ID); err != nil {
			return err
		}
		return b.pushFollowUps(job)
	}

	if err := b.jsonSet(ctx, b.keyJob+job.ID, "$.updatedAt", nowMillis()); err != nil {
		return err
	}
	if !job.Parallel {
		if err := b.advanceSequence(ctx, job.ID, partialJob.Sequence); err != nil {
			return err
		}
	}

	return b.finalizeIfDone(ctx, job)
}

// syncEmbeddedPartialJob patches the Job's embedded setup/cleanup snapshot
// with the just-finished PartialJob's final state, since Done() deletes the
// standalone PartialJob document (matching JobQueueBackendRedis.doneJob) -
// without this, the embedded copy would stay frozen at its initial state
// forever.
func (b *RedisBackend) syncEmbeddedPartialJob(ctx context.Context, jobID, field string, partialJob *PartialJob) error {
	done := *partialJob
	done.UpdatedAt = nowMillis()
	if done.FinishedAt <= 0 {
		done.FinishedAt = done.UpdatedAt
	}
	return b.jsonSet(ctx, b.keyJob+jobID, "$."+field, done)
}

// onPartialJobPermanentlyFailed handles a PartialJob that Error() gave up
// retrying on. Java has no equivalent (AbstractJobQueueBackend.error()
// never calls onJobFinished), so without this a permanently failed
// PartialJob's Job would hang forever (current can never reach total) - its
// unfinished share (total-current) is subtracted from the Job's total
// instead.
//
// A failed Setup PartialJob never created any PartialJobs, so isDone() can
// never trigger either - forceFail() ends the Job as failed directly. A
// failed Cleanup PartialJob means the Job is already finished; its error
// just needs merging so Status() reports failed instead of successful.
func (b *RedisBackend) onPartialJobPermanentlyFailed(ctx context.Context, partialJob *PartialJob) error {
	job, err := b.getJob(ctx, partialJob.PartOf)
	if err != nil || job == nil {
		return err
	}
	if job.Setup != nil && job.Setup.ID == partialJob.ID {
		if err := b.syncEmbeddedPartialJob(ctx, job.ID, "setup", partialJob); err != nil {
			return err
		}
		// No PartialJobs exist to trigger finalizeIfDone's isDone() check,
		// so force the Job to a failed end state directly.
		return b.forceFail(ctx, job, partialJob.Errors)
	}
	if job.Cleanup != nil && job.Cleanup.ID == partialJob.ID {
		if err := b.syncEmbeddedPartialJob(ctx, job.ID, "cleanup", partialJob); err != nil {
			return err
		}
		// Job is already finished; just surface the error so Status()
		// reports failed instead of successful.
		return b.mergeErrors(ctx, job.ID, partialJob.Errors)
	}

	if remaining := partialJob.Total - partialJob.Current; remaining > 0 {
		if err := b.jsonNumIncrBy(ctx, b.keyJob+job.ID, "$.total", -remaining); err != nil {
			return err
		}
	}
	if err := b.mergeErrors(ctx, job.ID, partialJob.Errors); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, b.keyJob+job.ID, "$.updatedAt", nowMillis()); err != nil {
		return err
	}
	if !job.Parallel {
		if err := b.advanceSequence(ctx, job.ID, partialJob.Sequence); err != nil {
			return err
		}
	}

	return b.finalizeIfDone(ctx, job)
}

// mergeErrors appends to Job.errors via the atomic JSON.ARRAPPEND command,
// so concurrent permanent failures on different PartialJobs of the same Job
// can never lose an error message to a stale read - a GET-then-SET
// read-modify-write (the previous implementation) could.
func (b *RedisBackend) mergeErrors(ctx context.Context, jobID string, errors []string) error {
	if len(errors) == 0 {
		return nil
	}
	values := make([]interface{}, len(errors))
	for i, e := range errors {
		encoded, err := json.Marshal(e)
		if err != nil {
			return err
		}
		values[i] = string(encoded)
	}
	return b.client.JSONArrAppend(ctx, b.keyJob+jobID, "$.errors", values...).Err()
}

// finalizeIfDone re-reads the Job (to see current/total after any
// concurrent atomic updates elsewhere) and, if every PartialJob is now
// accounted for, claims the right to finalize it via a Redis SETNX lock -
// if two PartialJobs finish at the same instant, only one wins and proceeds
// to set finishedAt and push cleanup/followUps; the other is a no-op. A
// plain "finishedAt <= 0" check in Go can't close this race, since both
// goroutines could observe "not yet finished" before either writes.
func (b *RedisBackend) finalizeIfDone(ctx context.Context, job *Job) error {
	fresh, err := b.getJob(ctx, job.ID)
	if err != nil || fresh == nil {
		return err
	}
	if !fresh.IsDone() || fresh.FinishedAt > 0 {
		return nil
	}

	claimed, err := b.client.SetNX(ctx, b.keyFinalized+job.ID, "1", 24*time.Hour).Result()
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	if err := b.jsonSet(ctx, b.keyJob+job.ID, "$.finishedAt", nowMillis()); err != nil {
		return err
	}
	if job.Cleanup != nil {
		return b.PushPartialJob(job.Cleanup, false)
	}
	// No cleanup step, so this is the final outcome - decide now instead of
	// waiting for a cleanup PartialJob that will never come.
	if err := b.clearProgressDetailsOnSuccess(ctx, job.ID); err != nil {
		return err
	}
	return b.pushFollowUps(job)
}

// clearProgressDetailsOnSuccess discards Job.progressDetails once a Job has
// fully finished without errors - it is deliberately kept intact on
// failure, for diagnosis.
func (b *RedisBackend) clearProgressDetailsOnSuccess(ctx context.Context, jobID string) error {
	job, err := b.getJob(ctx, jobID)
	if err != nil || job == nil {
		return err
	}
	if job.HasErrors() {
		return nil
	}
	return b.jsonSet(ctx, b.keyJob+jobID, "$.progressDetails", nil)
}

// forceFail marks a Job as finished-with-errors regardless of isDone() -
// for the case where current can never reach total because a permanently
// failed setup PartialJob means no PartialJobs were ever created. Uses the
// same keyFinalized SETNX claim as finalizeIfDone, both to stay consistent
// and so this can never race with (or duplicate) a normal finalization.
func (b *RedisBackend) forceFail(ctx context.Context, job *Job, errors []string) error {
	if err := b.mergeErrors(ctx, job.ID, errors); err != nil {
		return err
	}

	claimed, err := b.client.SetNX(ctx, b.keyFinalized+job.ID, "1", 24*time.Hour).Result()
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	return b.jsonSet(ctx, b.keyJob+job.ID, "$.finishedAt", nowMillis())
}

func (b *RedisBackend) pushFollowUps(job *Job) error {
	for _, followUp := range job.FollowUps {
		if err := b.PushJob(followUp); err != nil {
			return err
		}
	}
	return nil
}

// StartJob sets Job.startedAt to now (mirrors JobSet.start() in Java).
func (b *RedisBackend) StartJob(jobID string) error {
	return b.jsonSet(context.Background(), b.keyJob+jobID, "$.startedAt", nowMillis())
}

// SetProgressDetails overwrites Job.progressDetails wholesale - the
// one-time, type-specific initial build done by a setup JobProcessor.
func (b *RedisBackend) SetProgressDetails(jobID string, details any) error {
	return b.jsonSet(context.Background(), b.keyJob+jobID, "$.progressDetails", details)
}

// SetOutput writes a single outputs entry, keyed by name.
func (b *RedisBackend) SetOutput(jobID, key string, value OutputValue) error {
	return b.jsonSet(context.Background(), b.keyJob+jobID, "$.outputs."+key, value)
}

// Error mirrors the retry/fail semantics from AbstractJobQueueBackend.error();
// the current xtraplatform-redis Java backend has this as an unfinished stub
// (always returns false, no retry), which this reimplements properly.
func (b *RedisBackend) Error(partialJobID, message string, retry bool) error {
	ctx := context.Background()

	n, err := b.client.LRem(ctx, b.keyTaken, 1, partialJobID).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	partialJob, err := b.getPartialJob(ctx, partialJobID)
	if err != nil {
		return err
	}
	if partialJob == nil {
		return nil
	}

	partialJob.Errors = append(partialJob.Errors, message)
	partialJob.UpdatedAt = nowMillis()

	if retry && len(partialJob.Errors) <= maxRetries {
		return b.PushPartialJob(partialJob, true)
	}

	if err := b.putPartialJob(ctx, partialJob); err != nil {
		return err
	}
	if err := b.client.RPush(ctx, b.keyFailed, partialJobID).Err(); err != nil {
		return err
	}

	if partialJob.PartOf != "" {
		return b.onPartialJobPermanentlyFailed(ctx, partialJob)
	}
	return nil
}

func (b *RedisBackend) GetJobs() ([]*Job, error) {
	ctx := context.Background()

	keys, err := b.client.Keys(ctx, b.keyJob+"*").Result()
	if err != nil {
		return nil, err
	}

	result := make([]*Job, 0, len(keys))
	for _, k := range keys {
		id := strings.TrimPrefix(k, b.keyJob)
		job, err := b.getJob(ctx, id)
		if err != nil {
			return nil, err
		}
		if job != nil {
			result = append(result, job)
		}
	}
	return result, nil
}

func (b *RedisBackend) GetJob(id string) (*Job, error) {
	return b.getJob(context.Background(), id)
}

func (b *RedisBackend) GetOpen(partialJobType string) ([]*PartialJob, error) {
	ctx := context.Background()

	priorities, err := b.priorities(ctx, partialJobType)
	if err != nil {
		return nil, err
	}

	partialJobs := make([]*PartialJob, 0)
	for _, p := range priorities {
		ids, err := b.client.LRange(ctx, b.queueKey(partialJobType, p), 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			partialJob, err := b.getPartialJob(ctx, id)
			if err != nil {
				return nil, err
			}
			if partialJob != nil {
				partialJobs = append(partialJobs, partialJob)
			}
		}
	}
	return partialJobs, nil
}

func (b *RedisBackend) getPartialJobsByIDList(ctx context.Context, listKey string) ([]*PartialJob, error) {
	ids, err := b.client.LRange(ctx, listKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	partialJobs := make([]*PartialJob, 0, len(ids))
	for _, id := range ids {
		partialJob, err := b.getPartialJob(ctx, id)
		if err != nil {
			return nil, err
		}
		if partialJob != nil {
			partialJobs = append(partialJobs, partialJob)
		}
	}
	return partialJobs, nil
}

func (b *RedisBackend) GetTaken() ([]*PartialJob, error) {
	return b.getPartialJobsByIDList(context.Background(), b.keyTaken)
}

func (b *RedisBackend) GetFailed() ([]*PartialJob, error) {
	return b.getPartialJobsByIDList(context.Background(), b.keyFailed)
}

func (b *RedisBackend) InitJob(jobID string, totalDelta int, updates []ProgressUpdate) error {
	ctx := context.Background()
	key := b.keyJob + jobID

	if err := b.jsonNumIncrBy(ctx, key, "$.total", totalDelta); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, key, "$.updatedAt", nowMillis()); err != nil {
		return err
	}
	return b.applyProgressUpdates(ctx, key, totalDelta, updates)
}

func (b *RedisBackend) UpdateJob(jobID string, currentDelta int, updates []ProgressUpdate) error {
	ctx := context.Background()
	key := b.keyJob + jobID

	if err := b.jsonNumIncrBy(ctx, key, "$.current", currentDelta); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, key, "$.updatedAt", nowMillis()); err != nil {
		return err
	}
	return b.applyProgressUpdates(ctx, key, currentDelta, updates)
}

// applyProgressUpdates is the generic, job-type-agnostic mechanism: it
// applies the same delta reported for PartialJob.current to each declared
// progressDetails path via an atomic JSON.NUMINCRBY, without the queue core
// needing to know the job type. The paths must already exist (initialized
// by the type-specific setup step, e.g. tileseedingdemo.SetupProcessor.setup
// via SetProgressDetails).
func (b *RedisBackend) applyProgressUpdates(ctx context.Context, key string, delta int, updates []ProgressUpdate) error {
	for _, u := range updates {
		d := delta
		if u.Op == ProgressOpSubtract {
			d = -d
		}
		if err := b.jsonNumIncrBy(ctx, key, "$.progressDetails."+u.Path, d); err != nil {
			return err
		}
	}
	return nil
}

// UpdatePartialJob reports progress for a single PartialJob: it applies
// currentDelta to PartialJob.current and, if the PartialJob carries a
// progress-update descriptor (attached at creation time in setup), applies
// the same delta to its Job's current/progressDetails. This is the single
// generic entry point a JobProcessor calls with just (partialJobID, delta) -
// the core never needs to know the job type, only what the PartialJob's own
// UpdateTargets declare.
func (b *RedisBackend) UpdatePartialJob(partialJobID string, currentDelta int) error {
	ctx := context.Background()
	key := b.keyPartial + partialJobID

	partialJob, err := b.getPartialJob(ctx, partialJobID)
	if err != nil {
		return err
	}
	if partialJob == nil {
		return fmt.Errorf("partial job not found: %s", partialJobID)
	}

	if err := b.jsonNumIncrBy(ctx, key, "$.current", currentDelta); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, key, "$.updatedAt", nowMillis()); err != nil {
		return err
	}

	if partialJob.PartOf != "" && len(partialJob.UpdateTargets) > 0 {
		return b.UpdateJob(partialJob.PartOf, currentDelta, partialJob.UpdateTargets)
	}
	return nil
}

// registerSequence atomically applies delta to Job.sequenceRemaining[sequence]
// for a Parallel=false Job (a no-op otherwise, and a no-op for a standalone
// PartialJob with jobID==""), via the same JSON.NUMINCRBY technique
// BaseJob.Total/Current already use, so PartialJobs of the same Sequence
// finishing at once can never lose an update to a stale whole-document
// read. JSON.SET ... NX seeds the leaf to 0 first (idempotent, a no-op if
// another goroutine/process already did it) since NUMINCRBY refuses to
// operate on a path that doesn't exist yet - Job.SequenceRemaining is
// deliberately never omitted from the stored JSON (s. model.go) so that
// parent object is always there for the leaf to be added to.
func (b *RedisBackend) registerSequence(ctx context.Context, jobID string, sequence int, delta int) error {
	if jobID == "" {
		return nil
	}
	job, err := b.getJob(ctx, jobID)
	if err != nil || job == nil || job.Parallel {
		return err
	}

	key := b.keyJob + jobID
	path := fmt.Sprintf("$.sequenceRemaining.%d", sequence)
	// NX reports redis.Nil (a "null bulk reply") when the leaf already
	// exists - that's the expected, harmless case (someone got there
	// first), not a real error; only a different error is.
	if err := b.client.JSONSetMode(ctx, key, path, 0, "NX").Err(); err != nil && err != redis.Nil {
		return err
	}
	return b.jsonNumIncrBy(ctx, key, path, delta)
}

// advanceSequence decrements Job.sequenceRemaining[sequence] (the
// PartialJob that just finished or permanently failed) and, once a
// Sequence's count reaches zero, advances Job.currentSequence to the next
// one already known (registered by an earlier PushPartialJob covering all
// Sequences up front) - repeating in case that next Sequence turns out to
// already be empty too. A no-op for a Parallel=true Job (checked by the
// caller before this is ever invoked).
//
// The decrement itself is atomic (s. registerSequence), but the
// read-decide-advance step that follows re-reads the whole Job and is not
// itself atomic against another PartialJob of the very same Sequence
// finishing at the exact same instant - a narrow race specific to
// Parallel=false with more than one PartialJob per Sequence, accepted for
// now (every future completion re-triggers this check, so a missed
// advance is self-correcting as long as something eventually finishes at
// the Sequence current-sequence is stuck on).
func (b *RedisBackend) advanceSequence(ctx context.Context, jobID string, sequence int) error {
	if err := b.registerSequence(ctx, jobID, sequence, -1); err != nil {
		return err
	}

	job, err := b.getJob(ctx, jobID)
	if err != nil || job == nil {
		return err
	}

	advanced := false
	for {
		remaining, ok := job.SequenceRemaining[job.CurrentSequence]
		if !ok || remaining > 0 {
			break
		}
		next := job.CurrentSequence + 1
		if _, exists := job.SequenceRemaining[next]; !exists {
			break
		}
		job.CurrentSequence = next
		advanced = true
	}
	if !advanced {
		return nil
	}
	return b.jsonSet(ctx, b.keyJob+jobID, "$.currentSequence", job.CurrentSequence)
}
