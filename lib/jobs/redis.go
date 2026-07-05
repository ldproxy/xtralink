package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key schema, analogous to JobQueueBackendRedis.java but namespaced
// under xtrasync (not xtraplatform) to avoid colliding with a real
// xtraplatform instance sharing the same Redis.
const (
	keyPriorities = "xtrasync:jobs:priorities:"
	keyQueue      = "xtrasync:jobs:queue:"
	keyJob        = "xtrasync:jobs:job:"
	keySet        = "xtrasync:jobs:set:"
	keyTaken      = "xtrasync:jobs:taken"
	keyFailed     = "xtrasync:jobs:failed"
	// keyFinalized must NOT share the keySet prefix: GetSets() lists every
	// key matching keySet+"*" and JSON.GETs it, so a plain SETNX string key
	// under that same prefix breaks it ("wrong Redis type"). Own prefix.
	keyFinalized = "xtrasync:jobs:finalized:"

	// maxRetries mirrors AbstractJobQueueBackend's retry cap. The current
	// xtraplatform-redis Java backend has an unfinished stub for error()
	// (always returns false, no retry) - this reimplements the intended
	// retry-then-fail behavior from the abstract base class.
	maxRetries = 3
)

// RedisBackend implements Backend directly against Redis (RedisJSON module
// required), with no AbstractJobQueueBackend-style base class: there is only
// one backend implementation, so the extra layer of Java's template-method
// abstraction is not reproduced.
type RedisBackend struct {
	client *redis.Client
}

// NewRedisBackend connects lazily (go-redis does not dial until the first
// command), so constructing this at startup never blocks or fails other
// commands when Redis is unavailable.
func NewRedisBackend(addr string) *RedisBackend {
	return &RedisBackend{
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}

func (b *RedisBackend) IsEnabled() bool {
	return true
}

func queueKey(jobType string, priority int) string {
	return keyQueue + jobType + ":" + strconv.Itoa(priority)
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

func (b *RedisBackend) registerPriority(ctx context.Context, jobType string, priority int) error {
	return b.client.ZAdd(ctx, keyPriorities+jobType, redis.Z{
		Score:  float64(priority),
		Member: strconv.Itoa(priority),
	}).Err()
}

func (b *RedisBackend) priorities(ctx context.Context, jobType string) ([]int, error) {
	vals, err := b.client.ZRevRange(ctx, keyPriorities+jobType, 0, -1).Result()
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

func (b *RedisBackend) putJob(ctx context.Context, job *Job) error {
	return b.jsonSet(ctx, keyJob+job.ID, "$", job)
}

func (b *RedisBackend) getJob(ctx context.Context, id string) (*Job, error) {
	var job Job
	ok, err := b.jsonGet(ctx, keyJob+id, &job)
	if err != nil || !ok {
		return nil, err
	}
	return &job, nil
}

func (b *RedisBackend) putJobSet(ctx context.Context, js *JobSet) error {
	return b.jsonSet(ctx, keySet+js.ID, "$", js)
}

func (b *RedisBackend) getJobSet(ctx context.Context, id string) (*JobSet, error) {
	var js JobSet
	ok, err := b.jsonGet(ctx, keySet+id, &js)
	if err != nil || !ok {
		return nil, err
	}
	return &js, nil
}

func (b *RedisBackend) PushJobSet(js *JobSet) error {
	ctx := context.Background()
	if err := b.putJobSet(ctx, js); err != nil {
		return err
	}
	if js.Setup != nil {
		return b.PushJob(js.Setup, false)
	}
	return nil
}

func (b *RedisBackend) PushJob(job *Job, untake bool) error {
	ctx := context.Background()
	queue := queueKey(job.Type, job.Priority)

	if err := b.registerPriority(ctx, job.Type, job.Priority); err != nil {
		return err
	}
	if err := b.putJob(ctx, job); err != nil {
		return err
	}

	if untake {
		if err := b.client.LRem(ctx, keyTaken, 1, job.ID).Err(); err != nil {
			return err
		}
		return b.client.RPush(ctx, queue, job.ID).Err()
	}
	return b.client.LPush(ctx, queue, job.ID).Err()
}

func (b *RedisBackend) Take(jobType, executor string) (*Job, error) {
	ctx := context.Background()

	priorities, err := b.priorities(ctx, jobType)
	if err != nil {
		return nil, err
	}

	for _, p := range priorities {
		queue := queueKey(jobType, p)
		jobID, err := b.client.LMove(ctx, queue, keyTaken, "RIGHT", "LEFT").Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}

		job, err := b.getJob(ctx, jobID)
		if err != nil || job == nil {
			return nil, err
		}

		now := nowMillis()
		job.Executor = &executor
		job.StartedAt = now
		job.UpdatedAt = now
		if err := b.putJob(ctx, job); err != nil {
			return nil, err
		}
		return job, nil
	}

	return nil, nil
}

// Done removes jobID from the taken list, runs the setup/cleanup/followUps
// decision if the Job belongs to a JobSet (onJobDone), and discards the
// Job's document - the finished Job state itself is not persisted, matching
// JobQueueBackendRedis.doneJob.
func (b *RedisBackend) Done(jobID string) error {
	ctx := context.Background()

	n, err := b.client.LRem(ctx, keyTaken, 1, jobID).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	job, err := b.getJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job != nil && job.PartOf != "" {
		if err := b.onJobDone(ctx, job); err != nil {
			return err
		}
	}

	return b.jsonDel(ctx, keyJob+jobID)
}

// onJobDone mirrors JobSet.done(job) + AbstractJobQueueBackend.onJobFinished
// in Java: the setup Job finishing syncs its embedded snapshot (the setup
// processor already pushed the sub-Jobs itself, nothing else to do here);
// the cleanup Job finishing syncs its snapshot and pushes the set's
// followUps; any other (regular) sub-Job finishing merges its errors into
// the JobSet and hands off to finalizeIfDone.
func (b *RedisBackend) onJobDone(ctx context.Context, job *Job) error {
	js, err := b.getJobSet(ctx, job.PartOf)
	if err != nil || js == nil {
		return err
	}

	if js.Setup != nil && js.Setup.ID == job.ID {
		return b.syncEmbeddedJob(ctx, js.ID, "setup", job)
	}
	if js.Cleanup != nil && js.Cleanup.ID == job.ID {
		if err := b.syncEmbeddedJob(ctx, js.ID, "cleanup", job); err != nil {
			return err
		}
		return b.pushFollowUps(js)
	}

	if err := b.mergeErrors(ctx, js.ID, job.Errors); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, keySet+js.ID, "$.updatedAt", nowMillis()); err != nil {
		return err
	}

	return b.finalizeIfDone(ctx, js)
}

// syncEmbeddedJob patches the JobSet's embedded setup/cleanup snapshot with
// the just-finished Job's final state. Without this, the snapshot stays
// frozen at whatever it looked like when the JobSet was first created:
// finished Job state is never persisted standalone (Done() deletes the Job
// document, matching JobQueueBackendRedis.doneJob), so the embedded copy is
// the only place this state could still show up - but only if something
// writes it there before the standalone document disappears.
func (b *RedisBackend) syncEmbeddedJob(ctx context.Context, jobSetID, field string, job *Job) error {
	done := *job
	done.UpdatedAt = nowMillis()
	if done.FinishedAt <= 0 {
		done.FinishedAt = done.UpdatedAt
	}
	return b.jsonSet(ctx, keySet+jobSetID, "$."+field, done)
}

// onJobPermanentlyFailed handles a Job that Error() gave up retrying on
// (retry exhausted or retry=false). Diagram/Java don't cover this case -
// AbstractJobQueueBackend.error() never calls onJobFinished either, so a
// permanently failed regular sub-Job's JobSet would otherwise hang forever
// (current can never reach total). Since nothing will ever again report
// progress for this Job, its unfinished share (total-current) is subtracted
// from the JobSet's total so current can still catch up.
//
// If the permanently failed Job is Setup itself, no sub-Jobs were ever
// created, so isDone() can never trigger either - forceFail() ends the
// JobSet as failed directly. If it is Cleanup, the JobSet is already
// finished (that's what caused cleanup to be pushed); its error just needs
// to be merged so Status() reports failed instead of successful.
func (b *RedisBackend) onJobPermanentlyFailed(ctx context.Context, job *Job) error {
	js, err := b.getJobSet(ctx, job.PartOf)
	if err != nil || js == nil {
		return err
	}
	if js.Setup != nil && js.Setup.ID == job.ID {
		if err := b.syncEmbeddedJob(ctx, js.ID, "setup", job); err != nil {
			return err
		}
		// Setup failing means no sub-Jobs were ever created, so current can
		// never reach total and finalizeIfDone's isDone() check would never
		// fire - without this, the JobSet would hang at "accepted" forever
		// instead of ending up "failed".
		return b.forceFail(ctx, js, job.Errors)
	}
	if js.Cleanup != nil && js.Cleanup.ID == job.ID {
		if err := b.syncEmbeddedJob(ctx, js.ID, "cleanup", job); err != nil {
			return err
		}
		// The JobSet is already finished (finalizeIfDone ran when the last
		// sub-Job completed, which is what caused cleanup to be pushed) -
		// just make sure this error surfaces so Status() reports failed
		// instead of successful.
		return b.mergeErrors(ctx, js.ID, job.Errors)
	}

	if remaining := job.Total - job.Current; remaining > 0 {
		if err := b.jsonNumIncrBy(ctx, keySet+js.ID, "$.total", -remaining); err != nil {
			return err
		}
	}
	if err := b.mergeErrors(ctx, js.ID, job.Errors); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, keySet+js.ID, "$.updatedAt", nowMillis()); err != nil {
		return err
	}

	return b.finalizeIfDone(ctx, js)
}

func (b *RedisBackend) mergeErrors(ctx context.Context, jobSetID string, errors []string) error {
	if len(errors) == 0 {
		return nil
	}
	js, err := b.getJobSet(ctx, jobSetID)
	if err != nil || js == nil {
		return err
	}
	merged := append(append([]string{}, js.Errors...), errors...)
	return b.jsonSet(ctx, keySet+jobSetID, "$.errors", merged)
}

// finalizeIfDone re-reads the JobSet (to see current/total after any
// concurrent atomic updates elsewhere) and, if every sub-Job is now
// accounted for, atomically claims the right to finalize it via a Redis
// SETNX lock - if two sub-Jobs finish at the exact same instant, only one of
// them wins this and proceeds to set finishedAt and push the cleanup Job (or
// followUps if there is none); the other is a no-op. This closes the
// double-push race that a plain "finishedAt <= 0" check in Go could not
// (both goroutines could observe "not yet finished" before either writes).
func (b *RedisBackend) finalizeIfDone(ctx context.Context, js *JobSet) error {
	fresh, err := b.getJobSet(ctx, js.ID)
	if err != nil || fresh == nil {
		return err
	}
	if !fresh.IsDone() || fresh.FinishedAt > 0 {
		return nil
	}

	claimed, err := b.client.SetNX(ctx, keyFinalized+js.ID, "1", 24*time.Hour).Result()
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	if err := b.jsonSet(ctx, keySet+js.ID, "$.finishedAt", nowMillis()); err != nil {
		return err
	}
	if js.Cleanup != nil {
		return b.PushJob(js.Cleanup, false)
	}
	return b.pushFollowUps(js)
}

// forceFail marks a JobSet as finished-with-errors regardless of isDone() -
// for the case where current can never reach total because a permanently
// failed setup Job means no sub-Jobs were ever created. Uses the same
// keyFinalized SETNX claim as finalizeIfDone, both to stay consistent and
// so this can never race with (or duplicate) a normal finalization.
func (b *RedisBackend) forceFail(ctx context.Context, js *JobSet, errors []string) error {
	if err := b.mergeErrors(ctx, js.ID, errors); err != nil {
		return err
	}

	claimed, err := b.client.SetNX(ctx, keyFinalized+js.ID, "1", 24*time.Hour).Result()
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}

	return b.jsonSet(ctx, keySet+js.ID, "$.finishedAt", nowMillis())
}

func (b *RedisBackend) pushFollowUps(js *JobSet) error {
	for _, followUp := range js.FollowUps {
		if err := b.PushJobSet(followUp); err != nil {
			return err
		}
	}
	return nil
}

// StartJobSet sets JobSet.startedAt to now (Diagram: JobSet.start()).
func (b *RedisBackend) StartJobSet(jobSetID string) error {
	return b.jsonSet(context.Background(), keySet+jobSetID, "$.startedAt", nowMillis())
}

// SetProgressDetails overwrites JobSet.progressDetails wholesale - the
// one-time, type-specific initial build done by a setup JobProcessor.
func (b *RedisBackend) SetProgressDetails(jobSetID string, details any) error {
	return b.jsonSet(context.Background(), keySet+jobSetID, "$.progressDetails", details)
}

// SetOutput writes a single outputs entry, keyed by name.
func (b *RedisBackend) SetOutput(jobSetID, key string, value OutputValue) error {
	return b.jsonSet(context.Background(), keySet+jobSetID, "$.outputs."+key, value)
}

// Error mirrors the retry/fail semantics from AbstractJobQueueBackend.error();
// the current xtraplatform-redis Java backend has this as an unfinished stub
// (always returns false, no retry), which this reimplements properly.
func (b *RedisBackend) Error(jobID, message string, retry bool) error {
	ctx := context.Background()

	n, err := b.client.LRem(ctx, keyTaken, 1, jobID).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	job, err := b.getJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return nil
	}

	job.Errors = append(job.Errors, message)
	job.UpdatedAt = nowMillis()

	if retry && len(job.Errors) <= maxRetries {
		return b.PushJob(job, true)
	}

	if err := b.putJob(ctx, job); err != nil {
		return err
	}
	if err := b.client.RPush(ctx, keyFailed, jobID).Err(); err != nil {
		return err
	}

	if job.PartOf != "" {
		return b.onJobPermanentlyFailed(ctx, job)
	}
	return nil
}

func (b *RedisBackend) GetSets() ([]*JobSet, error) {
	ctx := context.Background()

	keys, err := b.client.Keys(ctx, keySet+"*").Result()
	if err != nil {
		return nil, err
	}

	sets := make([]*JobSet, 0, len(keys))
	for _, k := range keys {
		id := strings.TrimPrefix(k, keySet)
		js, err := b.getJobSet(ctx, id)
		if err != nil {
			return nil, err
		}
		if js != nil {
			sets = append(sets, js)
		}
	}
	return sets, nil
}

func (b *RedisBackend) GetSet(id string) (*JobSet, error) {
	return b.getJobSet(context.Background(), id)
}

func (b *RedisBackend) GetOpen(jobType string) ([]*Job, error) {
	ctx := context.Background()

	priorities, err := b.priorities(ctx, jobType)
	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0)
	for _, p := range priorities {
		ids, err := b.client.LRange(ctx, queueKey(jobType, p), 0, -1).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			job, err := b.getJob(ctx, id)
			if err != nil {
				return nil, err
			}
			if job != nil {
				jobs = append(jobs, job)
			}
		}
	}
	return jobs, nil
}

func (b *RedisBackend) getJobsByIDList(ctx context.Context, listKey string) ([]*Job, error) {
	ids, err := b.client.LRange(ctx, listKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0, len(ids))
	for _, id := range ids {
		job, err := b.getJob(ctx, id)
		if err != nil {
			return nil, err
		}
		if job != nil {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (b *RedisBackend) GetTaken() ([]*Job, error) {
	return b.getJobsByIDList(context.Background(), keyTaken)
}

func (b *RedisBackend) GetFailed() ([]*Job, error) {
	return b.getJobsByIDList(context.Background(), keyFailed)
}

func (b *RedisBackend) InitJobSet(jobSetID string, totalDelta int, updates []ProgressUpdate) error {
	ctx := context.Background()
	key := keySet + jobSetID

	if err := b.jsonNumIncrBy(ctx, key, "$.total", totalDelta); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, key, "$.updatedAt", nowMillis()); err != nil {
		return err
	}
	return b.applyProgressUpdates(ctx, key, totalDelta, updates)
}

func (b *RedisBackend) UpdateJobSet(jobSetID string, currentDelta int, updates []ProgressUpdate) error {
	ctx := context.Background()
	key := keySet + jobSetID

	if err := b.jsonNumIncrBy(ctx, key, "$.current", currentDelta); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, key, "$.updatedAt", nowMillis()); err != nil {
		return err
	}
	return b.applyProgressUpdates(ctx, key, currentDelta, updates)
}

// applyProgressUpdates is the generic, job-type-agnostic mechanism from
// Diagram §4: it applies the same delta reported for Job.current to each
// declared progressDetails path via an atomic JSON.NUMINCRBY, without the
// queue core needing to know the job type. The paths must already exist
// (initialized by the type-specific setup step, e.g.
// tileseedingdemo.SetupProcessor.setup via SetProgressDetails).
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

// UpdateJob reports progress for a single Job: it applies currentDelta to
// Job.current and, if the Job carries a progress-update descriptor (attached
// at creation time in setup, Diagram §4), applies the same delta to its
// JobSet's current/progressDetails. This is the single generic entry point
// a JobProcessor calls with just (jobID, delta) - the core never needs to
// know the job type, only what the Job's own UpdateTargets declare.
func (b *RedisBackend) UpdateJob(jobID string, currentDelta int) error {
	ctx := context.Background()
	key := keyJob + jobID

	job, err := b.getJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if err := b.jsonNumIncrBy(ctx, key, "$.current", currentDelta); err != nil {
		return err
	}
	if err := b.jsonSet(ctx, key, "$.updatedAt", nowMillis()); err != nil {
		return err
	}

	if job.PartOf != "" && len(job.UpdateTargets) > 0 {
		return b.UpdateJobSet(job.PartOf, currentDelta, job.UpdateTargets)
	}
	return nil
}
