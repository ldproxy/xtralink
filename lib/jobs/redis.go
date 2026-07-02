package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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

// Done removes jobID from the taken list and discards its document, mirroring
// JobQueueBackendRedis.doneJob/onJobFinished: the finished state is not
// persisted. Updating the parent JobSet's progress/finishedAt or triggering
// cleanup/followUps is JobRunner/JobSet.done() orchestration and out of
// scope for this iteration.
func (b *RedisBackend) Done(jobID string) error {
	ctx := context.Background()

	n, err := b.client.LRem(ctx, keyTaken, 1, jobID).Result()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	return b.jsonDel(ctx, keyJob+jobID)
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
	return b.client.RPush(ctx, keyFailed, jobID).Err()
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
// (initialized by the type-specific setup step) - not yet exercised by the
// CLI in this iteration since there is no setup/JobProcessor machinery.
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
// a future JobProcessor calls with just (jobID, delta) - the core never
// needs to know the job type, only what the Job's own UpdateTargets declare.
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
