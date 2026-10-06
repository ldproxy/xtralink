package jobs

import (
	"github.com/ldproxy/xtralink/model"
	"github.com/rs/zerolog"
)

// The log helpers below are shared by MemoryBackend and RedisBackend so both
// report the same setup/cleanup/sequence/followUps decisions with the same
// messages and fields, whichever queue is configured.

func logJobPushed(logger zerolog.Logger, job *model.Job) {
	logger.Debug().Str("job", job.Id).Str("kind", job.Kind).
		Bool("setup", job.Setup != nil).Bool("cleanup", job.Cleanup != nil).
		Bool("sequenced", job.Sequence != nil).Int("followUps", len(job.FollowUps)).
		Msg("job pushed")
}

func logSequenceGated(logger zerolog.Logger, job *model.Job, partialJob *model.PartialJob) {
	event := logger.Trace().Str("job", job.Id).Str("partialJob", partialJob.Id).Int("current", job.Sequence.Current)
	if partialJob.Sequence != nil {
		event = event.Int("sequence", *partialJob.Sequence)
	}
	event.Msg("partial job waiting for its sequence")
}

func logSequenceAdvanced(logger zerolog.Logger, job *model.Job) {
	logger.Debug().Str("job", job.Id).Int("current", job.Sequence.Current).Int("remaining", job.Sequence.Remaining).
		Msg("sequence advanced")
}

func logSequenceStopped(logger zerolog.Logger, job *model.Job, partialJob *model.PartialJob) {
	event := logger.Debug().Str("job", job.Id).Str("partialJob", partialJob.Id)
	if partialJob.Sequence != nil {
		event = event.Int("sequence", *partialJob.Sequence)
	}
	event.Msg("sequence stopped by failed partial job")
}

func logJobFinished(logger zerolog.Logger, job *model.Job, msg string) {
	logger.Debug().Str("job", job.Id).Str("status", string(job.GetStatus())).
		Strs("errors", job.Errors).Bool("cleanup", job.Cleanup != nil).
		Msg(msg)
}

func logFollowUpsSkipped(logger zerolog.Logger, job *model.Job, reason string) {
	if len(job.FollowUps) == 0 {
		return
	}
	logger.Debug().Str("job", job.Id).Int("followUps", len(job.FollowUps)).Str("reason", reason).
		Msg("follow-ups skipped")
}

func logFollowUpsPushed(logger zerolog.Logger, job *model.Job) {
	if len(job.FollowUps) == 0 {
		return
	}
	logger.Debug().Str("job", job.Id).Int("followUps", len(job.FollowUps)).Msg("pushing follow-ups")
}

func logPartialJobRetry(logger zerolog.Logger, partialJob *model.PartialJob, message string) {
	logger.Debug().Str("partialJob", partialJob.Id).Str("job", partialJob.PartOf).
		Int("attempt", len(partialJob.Errors)).Int("maxRetries", maxRetries).Str("error", message).
		Msg("partial job failed, retrying")
}

func logPartialJobFailed(logger zerolog.Logger, partialJob *model.PartialJob, message string) {
	logger.Debug().Str("partialJob", partialJob.Id).Str("job", partialJob.PartOf).Str("error", message).
		Msg("partial job permanently failed")
}
