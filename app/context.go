package app

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/ldproxy/xtralink/lib/drivers"
	"github.com/ldproxy/xtralink/lib/jobs"
	"github.com/ldproxy/xtralink/lib/lock"
	"github.com/rs/zerolog"
)

// AppContext will hold all dependencies for your application.
type AppContext struct {
	zerolog.Logger

	Version  string
	Dev      bool
	Settings *Settings

	Drivers *drivers.Factory
	Jobs    jobs.Backend
	Locks   lock.Locker
}

// NewAppContext returns an initialized context.
func NewAppContext(name string, version string, verbosity uint, settings *Settings) *AppContext {
	isDev := version == "DEV"

	logLevel := zerolog.InfoLevel
	if isDev || verbosity == 1 {
		logLevel = zerolog.DebugLevel
	}
	if verbosity == 2 {
		logLevel = zerolog.TraceLevel
	}
	if raw := strings.TrimSpace(os.Getenv("LOG_LEVEL")); raw != "" {
		if parsed, err := zerolog.ParseLevel(strings.ToLower(raw)); err == nil {
			logLevel = parsed
		}
	}

	var logger zerolog.Logger
	var out io.Writer = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}

	if isDev {
		logger = zerolog.New(out).Level(logLevel).With().Timestamp().Caller().Logger()
	} else {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
		logger = zerolog.New(os.Stdout).Level(logLevel).With().Timestamp().Logger()
	}

	c := AppContext{
		Logger:   logger,
		Version:  version,
		Dev:      isDev,
		Settings: settings,
		Drivers:  drivers.NewFactoryWithLogger(logger),
		Jobs:     newJobsBackend(settings),
		Locks:    newLocker(settings),
	}

	return &c
}

// newJobsBackend picks the job queue backend from Settings.JobQueue.Queue -
// "redis" connects to Settings.JobQueue.Redis (single node or cluster, s.
// jobs.NewRedisBackend), anything else (including a zero-value Settings
// that never went through LoadSettings, e.g. in tests) defaults to the
// in-memory backend, matching JobsConfiguration's own "LOCAL" default.
func newJobsBackend(settings *Settings) jobs.Backend {
	if settings != nil && strings.EqualFold(settings.JobQueue.Queue, "redis") {
		return jobs.NewRedisBackend(settings.JobQueue.Redis, settings.JobQueue.Cluster)
	}
	return jobs.NewMemoryBackend()
}

// newLocker only returns a real distributed lock if Redis is actually
// configured - without it, there is by definition only one process to
// coordinate, so a NoopLocker is both correct and avoids depending on a
// Redis that was never set up.
func newLocker(settings *Settings) lock.Locker {
	if settings != nil && len(settings.JobQueue.Redis) > 0 {
		return lock.NewRedisLocker(settings.JobQueue.Redis, settings.JobQueue.Cluster)
	}
	return lock.NoopLocker{}
}
