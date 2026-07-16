package app

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/ldproxy/xtralink/lib/cache"
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
	Cache   cache.Cache
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

	appCache := newCache(settings)
	c := AppContext{
		Logger:   logger,
		Version:  version,
		Dev:      isDev,
		Settings: settings,
		Drivers:  drivers.NewFactoryWithLoggerAndCache(logger, appCache),
		Jobs:     newJobsBackend(settings),
		Locks:    newLocker(settings),
		Cache:    appCache,
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
		return jobs.NewRedisBackend(settings.JobQueue.Redis)
	}
	return jobs.NewMemoryBackend()
}

// newLocker only returns a real distributed lock if Redis is actually
// configured - without it, there is by definition only one process to
// coordinate, so a NoopLocker is both correct and avoids depending on a
// Redis that was never set up.
func newLocker(settings *Settings) lock.Locker {
	if settings != nil && len(settings.JobQueue.Redis) > 0 {
		return lock.NewRedisLocker(settings.JobQueue.Redis)
	}
	return lock.NoopLocker{}
}

// newCache only returns a real Redis-backed cache if Redis is actually
// configured - a shared cache without shared storage behind it isn't
// meaningful, so a NoopCache (always miss) is both correct and avoids
// depending on a Redis that was never set up. Reuses the same
// Settings.JobQueue.Redis nodes as the jobs backend/lock (one shared Redis
// instance, differentiated only by key prefix, s. lib/cache's keyPrefix).
func newCache(settings *Settings) cache.Cache {
	if settings != nil && len(settings.JobQueue.Redis) > 0 {
		return cache.NewRedisCache(settings.JobQueue.Redis)
	}
	return cache.NoopCache{}
}
