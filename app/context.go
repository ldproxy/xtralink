package app

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/ldproxy/xtrasync/lib/drivers"
	"github.com/ldproxy/xtrasync/lib/jobs"
	"github.com/ldproxy/xtrasync/lib/lock"
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

	redisAddr := strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	c := AppContext{
		Logger:   logger,
		Version:  version,
		Dev:      isDev,
		Settings: settings,
		Drivers:  drivers.NewFactoryWithLogger(logger),
		Jobs:     jobs.NewRedisBackend(redisAddr),
		Locks:    lock.NewRedisLocker(redisAddr),
	}

	return &c
}
