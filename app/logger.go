package app

import (
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
)

func NewLoggerFromEnv() zerolog.Logger {
	env := strings.ToLower(strings.TrimSpace(firstNonEmpty(os.Getenv("APP_ENV"), os.Getenv("ENV"))))
	isDev := env == "" || env == "dev" || env == "development" || env == "local"

	level := zerolog.InfoLevel
	if isDev {
		level = zerolog.DebugLevel
	}
	if raw := strings.TrimSpace(os.Getenv("LOG_LEVEL")); raw != "" {
		if parsed, err := zerolog.ParseLevel(strings.ToLower(raw)); err == nil {
			level = parsed
		}
	}

	var out io.Writer = os.Stdout
	if isDev {
		out = zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	}

	return zerolog.New(out).Level(level).With().Timestamp().Logger()
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
