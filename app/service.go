package app

import (
	"github.com/rs/zerolog"

	"github.com/ldproxy/xtrasync/lib/drivers"
)

type Service struct {
	drivers *drivers.Factory
	logger  zerolog.Logger
}

func NewService() *Service {
	dotEnvErr := loadDotEnvIfPresent()
	logger := NewLoggerFromEnv().With().Str("component", "service").Logger()
	if dotEnvErr != nil {
		logger.Warn().Err(dotEnvErr).Msg("could not load .env")
	}

	return &Service{
		drivers: drivers.NewFactoryWithLogger(logger),
		logger:  logger,
	}
}

func (s *Service) Run(configPath string) error {
	settings, err := LoadSettings(configPath)
	if err != nil {
		return err
	}

	return RunSync(settings, s.drivers, s.logger)
}

func (s *Service) Logger() zerolog.Logger {
	return s.logger
}
