package app

import (
	"github.com/rs/zerolog"

	"xtra-sync/lib/drivers"
)

type Service struct {
	drivers *drivers.Factory
	logger  zerolog.Logger
}

func NewService() *Service {
	logger := NewLoggerFromEnv().With().Str("component", "service").Logger()

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
