package app

import (
	"github.com/mew-sh/dotenv"
	"github.com/rs/zerolog"

	"github.com/ldproxy/xtrasync/lib/drivers"
)

type Service struct {
	drivers *drivers.Factory
	logger  zerolog.Logger
}

func NewService() *Service {
	dotenv.Load()
	logger := NewLoggerFromEnv().With().Str("component", "service").Logger()

	return &Service{
		drivers: drivers.NewFactoryWithLogger(logger),
		logger:  logger,
	}
}

func (s *Service) Run(configPath, pkgId string) error {
	settings, err := LoadSettings(configPath)
	if err != nil {
		return err
	}

	return RunSync(settings, s.drivers, s.logger, pkgId)
}

func (s *Service) Logger() zerolog.Logger {
	return s.logger
}
