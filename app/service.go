package app

import "xtra-sync/lib/drivers"

type Service struct {
	drivers *drivers.Factory
}

func NewService() *Service {
	return &Service{
		drivers: drivers.NewFactory(),
	}
}

func (s *Service) Run(configPath string) error {
	settings, err := LoadSettings(configPath)
	if err != nil {
		return err
	}

	return RunSync(settings, s.drivers)
}
