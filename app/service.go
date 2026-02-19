package app

import "xtra-sync/lib/git"

type Service struct {
	gitClient GitClient
}

func NewService() *Service {
	return &Service{
		gitClient: git.NewAdapter(),
	}
}

func (s *Service) Run(configPath string) error {
	settings, err := LoadSettings(configPath)
	if err != nil {
		return err
	}

	return RunSync(settings, s.gitClient)
}
