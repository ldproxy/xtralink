package app

import "fmt"

type GitClient interface {
	Fetch(url, tag, subPath, destination, user, password string) error
}

// RunSync processes all configured remotes.
// At the moment, only GIT remotes are actively executed.
func RunSync(settings *Settings, gitClient GitClient) error {
	if settings == nil {
		return fmt.Errorf("settings is nil")
	}
	if gitClient == nil {
		return fmt.Errorf("git client is nil")
	}
	if len(settings.Remotes) == 0 {
		return fmt.Errorf("no remotes configured")
	}

	for i, r := range settings.Remotes {
		fmt.Printf("[xtra-sync] remote[%d]: type=%s url=%s path=%s target=%s\n", i, r.Type, r.URL, r.Path, r.ResolvedLocalPath)

		if r.Type != "GIT" {
			fmt.Printf("[xtra-sync] remote[%d] type=%s not implemented yet, skipping\n", i, r.Type)
			continue
		}

		if err := gitClient.Fetch(r.URL, r.Tag, r.Path, r.ResolvedLocalPath, r.User, r.Password); err != nil {
			return fmt.Errorf("remote[%d] fetch failed: %w", i, err)
		}
	}

	return nil
}
