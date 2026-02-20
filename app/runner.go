package app

import (
	"fmt"

	"xtra-sync/lib/drivers"
)

// RunSync processes all configured remotes.
// At the moment, only GIT remotes are actively executed.
func RunSync(settings *Settings, factory *drivers.Factory) error {
	if settings == nil {
		return fmt.Errorf("settings is nil")
	}
	if factory == nil {
		return fmt.Errorf("drivers factory is nil")
	}
	if len(settings.Remotes) == 0 {
		return fmt.Errorf("no remotes configured")
	}

	for i, r := range settings.Remotes {
		fmt.Printf("[xtra-sync] remote[%d]: type=%s url=%s path=%s target=%s\n", i, r.Type, r.URL, r.Path, r.ResolvedLocalPath)

		driver, err := factory.For(r.Type)
		if err != nil {
			return fmt.Errorf("remote[%d] driver resolution failed: %w", i, err)
		}

		remote := drivers.Remote{
			Type:              r.Type,
			URL:               r.URL,
			Tag:               r.Tag,
			User:              r.User,
			Password:          r.Password,
			Path:              r.Path,
			ResolvedLocalPath: r.ResolvedLocalPath,
		}

		if err := driver.Sync(remote); err != nil {
			return fmt.Errorf("remote[%d] fetch failed: %w", i, err)
		}
	}

	return nil
}
