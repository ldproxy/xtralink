package app

import (
	"fmt"

	"github.com/rs/zerolog"

	"xtra-sync/lib/drivers"
)

// RunSync processes all configured remotes.
func RunSync(settings *Settings, factory *drivers.Factory, logger zerolog.Logger) error {
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
		logger.Info().
			Int("remote_index", i).
			Str("type", r.Type).
			Str("url", r.URL).
			Str("path", r.Path).
			Str("target", r.ResolvedLocalPath).
			Msg("processing remote")

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
