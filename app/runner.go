package app

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/ldproxy/xtrasync/lib/drivers"
)

// RunSync processes all configured remotes.
func RunSync(settings *Settings, factory *drivers.Factory, logger zerolog.Logger, pkgId string) error {
	if settings == nil {
		return fmt.Errorf("settings is nil")
	}
	if factory == nil {
		return fmt.Errorf("drivers factory is nil")
	}
	if len(settings.Packages) == 0 {
		return fmt.Errorf("no remotes configured")
	}
	if pkgId != "" && !settings.HasPackage(pkgId) {
		return fmt.Errorf("remote with id '%s' not found", pkgId)
	}

	for i, r := range settings.Packages {
		if pkgId != "" && r.Id != pkgId {
			continue
		}

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
			ID:                r.Id,
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
