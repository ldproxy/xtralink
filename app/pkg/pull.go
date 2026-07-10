package pkg

import (
	"fmt"

	"github.com/ldproxy/xtrasync/app"
	"github.com/ldproxy/xtrasync/lib/drivers"
)

// Pull processes all configured remotes.
func Pull(appCtx *app.AppContext, pkgId string) error {
	if appCtx.Settings == nil {
		return fmt.Errorf("settings is nil")
	}
	if appCtx.Drivers == nil {
		return fmt.Errorf("drivers factory is nil")
	}
	if len(appCtx.Settings.Packages) == 0 {
		return fmt.Errorf("no remotes configured")
	}
	if pkgId != "" && !appCtx.Settings.HasPackage(pkgId) {
		return fmt.Errorf("remote with id '%s' not found", pkgId)
	}

	for i, r := range appCtx.Settings.Packages {
		if pkgId != "" && r.Id != pkgId {
			continue
		}

		appCtx.Logger.Info().
			Int("remote_index", i).
			Str("type", r.Type).
			Str("url", r.URL).
			Str("path", r.Path).
			Str("target", r.ResolvedLocalPath).
			Msg("processing remote")

		driver, err := appCtx.Drivers.For(r.Type)
		if err != nil {
			return fmt.Errorf("remote[%d] driver resolution failed: %w", i, err)
		}

		if err := driver.Sync(RemoteFor(r)); err != nil {
			return fmt.Errorf("remote[%d] fetch failed: %w", i, err)
		}
	}

	return nil
}

// RemoteFor builds the lib/drivers.Remote for a configured Package - shared
// by Pull and by pkg:mv_file's implicit SyncBack.
func RemoteFor(p app.Package) drivers.Remote {
	return drivers.Remote{
		Type:              p.Type,
		ID:                p.Id,
		URL:               p.URL,
		Tag:               p.Tag,
		User:              p.User,
		Password:          p.Password,
		Path:              p.Path,
		ResolvedLocalPath: p.ResolvedLocalPath,
	}
}
