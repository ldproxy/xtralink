package pkg

import (
	"fmt"
	"strings"

	"github.com/ldproxy/xtralink/app"
	"github.com/ldproxy/xtralink/lib/drivers"
)

const (
	xtraPkgArtifactType = "application/vnd.iide.xtrapkg"
)

func Push(appCtx *app.AppContext, sourceRemoteId, targetRemoteId, targetTag string) error {

	source, err := findRemoteByID(appCtx.Settings, sourceRemoteId)
	if err != nil {
		return err
	}

	if err := Pull(appCtx, source.Id); err != nil {
		return err
	}

	target, err := findRemoteByID(appCtx.Settings, targetRemoteId)
	if err != nil {
		return err
	}

	if strings.TrimSpace(targetTag) == "" {
		targetTag = "latest"
	}

	pusher, err := appCtx.Drivers.PusherFor(target.Type)
	if err != nil {
		return err
	}

	user, password := resolveRemoteCredentials(*target)

	if err := pusher.Push(drivers.PushRequest{
		Source: drivers.Remote{
			ResolvedLocalPath: source.ResolvedLocalPath,
		},
		Target: drivers.Remote{
			URL:      target.URL,
			User:     user,
			Password: password,
		},
		TargetTag: targetTag,
	}); err != nil {
		return err
	}

	appCtx.Logger.Info().
		Str("source_id", source.Id).
		Str("source_path", source.ResolvedLocalPath).
		Str("target_repository", target.URL).
		Str("target_tag", targetTag).
		Msg("pushed xtrapkg artifact")

	return nil
}

func findRemoteByID(settings *app.Settings, remoteID string) (*app.Package, error) {
	if settings == nil {
		return nil, fmt.Errorf("settings is nil")
	}
	id := strings.TrimSpace(remoteID)
	if id == "" {
		return nil, fmt.Errorf("remote id is empty")
	}

	for i := range settings.Packages {
		if strings.TrimSpace(settings.Packages[i].Id) == id {
			return &settings.Packages[i], nil
		}
	}

	return nil, fmt.Errorf("remote with id %q not found", id)
}

func resolveRemoteCredentials(r app.Package) (string, string) {
	return strings.TrimSpace(r.User), strings.TrimSpace(r.Password)
}
