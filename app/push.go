package app

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ldproxy/xtrasync/lib/drivers"
)

const (
	xtraPkgArtifactType = "application/vnd.iide.xtrapkg"
)

func (s *Service) RunPush(configPath, remoteID, imageName, targetTag string) error {
	if strings.TrimSpace(imageName) == "" {
		return fmt.Errorf("image name is empty")
	}

	settings, err := LoadSettings(configPath)
	if err != nil {
		return err
	}

	r, err := findRemoteByID(settings, remoteID)
	if err != nil {
		return err
	}

	if err := RunSync(settings, s.drivers, s.logger, r.Id); err != nil {
		return err
	}

	zipBytes, err := zipDirectoryToBytes(r.ResolvedLocalPath)
	if err != nil {
		return err
	}

	if strings.TrimSpace(targetTag) == "" {
		targetTag = "latest"
	}
	repoRef := strings.TrimSpace(imageName)

	user, password := resolveRemoteCredentials(*r)
	pusher, err := s.drivers.PusherFor("OCI")
	if err != nil {
		return err
	}

	if err := pusher.Push(drivers.PushRequest{
		Repository:   repoRef,
		Reference:    targetTag,
		User:         user,
		Password:     password,
		Payload:      zipBytes,
		PayloadMedia: "archive/zip",
		ArtifactType: xtraPkgArtifactType,
	}); err != nil {
		return err
	}

	s.logger.Info().
		Str("source_id", r.Id).
		Str("source_path", r.ResolvedLocalPath).
		Str("target_repository", repoRef).
		Str("target_tag", targetTag).
		Msg("pushed xtrapkg artifact")

	return nil
}

func findRemoteByID(settings *Settings, remoteID string) (*Remote, error) {
	if settings == nil {
		return nil, fmt.Errorf("settings is nil")
	}
	id := strings.TrimSpace(remoteID)
	if id == "" {
		return nil, fmt.Errorf("remote id is empty")
	}

	for i := range settings.Remotes {
		if strings.TrimSpace(settings.Remotes[i].Id) == id {
			return &settings.Remotes[i], nil
		}
	}

	return nil, fmt.Errorf("remote with id %q not found", id)
}

func resolveRemoteCredentials(r Remote) (string, string) {
	return strings.TrimSpace(r.User), strings.TrimSpace(r.Password)
}

func zipDirectoryToBytes(sourceDir string) ([]byte, error) {
	root := strings.TrimSpace(sourceDir)
	if root == "" {
		return nil, fmt.Errorf("source directory is empty")
	}

	info, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("source directory not found (%s): %w", root, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("push source must be a directory: %s", root)
	}

	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)

	err = filepath.Walk(root, func(path string, fi os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if fi.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "" || rel == "." {
			return nil
		}

		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()

		w, err := zw.Create(rel)
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, in); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		zw.Close()
		return nil, fmt.Errorf("could not build zip payload from %s: %w", root, err)
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("could not finalize zip payload: %w", err)
	}

	return buf.Bytes(), nil
}
