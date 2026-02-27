package app

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

const (
	xtraPkgArtifactType = "application/vnd.iide.xtrapkg"
	xtraPkgRegistryBase = "docker.ci.interactive-instruments.de/xtrasync"
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

	if err := RunSync(&Settings{TargetDir: settings.TargetDir, Remotes: []Remote{*r}}, s.drivers, s.logger); err != nil {
		return err
	}

	zipBytes, err := zipDirectoryToBytes(r.ResolvedLocalPath)
	if err != nil {
		return err
	}

	if strings.TrimSpace(targetTag) == "" {
		targetTag = "latest"
	}
	repoRef := fmt.Sprintf("%s/%s", xtraPkgRegistryBase, strings.TrimSpace(imageName))

	user, password := resolveRemoteCredentials(*r)
	repo, err := pushRepository(repoRef, user, password)
	if err != nil {
		return err
	}

	if err := pushXtraPackage(context.Background(), repo, targetTag, zipBytes); err != nil {
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

func pushRepository(ref, user, password string) (*remote.Repository, error) {
	repo, err := remote.NewRepository(ref)
	if err != nil {
		return nil, fmt.Errorf("invalid oci repository %q: %w", ref, err)
	}

	if strings.TrimSpace(user) != "" || strings.TrimSpace(password) != "" {
		repo.Client = &auth.Client{
			Client: retry.DefaultClient,
			Cache:  auth.NewCache(),
			Credential: auth.StaticCredential(repo.Reference.Registry, auth.Credential{
				Username: strings.TrimSpace(user),
				Password: strings.TrimSpace(password),
			}),
		}
	}

	return repo, nil
}

func pushXtraPackage(ctx context.Context, repo *remote.Repository, reference string, zipPayload []byte) error {
	configBytes := []byte("{}")
	configDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    digest.FromBytes(configBytes),
		Size:      int64(len(configBytes)),
	}
	if err := repo.Push(ctx, configDesc, bytes.NewReader(configBytes)); err != nil {
		return fmt.Errorf("push config blob failed: %w", err)
	}

	layerDesc := ocispec.Descriptor{
		MediaType: "archive/zip",
		Digest:    digest.FromBytes(zipPayload),
		Size:      int64(len(zipPayload)),
	}
	if err := repo.Push(ctx, layerDesc, bytes.NewReader(zipPayload)); err != nil {
		return fmt.Errorf("push layer blob failed: %w", err)
	}

	manifest := ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: xtraPkgArtifactType,
		Config:       configDesc,
		Layers:       []ocispec.Descriptor{layerDesc},
	}
	manifestBytes, err := jsonMarshal(manifest)
	if err != nil {
		return err
	}

	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestBytes),
		Size:      int64(len(manifestBytes)),
	}

	if err := repo.PushReference(ctx, manifestDesc, bytes.NewReader(manifestBytes), reference); err != nil {
		return fmt.Errorf("push manifest failed: %w", err)
	}

	return nil
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

func jsonMarshal(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("could not encode oci manifest: %w", err)
	}
	return b, nil
}
