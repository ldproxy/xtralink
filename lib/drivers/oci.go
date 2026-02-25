package drivers

import (
	"archive/zip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/rs/zerolog"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

type ociDriver struct{ logger zerolog.Logger }

func NewOCIDriver(logger zerolog.Logger) SyncDriver {
	return &ociDriver{logger: logger}
}

func (d *ociDriver) Sync(remote Remote) error {
	repoRef, reference, err := parseOCIRepositoryAndReference(remote.URL, remote.Tag)
	if err != nil {
		return err
	}
	user := strings.TrimSpace(remote.User)
	password := strings.TrimSpace(remote.Password)
	remoteID := strings.TrimSpace(remote.ID)
	if user == "" {
		user = firstEnvWithRemoteID(remoteID, "user")
	}
	if password == "" {
		password = firstEnvWithRemoteID(remoteID, "password")
	}

	repo, err := remoteRepository(repoRef, user, password)
	if err != nil {
		return err
	}

	ctx := context.Background()
	manifestDesc, err := repo.Resolve(ctx, reference)
	if err != nil {
		return fmt.Errorf("could not resolve oci reference %s:%s: %w", repoRef, reference, err)
	}

	rc, err := repo.Fetch(ctx, manifestDesc)
	if err != nil {
		return fmt.Errorf("could not fetch oci manifest %s@%s: %w", repoRef, manifestDesc.Digest, err)
	}
	manifestRaw, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("could not read oci manifest bytes: %w", err)
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		return fmt.Errorf("could not decode oci manifest: %w", err)
	}

	if err := validateArtifactType(manifest.ArtifactType); err != nil {
		return err
	}
	if len(manifest.Layers) == 0 {
		return fmt.Errorf("oci manifest has no layers")
	}
	// Current implementation supports only artifacts where the first layer
	// contains the payload ZIP that should be extracted and synced.
	firstLayer := manifest.Layers[0]
	if firstLayer.MediaType != "archive/zip" {
		return fmt.Errorf("first layer mediaType must be archive/zip, got: %s", firstLayer.MediaType)
	}

	cacheRoot := filepath.Join(os.TempDir(), "xtra-sync-cache", "oci", hashStringOCI(repoRef+"|"+reference))
	layerDigestState := filepath.Join(cacheRoot, ".layer-digest")
	manifestState := filepath.Join(cacheRoot, "manifest.json")
	cacheZip := filepath.Join(cacheRoot, "layer.zip")
	cacheExtracted := filepath.Join(cacheRoot, "data")

	cacheFresh := ociCacheHasLayerDigest(layerDigestState, firstLayer.Digest.String())
	if _, err := os.Stat(cacheExtracted); os.IsNotExist(err) {
		cacheFresh = false
	}

	if !cacheFresh {
		if err := os.MkdirAll(cacheRoot, 0o755); err != nil {
			return fmt.Errorf("could not create oci cache root (%s): %w", cacheRoot, err)
		}

		layerRC, err := repo.Fetch(ctx, firstLayer)
		if err != nil {
			return fmt.Errorf("could not fetch oci layer %s: %w", firstLayer.Digest, err)
		}
		if err := writeReaderToFile(layerRC, cacheZip); err != nil {
			layerRC.Close()
			return fmt.Errorf("could not write oci layer zip (%s): %w", cacheZip, err)
		}
		layerRC.Close()

		if err := os.RemoveAll(cacheExtracted); err != nil {
			return fmt.Errorf("could not clear oci cache dir (%s): %w", cacheExtracted, err)
		}
		if err := unzipArchive(cacheZip, cacheExtracted); err != nil {
			return fmt.Errorf("could not extract oci layer zip: %w", err)
		}

		if err := os.WriteFile(layerDigestState, []byte(firstLayer.Digest.String()), 0o644); err != nil {
			return fmt.Errorf("could not write oci cache layer digest state (%s): %w", layerDigestState, err)
		}
		if err := os.WriteFile(manifestState, manifestRaw, 0o644); err != nil {
			return fmt.Errorf("could not write oci manifest cache (%s): %w", manifestState, err)
		}
		d.logger.Info().
			Str("repository", repoRef).
			Str("reference", reference).
			Str("cache_root", cacheRoot).
			Msg("refreshed oci cache")
	} else {
		d.logger.Debug().
			Str("repository", repoRef).
			Str("reference", reference).
			Msg("oci cache unchanged")
	}

	sourcePath, err := resolveOCISubpath(cacheExtracted, remote.Path)
	if err != nil {
		return err
	}

	if err := syncPathMirror(sourcePath, remote.ResolvedLocalPath); err != nil {
		return fmt.Errorf("could not mirror oci cache to target (%s -> %s): %w", sourcePath, remote.ResolvedLocalPath, err)
	}

	d.logger.Info().
		Str("repository", repoRef).
		Str("reference", reference).
		Str("path", strings.TrimSpace(remote.Path)).
		Str("target", remote.ResolvedLocalPath).
		Msg("synced oci artifact")
	return nil
}

func remoteRepository(ref, user, password string) (*remote.Repository, error) {
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

func parseOCIRepositoryAndReference(raw, tagOverride string) (string, string, error) {
	input := strings.TrimSpace(raw)
	if input == "" {
		return "", "", fmt.Errorf("oci url is empty")
	}

	input = strings.TrimPrefix(input, "oci://")
	input = strings.TrimPrefix(input, "OCI://")
	if strings.HasPrefix(strings.ToLower(input), "http://") || strings.HasPrefix(strings.ToLower(input), "https://") {
		u := strings.TrimPrefix(strings.TrimPrefix(input, "https://"), "http://")
		input = u
	}

	ref, err := registry.ParseReference(input)
	if err != nil {
		return "", "", fmt.Errorf("invalid oci reference %q: %w", raw, err)
	}
	if err := ref.ValidateRepository(); err != nil {
		return "", "", fmt.Errorf("invalid oci repository in %q: %w", raw, err)
	}

	repository := ref.Registry + "/" + ref.Repository
	reference := strings.TrimSpace(tagOverride)
	if reference == "" {
		reference = strings.TrimSpace(ref.Reference)
	}
	if reference == "" {
		reference = "latest"
	}

	return repository, reference, nil
}

func validateArtifactType(artifactType string) error {
	const required = "application/vnd.opentofu.modulepkg"
	if strings.TrimSpace(artifactType) != required {
		return fmt.Errorf("artifactType must be %q, got %q", required, artifactType)
	}
	return nil
}

func resolveOCISubpath(root, remotePath string) (string, error) {
	cleanPath := filepath.Clean(strings.TrimSpace(remotePath))
	if cleanPath == "" || cleanPath == "." {
		return root, nil
	}
	if filepath.IsAbs(cleanPath) {
		return "", fmt.Errorf("remote path must be relative: %s", remotePath)
	}

	joined := filepath.Join(root, cleanPath)
	rel, err := filepath.Rel(root, joined)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("remote path escapes archive root: %s", remotePath)
	}

	info, err := os.Stat(joined)
	if err != nil {
		return "", fmt.Errorf("source path not found (%s): %w", joined, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("remote.path must point to a directory, but got file: %s", remotePath)
	}

	return joined, nil
}

func ociCacheHasLayerDigest(stateFile, digest string) bool {
	raw, err := os.ReadFile(stateFile)
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(raw)) == strings.TrimSpace(digest)
}

func hashStringOCI(v string) string {
	s := sha1.Sum([]byte(v))
	return hex.EncodeToString(s[:])
}

func unzipArchive(zipPath, destination string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	if err := os.MkdirAll(destination, 0o755); err != nil {
		return err
	}

	for _, f := range r.File {
		if strings.TrimSpace(f.Name) == "" {
			continue
		}

		target := filepath.Join(destination, f.Name)
		rel, err := filepath.Rel(destination, target)
		if err != nil {
			return err
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("zip entry escapes destination: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}
		if err := writeReaderToFile(rc, target); err != nil {
			rc.Close()
			return err
		}
		rc.Close()
	}

	return nil
}
