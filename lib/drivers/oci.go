package drivers

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/rs/zerolog"
	"oras.land/oras-go/v2"
	orasfile "oras.land/oras-go/v2/content/file"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/retry"
)

const xtrapkgMediaType = "application/vnd.iide.xtrapkg"

type ociDriver struct{ logger zerolog.Logger }

type imagePart struct {
	Descriptor ocispec.Descriptor
	Blob       []byte
}

func NewOCIDriver(logger zerolog.Logger) *ociDriver {
	return &ociDriver{logger: logger}
}

func (d *ociDriver) Sync(remote Remote) error {
	repoRef, reference, err := parseOCIRepositoryAndReference(remote.URL, remote.Tag)
	if err != nil {
		return err
	}
	user := strings.TrimSpace(remote.User)
	password := strings.TrimSpace(remote.Password)

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

	cacheRoot := filepath.Join(os.TempDir(), "xtralink-cache", "oci", hashStringOCI(repoRef+"|"+reference))
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

func (d *ociDriver) Push(push PushRequest) error {
	repository, err := normalizeOCIRepository(push.Target.URL)
	if err != nil {
		return err
	}
	if repository == "" {
		return fmt.Errorf("oci push repository is empty")
	}

	reference := strings.TrimSpace(push.TargetTag)
	if reference == "" {
		reference = "latest"
	}

	repo, err := remoteRepository(repository, push.Target.User, push.Target.Password)
	if err != nil {
		return err
	}

	ctx := context.Background()

	manifest1, err := d.pushImage(ctx, push.Source.ResolvedLocalPath, repo, "", "amd64")
	if err != nil {
		return fmt.Errorf("failed to create oci image: %w", err)
	}

	manifest2, err := d.pushImage(ctx, push.Source.ResolvedLocalPath, repo, "", "arm64")
	if err != nil {
		return fmt.Errorf("failed to create oci image: %w", err)
	}

	_, err = d.pushIndex(ctx, []ocispec.Descriptor{*manifest1, *manifest2}, xtrapkgMediaType, repo, reference)
	if err != nil {
		return fmt.Errorf("failed to create oci index: %w", err)
	}

	d.logger.Info().
		Str("repository", repository).
		Str("reference", reference).
		Msg("pushed oci artifact")

	return nil
}

func (d *ociDriver) pushImage(ctx context.Context, directoryPath string, repo oras.Target, tag string, arch string) (*ocispec.Descriptor, error) {
	store, err := orasfile.New("")
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %v", err)
	}
	defer func() { _ = store.Close() }()

	file, err := addFile(ctx, store, ".", ocispec.MediaTypeImageLayerGzip, directoryPath)
	if err != nil {
		return nil, fmt.Errorf("failed to add file: %v", err)
	}

	contentDigest := file.Annotations["io.deis.oras.content.digest"]
	d.logger.Debug().Str("contentDigest", contentDigest).Msg("created content digest")

	config, err := d.createConfig(contentDigest, arch)
	if err != nil {
		return nil, fmt.Errorf("failed to create config: %v", err)
	}

	if err := store.Push(ctx, config.Descriptor, bytes.NewReader(config.Blob)); err != nil {
		return nil, fmt.Errorf("push config blob failed: %v", err)
	}
	d.logger.Debug().Str("configDigest", config.Descriptor.Digest.String()).Msg("created config digest")

	manifest, err := d.createManifest(config.Descriptor, file, xtrapkgMediaType, arch)
	if err != nil {
		return nil, fmt.Errorf("failed to create manifest: %v", err)
	}

	if err := store.Push(ctx, manifest.Descriptor, bytes.NewReader(manifest.Blob)); err != nil {
		return nil, fmt.Errorf("push manifest blob failed: %v", err)
	}
	d.logger.Debug().Str("manifestDigest", manifest.Descriptor.Digest.String()).Msg("created manifest digest")

	if err = store.Tag(ctx, manifest.Descriptor, manifest.Descriptor.Digest.String()); err != nil {
		return nil, fmt.Errorf("tag manifest failed: %v", err)
	}

	copyOptions := oras.DefaultCopyOptions

	if _, err := oras.Copy(ctx, store, manifest.Descriptor.Digest.String(), repo, manifest.Descriptor.Digest.String(), copyOptions); err != nil {
		return nil, fmt.Errorf("oras copy failed: %v", err)
	}

	return &manifest.Descriptor, nil
}

func (d *ociDriver) pushIndex(ctx context.Context, manifestDesc []ocispec.Descriptor, artifactType string, repo oras.Target, tag string) (*ocispec.Descriptor, error) {
	store, err := orasfile.New("")
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %v", err)
	}
	defer func() { _ = store.Close() }()

	index, err := d.createIndex(manifestDesc, artifactType)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %v", err)
	}

	if err := store.Push(ctx, index.Descriptor, bytes.NewReader(index.Blob)); err != nil {
		return nil, fmt.Errorf("push index blob failed: %v", err)
	}
	d.logger.Debug().Str("indexDigest", index.Descriptor.Digest.String()).Msg("created index digest")

	if err = store.Tag(ctx, index.Descriptor, index.Descriptor.Digest.String()); err != nil {
		return nil, fmt.Errorf("tag index failed: %v", err)
	}

	copyOptions := oras.DefaultCopyOptions

	if _, err := oras.Copy(ctx, store, index.Descriptor.Digest.String(), repo, tag, copyOptions); err != nil {
		return nil, fmt.Errorf("oras copy failed: %v", err)
	}

	return &index.Descriptor, nil
}

func (d *ociDriver) createIndex(manifestDesc []ocispec.Descriptor, artifactType string) (*imagePart, error) {
	index := ocispec.Index{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageIndex,
		ArtifactType: artifactType,
		Manifests:    manifestDesc,
	}

	indexBytes, err := json.Marshal(index)
	if err != nil {
		return nil, fmt.Errorf("could not encode oci index: %v", err)
	}
	d.logger.Debug().Str("indexJson", string(indexBytes)).Msg("created index JSON")

	indexDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageIndex,
		Digest:    digest.FromBytes(indexBytes),
		Size:      int64(len(indexBytes)),
	}

	return &imagePart{
		Descriptor: indexDesc,
		Blob:       indexBytes,
	}, nil
}

func (d *ociDriver) createManifest(configDesc ocispec.Descriptor, layerDesc ocispec.Descriptor, artifactType string, arch string) (*imagePart, error) {
	manifest := ocispec.Manifest{
		Versioned:    specs.Versioned{SchemaVersion: 2},
		MediaType:    ocispec.MediaTypeImageManifest,
		ArtifactType: artifactType,
		Config:       configDesc,
		Layers:       []ocispec.Descriptor{layerDesc},
	}

	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("could not encode oci manifest: %v", err)
	}
	d.logger.Debug().Str("manifestJson", string(manifestBytes)).Msg("created manifest JSON")

	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestBytes),
		Size:      int64(len(manifestBytes)),
		Platform: &ocispec.Platform{
			OS:           "linux",
			Architecture: arch,
		},
	}

	return &imagePart{
		Descriptor: manifestDesc,
		Blob:       manifestBytes,
	}, nil
}

func (d *ociDriver) createConfig(contentDigest string, arch string) (*imagePart, error) {
	config := ocispec.Image{
		Platform: ocispec.Platform{
			OS:           "linux",
			Architecture: arch,
		},
		RootFS: ocispec.RootFS{
			Type:    "layers",
			DiffIDs: []digest.Digest{digest.Digest(contentDigest)},
		},
	}

	configJson, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("could not encode oci config: %v", err)
	}
	d.logger.Debug().Str("configJson", string(configJson)).Msg("created config JSON")

	configDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageConfig,
		Digest:    digest.FromBytes(configJson),
		Size:      int64(len(configJson)),
	}

	return &imagePart{
		Descriptor: configDesc,
		Blob:       configJson,
	}, nil
}

func addFile(ctx context.Context, store *orasfile.Store, name string, mediaType string, filename string) (ocispec.Descriptor, error) {
	file, err := store.Add(ctx, name, mediaType, filename)
	if err != nil {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			err = pathErr
		}
		return ocispec.Descriptor{}, fmt.Errorf("failed to add file: %v", err)
	}
	return file, nil
}

func normalizeOCIRepository(raw string) (string, error) {
	input := strings.TrimSpace(raw)
	if input == "" {
		return "", nil
	}

	input = strings.TrimPrefix(input, "oci://")
	input = strings.TrimPrefix(input, "OCI://")

	ref, err := registry.ParseReference(input)
	if err != nil {
		return "", fmt.Errorf("invalid oci repository %q: %w", raw, err)
	}
	if err := ref.ValidateRepository(); err != nil {
		return "", fmt.Errorf("invalid oci repository %q: %w", raw, err)
	}

	return ref.Registry + "/" + ref.Repository, nil
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
	const required = "application/vnd.iide.xtrapkg"
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
