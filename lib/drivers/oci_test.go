package drivers

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/rs/zerolog"
	"oras.land/oras-go/v2/content/memory"
)

func TestPkgPush(t *testing.T) {
	ctx := context.Background()

	filename := "./"

	filename, err := filepath.Abs(filename)
	if err != nil {
		t.Fatalf("failed to get absolute path: %v", err)
	}

	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	ociDriver := NewOCIDriver(logger)
	memoryStore := memory.New()

	_, err = ociDriver.pushImage(ctx, filename, memoryStore, "latest", "amd64")
	if err != nil {
		t.Fatalf("failed to create oci image: %v", err)
	}

	// The tag must resolve to a manifest descriptor.
	manifestDesc, err := memoryStore.Resolve(ctx, "latest")
	if err != nil {
		t.Fatalf("failed to resolve tag %q: %v", "latest", err)
	}
	if manifestDesc.MediaType != ocispec.MediaTypeImageManifest {
		t.Fatalf("unexpected manifest media type: got %q, want %q", manifestDesc.MediaType, ocispec.MediaTypeImageManifest)
	}

	if exists, err := memoryStore.Exists(ctx, manifestDesc); err != nil {
		t.Fatalf("failed to check manifest existence: %v", err)
	} else if !exists {
		t.Fatalf("manifest blob %s not found in store", manifestDesc.Digest)
	}

	// Read and decode the manifest.
	manifestRC, err := memoryStore.Fetch(ctx, manifestDesc)
	if err != nil {
		t.Fatalf("failed to fetch manifest: %v", err)
	}
	manifestBytes, err := io.ReadAll(manifestRC)
	manifestRC.Close()
	if err != nil {
		t.Fatalf("failed to read manifest: %v", err)
	}

	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("failed to decode manifest: %v", err)
	}

	if manifest.ArtifactType != xtrapkgMediaType {
		t.Fatalf("unexpected artifact type: got %q, want %q", manifest.ArtifactType, xtrapkgMediaType)
	}

	// The config blob must exist and describe the expected platform.
	if manifest.Config.MediaType != ocispec.MediaTypeImageConfig {
		t.Fatalf("unexpected config media type: got %q, want %q", manifest.Config.MediaType, ocispec.MediaTypeImageConfig)
	}
	if exists, err := memoryStore.Exists(ctx, manifest.Config); err != nil {
		t.Fatalf("failed to check config existence: %v", err)
	} else if !exists {
		t.Fatalf("config blob %s not found in store", manifest.Config.Digest)
	}

	configRC, err := memoryStore.Fetch(ctx, manifest.Config)
	if err != nil {
		t.Fatalf("failed to fetch config: %v", err)
	}
	configBytes, err := io.ReadAll(configRC)
	configRC.Close()
	if err != nil {
		t.Fatalf("failed to read config: %v", err)
	}

	var config ocispec.Image
	if err := json.Unmarshal(configBytes, &config); err != nil {
		t.Fatalf("failed to decode config: %v", err)
	}
	if config.OS != "linux" {
		t.Fatalf("unexpected config OS: got %q, want %q", config.OS, "linux")
	}
	if config.Architecture != "amd64" {
		t.Fatalf("unexpected config architecture: got %q, want %q", config.Architecture, "amd64")
	}

	// The manifest must reference exactly one layer, and that layer blob must exist.
	if len(manifest.Layers) != 1 {
		t.Fatalf("unexpected number of layers: got %d, want %d", len(manifest.Layers), 1)
	}
	layer := manifest.Layers[0]
	if exists, err := memoryStore.Exists(ctx, layer); err != nil {
		t.Fatalf("failed to check layer existence: %v", err)
	} else if !exists {
		t.Fatalf("layer blob %s not found in store", layer.Digest)
	}
	if layer.Size <= 0 {
		t.Fatalf("unexpected layer size: got %d, want > 0", layer.Size)
	}
}
