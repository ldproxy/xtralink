package drivers

import (
	"archive/zip"
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// buildZip creates an in-memory zip archive from name->content entries, a
// trailing "/" name creates an empty directory entry.
func buildZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	for name, content := range files {
		fw, err := w.Create(name)
		if err != nil {
			t.Fatalf("Create(%s): %v", name, err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatalf("Write(%s): %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zip.Writer.Close: %v", err)
	}
	return buf.Bytes()
}

func TestUnzipArchive_ExtractsNestedFilesFromBytes(t *testing.T) {
	data := buildZip(t, map[string]string{
		"a.txt":        "hello",
		"nested/b.txt": "world",
	})
	dest := t.TempDir()

	if err := unzipArchive(data, dest); err != nil {
		t.Fatalf("unzipArchive: %v", err)
	}

	assertFileContent(t, filepath.Join(dest, "a.txt"), "hello")
	assertFileContent(t, filepath.Join(dest, "nested", "b.txt"), "world")
}

func TestUnzipArchive_RejectsPathTraversal(t *testing.T) {
	data := buildZip(t, map[string]string{
		"../escape.txt": "malicious",
	})
	dest := t.TempDir()

	if err := unzipArchive(data, dest); err == nil {
		t.Fatal("expected an error for a zip entry escaping the destination")
	}
}

func TestUnzipArchive_InvalidZipBytesIsError(t *testing.T) {
	if err := unzipArchive([]byte("not a zip file"), t.TempDir()); err == nil {
		t.Fatal("expected an error for invalid zip data")
	}
}

func TestUnzipArchive_OverwritesExistingDestination(t *testing.T) {
	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(dest, "old.txt"), []byte("stale"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	data := buildZip(t, map[string]string{"new.txt": "fresh"})
	if err := unzipArchive(data, dest); err != nil {
		t.Fatalf("unzipArchive: %v", err)
	}

	assertFileContent(t, filepath.Join(dest, "new.txt"), "fresh")
	// unzipArchive itself doesn't clear the destination (callers that need
	// a clean slate, e.g. oci.go's Sync, os.RemoveAll it first) - assert
	// the pre-existing file simply isn't touched, not that it's gone.
	assertFileContent(t, filepath.Join(dest, "old.txt"), "stale")
}
