package drivers

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestChangesSinceManifest(t *testing.T) {
	local := t.TempDir()
	writeFile(t, filepath.Join(local, "same.txt"), "same")
	writeFile(t, filepath.Join(local, "edited.txt"), "before")
	writeFile(t, filepath.Join(local, "gone.txt"), "gone")
	remote := Remote{ResolvedLocalPath: local, ManifestPath: filepath.Join(t.TempDir(), "m.json")}
	if err := RecordManifest(remote); err != nil {
		t.Fatalf("RecordManifest: %v", err)
	}

	writeFile(t, filepath.Join(local, "edited.txt"), "after!")
	later := time.Now().Add(time.Minute)
	if err := os.Chtimes(filepath.Join(local, "edited.txt"), later, later); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
	if err := os.Remove(filepath.Join(local, "gone.txt")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	writeFile(t, filepath.Join(local, "sub", "added.txt"), "added")

	c, err := changesSinceManifest(remote)
	if err != nil {
		t.Fatalf("changesSinceManifest: %v", err)
	}
	if want := []string{"edited.txt", "sub/added.txt"}; !reflect.DeepEqual(c.upload, want) {
		t.Errorf("upload = %v, want %v", c.upload, want)
	}
	if want := []string{"gone.txt"}; !reflect.DeepEqual(c.remove, want) {
		t.Errorf("remove = %v, want %v", c.remove, want)
	}
}

func TestChangesSinceManifest_MissingLocalDirectoryIsError(t *testing.T) {
	remote := Remote{ResolvedLocalPath: filepath.Join(t.TempDir(), "missing"), ManifestPath: filepath.Join(t.TempDir(), "m.json")}
	if _, err := changesSinceManifest(remote); err == nil {
		t.Fatal("expected an error for a missing local directory")
	}
}
