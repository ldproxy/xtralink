package drivers

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

func TestFSDriver_SyncMirrorsSourceIntoTarget(t *testing.T) {
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), "a")
	writeFile(t, filepath.Join(src, "nested", "b.txt"), "b")

	target := t.TempDir()
	driver := NewFSDriver(zerolog.Nop())

	if err := driver.Sync(Remote{URL: src, ResolvedLocalPath: target}); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	assertFileContent(t, filepath.Join(target, "a.txt"), "a")
	assertFileContent(t, filepath.Join(target, "nested", "b.txt"), "b")
}

func TestFSDriver_SyncBackMirrorsTargetIntoSource(t *testing.T) {
	remoteDir := t.TempDir() // the "remote" - just another local directory
	local := t.TempDir()
	writeFile(t, filepath.Join(local, "a.txt"), "a")
	writeFile(t, filepath.Join(local, "nested", "b.txt"), "b")

	driver := NewFSDriver(zerolog.Nop())
	remote := Remote{URL: remoteDir, ResolvedLocalPath: local}

	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack: %v", err)
	}

	assertFileContent(t, filepath.Join(remoteDir, "a.txt"), "a")
	assertFileContent(t, filepath.Join(remoteDir, "nested", "b.txt"), "b")
}

func TestFSDriver_SyncBackReflectsLocalDeletion(t *testing.T) {
	remoteDir := t.TempDir()
	local := t.TempDir()
	writeFile(t, filepath.Join(local, "a.txt"), "a")
	writeFile(t, filepath.Join(local, "b.txt"), "b")

	driver := NewFSDriver(zerolog.Nop())
	remote := Remote{URL: remoteDir, ResolvedLocalPath: local}

	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack (initial): %v", err)
	}

	// This is exactly what pkg:mv_file relies on: removing a file locally
	// and syncing back again must make it disappear at the remote too.
	if err := os.Remove(filepath.Join(local, "a.txt")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack (after deletion): %v", err)
	}

	if _, err := os.Stat(filepath.Join(remoteDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("expected a.txt to have been deleted from the remote, stat err = %v", err)
	}
	assertFileContent(t, filepath.Join(remoteDir, "b.txt"), "b")
}

func TestFSDriver_RoundTripPullThenPush(t *testing.T) {
	// Simulates pkg:mv_file's actual usage: pull a package's remote into
	// its local mirror, modify the mirror, push the change back.
	remoteDir := t.TempDir()
	writeFile(t, filepath.Join(remoteDir, "existing.txt"), "existing")

	local := t.TempDir()
	driver := NewFSDriver(zerolog.Nop())
	remote := Remote{URL: remoteDir, ResolvedLocalPath: local}

	if err := driver.Sync(remote); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	assertFileContent(t, filepath.Join(local, "existing.txt"), "existing")

	// A file "arrives" in the local mirror (e.g. moved there by pkg:mv_file
	// from another package) ...
	writeFile(t, filepath.Join(local, "moved.txt"), "moved")

	// ... and SyncBack must publish it to the remote.
	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack: %v", err)
	}
	assertFileContent(t, filepath.Join(remoteDir, "moved.txt"), "moved")
	assertFileContent(t, filepath.Join(remoteDir, "existing.txt"), "existing")
}

// With a manifest, SyncBack carries the changes made to the local copy
// since the pull - and nothing else: a file that reached the remote in the
// meantime (e.g. a new upload to an inbox) survives the push.
func TestFSDriver_SyncBackWithManifestCarriesOnlyLocalChanges(t *testing.T) {
	remoteDir := t.TempDir()
	writeFile(t, filepath.Join(remoteDir, "a.txt"), "a")
	writeFile(t, filepath.Join(remoteDir, "b.txt"), "b")

	driver := NewFSDriver(zerolog.Nop())
	remote := Remote{URL: remoteDir, ResolvedLocalPath: t.TempDir(), ManifestPath: filepath.Join(t.TempDir(), "foo.json")}
	if err := driver.Sync(remote); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := RecordManifest(remote); err != nil {
		t.Fatalf("RecordManifest: %v", err)
	}

	writeFile(t, filepath.Join(remoteDir, "late.txt"), "late")    // arrives at the remote after the pull
	writeFile(t, filepath.Join(remoteDir, "b.txt"), "remote edit") // changed at the remote, unchanged locally
	if err := os.Remove(filepath.Join(remote.ResolvedLocalPath, "a.txt")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	writeFile(t, filepath.Join(remote.ResolvedLocalPath, "nested", "new.txt"), "new")

	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack: %v", err)
	}

	if _, err := os.Stat(filepath.Join(remoteDir, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("expected a.txt to have been deleted from the remote, stat err = %v", err)
	}
	assertFileContent(t, filepath.Join(remoteDir, "nested", "new.txt"), "new")
	assertFileContent(t, filepath.Join(remoteDir, "late.txt"), "late")
	assertFileContent(t, filepath.Join(remoteDir, "b.txt"), "remote edit")

	// the push is recorded: deleting new.txt locally now removes it remotely, too
	if err := os.Remove(filepath.Join(remote.ResolvedLocalPath, "nested", "new.txt")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack (second): %v", err)
	}
	if _, err := os.Stat(filepath.Join(remoteDir, "nested", "new.txt")); !os.IsNotExist(err) {
		t.Errorf("expected new.txt to have been deleted from the remote, stat err = %v", err)
	}
	assertFileContent(t, filepath.Join(remoteDir, "late.txt"), "late")
}

// A local copy that was never pulled says nothing about what the remote
// holds: pushing it adds its files and deletes nothing.
func TestFSDriver_SyncBackWithoutPullOnlyAdds(t *testing.T) {
	remoteDir := t.TempDir()
	writeFile(t, filepath.Join(remoteDir, "keep-me.txt"), "precious")
	local := t.TempDir()
	writeFile(t, filepath.Join(local, "2026", "a.txt"), "a")

	driver := NewFSDriver(zerolog.Nop())
	remote := Remote{URL: remoteDir, ResolvedLocalPath: local, ManifestPath: filepath.Join(t.TempDir(), "foo.json")}
	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack: %v", err)
	}

	assertFileContent(t, filepath.Join(remoteDir, "2026", "a.txt"), "a")
	assertFileContent(t, filepath.Join(remoteDir, "keep-me.txt"), "precious")
}
