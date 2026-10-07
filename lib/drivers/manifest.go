package drivers

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// Manifest records the files of a local mirror as they were when it was
// last pulled or pushed, keyed by their slash-form path relative to the
// mirror. SyncBack compares the mirror against it to find what the
// workflow changed: a file that is new or differs from its entry is
// uploaded, and only a file that has an entry but is gone from the mirror
// is deleted at the remote.
//
// Comparing against the remote itself instead would delete everything that
// reached the remote after the pull - a file uploaded to an inbox while a
// job worked on its copy - and re-upload every unchanged file. With a
// manifest, a push carries exactly the changes made to the local copy.
type Manifest map[string]FileState

// FileState is what tells a changed file from an unchanged one without
// reading it.
type FileState struct {
	Size    int64 `json:"size"`
	ModTime int64 `json:"modTime"` // Unix nanoseconds
}

// ScanManifest records the files currently in dir.
func ScanManifest(dir string) (Manifest, error) {
	m := Manifest{}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		m[filepath.ToSlash(rel)] = FileState{Size: info.Size(), ModTime: info.ModTime().UnixNano()}
		return nil
	})
	return m, err
}

// ReadManifest reads the manifest at path. A missing file is no error: it
// returns a nil Manifest, i.e. nothing is known about the remote's content.
func ReadManifest(path string) (Manifest, error) {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("could not read manifest %s: %w", path, err)
	}
	m := Manifest{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("could not parse manifest %s: %w", path, err)
	}
	return m, nil
}

// WriteManifest replaces the manifest at path.
func WriteManifest(path string, m Manifest) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// RecordManifest stores what remote's local mirror holds right now - after
// a pull, the content the remote had. It does nothing for a remote without
// a ManifestPath.
func RecordManifest(remote Remote) error {
	if remote.ManifestPath == "" {
		return nil
	}
	m, err := ScanManifest(remote.ResolvedLocalPath)
	if err != nil {
		return fmt.Errorf("could not record the content of %s: %w", remote.ResolvedLocalPath, err)
	}
	return WriteManifest(remote.ManifestPath, m)
}

// changes is what a SyncBack has to carry to the remote.
type changes struct {
	local  Manifest
	upload []string
	remove []string
}

// changesSinceManifest compares remote's local mirror with its manifest.
// Without a manifest - the mirror was never pulled - every local file is
// uploaded and nothing is deleted, since nothing is known about what the
// remote holds.
func changesSinceManifest(remote Remote) (changes, error) {
	local, err := ScanManifest(remote.ResolvedLocalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return changes{}, fmt.Errorf("local directory does not exist: %s", remote.ResolvedLocalPath)
		}
		return changes{}, err
	}
	known, err := ReadManifest(remote.ManifestPath)
	if err != nil {
		return changes{}, err
	}

	c := changes{local: local}
	for rel, state := range local {
		if previous, ok := known[rel]; !ok || previous != state {
			c.upload = append(c.upload, rel)
		}
	}
	for rel := range known {
		if _, ok := local[rel]; !ok {
			c.remove = append(c.remove, rel)
		}
	}
	sort.Strings(c.upload)
	sort.Strings(c.remove)
	return c, nil
}
