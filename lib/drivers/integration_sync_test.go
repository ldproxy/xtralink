package drivers

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
)

func init() {
	godotenv.Load("../../.env")
}

func TestIntegrationSync_Git(t *testing.T) {
	url := os.Getenv("XTRALINK_IT_GIT_URL")
	if url == "" {
		t.Skip("set XTRALINK_IT_GIT_URL to run git integration test")
	}

	target := t.TempDir()
	driver := NewGitDriver(zerolog.Nop())

	err := driver.Sync(Remote{
		Type:              "GIT",
		URL:               url,
		Tag:               firstNonEmpty(os.Getenv("XTRALINK_IT_GIT_TAG"), "main"),
		Path:              os.Getenv("XTRALINK_IT_GIT_PATH"),
		User:              os.Getenv("XTRALINK_IT_GIT_USER"),
		Password:          os.Getenv("XTRALINK_IT_GIT_PASSWORD"),
		ResolvedLocalPath: target,
	})
	if err != nil {
		t.Fatalf("git sync failed: %v", err)
	}

	if count := countFiles(t, target); count == 0 {
		t.Fatalf("git sync produced no files in %s", target)
	}
}

func TestIntegrationSync_S3(t *testing.T) {
	url := os.Getenv("XTRALINK_IT_S3_URL")
	if url == "" {
		t.Skip("set XTRALINK_IT_S3_URL to run s3 integration test")
	}

	user := os.Getenv("XTRALINK_IT_S3_USER")
	password := os.Getenv("XTRALINK_IT_S3_PASSWORD")
	if user == "" || password == "" {
		t.Skip("set XTRALINK_IT_S3_USER and XTRALINK_IT_S3_PASSWORD to run s3 integration test")
	}

	target := t.TempDir()
	driver := NewS3Driver(zerolog.Nop())

	err := driver.Sync(Remote{
		Type:              "S3",
		URL:               url,
		Path:              os.Getenv("XTRALINK_IT_S3_PATH"),
		User:              user,
		Password:          password,
		ResolvedLocalPath: target,
	})
	if err != nil {
		t.Fatalf("s3 sync failed: %v", err)
	}

	if count := countFiles(t, target); count == 0 {
		t.Fatalf("s3 sync produced no files in %s", target)
	}
}

// TestIntegrationSyncBack_S3 verifies pkg:mv_file's actual requirement: a
// local file appearing/disappearing must be reflected in S3 (upload/delete).
// It never touches pre-existing bucket content - every object it creates
// lives under a freshly random-generated prefix
// (xtrasync-it-syncback-<uuid>/...), which does not collide with anything
// that already exists, and is deleted again in t.Cleanup regardless of the
// test's outcome (pass, fail, or panic).
func TestIntegrationSyncBack_S3(t *testing.T) {
	url := os.Getenv("XTRASYNC_IT_S3_URL")
	user := os.Getenv("XTRASYNC_IT_S3_USER")
	password := os.Getenv("XTRASYNC_IT_S3_PASSWORD")
	if url == "" || user == "" || password == "" {
		t.Skip("set XTRASYNC_IT_S3_URL/_USER/_PASSWORD to run the s3 sync-back integration test")
	}

	testPrefix := path.Join(os.Getenv("XTRASYNC_IT_S3_PATH"), "xtrasync-it-syncback-"+uuid.NewString())
	driver := NewS3Driver(zerolog.Nop())
	remote := Remote{Type: "S3", URL: url, Path: testPrefix, User: user, Password: password}

	// Safety check: this randomly generated prefix must be unused - abort
	// rather than risk ever deleting something that wasn't created by this
	// test run.
	if probe := t.TempDir(); true {
		if err := driver.Sync(Remote{Type: "S3", URL: url, Path: testPrefix, User: user, Password: password, ResolvedLocalPath: probe}); err == nil {
			t.Fatalf("test prefix %q unexpectedly already has content - aborting without touching it", testPrefix)
		}
	}

	t.Cleanup(func() {
		empty := remote
		empty.ResolvedLocalPath = t.TempDir() // nothing local -> SyncBack deletes everything it finds under testPrefix
		if err := driver.SyncBack(empty); err != nil {
			t.Logf("cleanup: could not remove test objects under %q: %v", testPrefix, err)
		}
	})

	local := t.TempDir()
	writeFile(t, filepath.Join(local, "a.txt"), "a")
	writeFile(t, filepath.Join(local, "nested", "b.txt"), "b")

	remote.ResolvedLocalPath = local
	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack (initial upload): %v", err)
	}

	pulled := t.TempDir()
	pullOnce := remote
	pullOnce.ResolvedLocalPath = pulled
	if err := driver.Sync(pullOnce); err != nil {
		t.Fatalf("Sync (verify upload): %v", err)
	}
	assertFileContent(t, filepath.Join(pulled, "a.txt"), "a")
	assertFileContent(t, filepath.Join(pulled, "nested", "b.txt"), "b")

	// Removing a.txt locally and syncing back must delete it from S3 too -
	// this is the exact behavior pkg:mv_file depends on.
	if err := os.Remove(filepath.Join(local, "a.txt")); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := driver.SyncBack(remote); err != nil {
		t.Fatalf("SyncBack (after local delete): %v", err)
	}

	pulledAfterDelete := t.TempDir()
	pullTwice := remote
	pullTwice.ResolvedLocalPath = pulledAfterDelete
	if err := driver.Sync(pullTwice); err != nil {
		t.Fatalf("Sync (verify deletion): %v", err)
	}
	if _, err := os.Stat(filepath.Join(pulledAfterDelete, "a.txt")); !os.IsNotExist(err) {
		t.Errorf("expected a.txt to have been deleted from s3 after SyncBack, stat err = %v", err)
	}
	assertFileContent(t, filepath.Join(pulledAfterDelete, "nested", "b.txt"), "b")
}

func TestIntegrationSync_OCI(t *testing.T) {
	url := os.Getenv("XTRALINK_IT_OCI_URL")
	if url == "" {
		t.Skip("set XTRALINK_IT_OCI_URL to run oci integration test")
	}

	target := t.TempDir()
	driver := NewOCIDriver(zerolog.Nop())

	err := driver.Sync(Remote{
		Type:              "OCI",
		URL:               url,
		Tag:               firstNonEmpty(os.Getenv("XTRALINK_IT_OCI_TAG"), "latest"),
		Path:              os.Getenv("XTRALINK_IT_OCI_PATH"),
		User:              os.Getenv("XTRALINK_IT_OCI_USER"),
		Password:          os.Getenv("XTRALINK_IT_OCI_PASSWORD"),
		ResolvedLocalPath: target,
	})
	if err != nil {
		t.Fatalf("oci sync failed: %v", err)
	}

	if count := countFiles(t, target); count == 0 {
		t.Fatalf("oci sync produced no files in %s", target)
	}
}

func countFiles(t *testing.T, root string) int {
	t.Helper()
	count := 0
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	return count
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if string(got) != want {
		t.Errorf("content of %s = %q, want %q", path, got, want)
	}
}

func firstNonEmpty(value, fallback string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return fallback
	}
	return v
}
