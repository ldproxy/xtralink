package drivers

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mew-sh/dotenv"
	"github.com/rs/zerolog"
)

func init() {
	dotenv.Load("../../.env")
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

func firstNonEmpty(value, fallback string) string {
	v := strings.TrimSpace(value)
	if v == "" {
		return fallback
	}
	return v
}
