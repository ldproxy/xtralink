package drivers

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

type gitDriver struct{}

func NewGitDriver() SyncDriver {
	return &gitDriver{}
}

func (d *gitDriver) Sync(remote Remote) error {
	if strings.TrimSpace(remote.URL) == "" {
		return fmt.Errorf("git url is empty")
	}
	if strings.TrimSpace(remote.ResolvedLocalPath) == "" {
		return fmt.Errorf("destination is empty")
	}

	repoDir, err := os.MkdirTemp("", "xtra-sync-git-*")
	if err != nil {
		return fmt.Errorf("could not create temp directory: %w", err)
	}
	defer os.RemoveAll(repoDir)

	auth := gitAuth(remote.User, remote.Password)
	ref := strings.TrimSpace(remote.Tag)
	if ref == "" {
		ref = "main"
	}

	if err := cloneWithRefFallback(remote.URL, repoDir, ref, auth); err != nil {
		return err
	}

	sourcePath := repoDir
	if strings.TrimSpace(remote.Path) != "" {
		sourcePath = filepath.Join(repoDir, filepath.Clean(remote.Path))
	}

	if _, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("source path not found (%s): %w", sourcePath, err)
	}

	if err := os.RemoveAll(remote.ResolvedLocalPath); err != nil {
		return fmt.Errorf("could not clean destination path (%s): %w", remote.ResolvedLocalPath, err)
	}

	if err := copyPath(sourcePath, remote.ResolvedLocalPath); err != nil {
		return fmt.Errorf("copy to %s failed: %w", remote.ResolvedLocalPath, err)
	}

	fmt.Printf("[xtra-sync][drivers/git] synced %s -> %s\n", sourcePath, remote.ResolvedLocalPath)
	return nil
}

func gitAuth(user, password string) *githttp.BasicAuth {
	if strings.TrimSpace(user) == "" && strings.TrimSpace(password) == "" {
		return nil
	}

	u := strings.TrimSpace(user)
	if u == "" {
		u = "git"
	}

	return &githttp.BasicAuth{
		Username: u,
		Password: strings.TrimSpace(password),
	}
}

func cloneWithRefFallback(url, repoDir, ref string, auth *githttp.BasicAuth) error {
	err := cloneWithReference(url, repoDir, plumbing.NewBranchReferenceName(ref), auth)
	if err == nil {
		return nil
	}

	errTag := cloneWithReference(url, repoDir, plumbing.NewTagReferenceName(ref), auth)
	if errTag == nil {
		return nil
	}

	return fmt.Errorf("git clone failed for ref %q as branch (%v) and tag (%v)", ref, err, errTag)
}

func cloneWithReference(url, repoDir string, ref plumbing.ReferenceName, auth *githttp.BasicAuth) error {
	_, err := git.PlainClone(repoDir, false, &git.CloneOptions{
		URL:           url,
		Depth:         1,
		SingleBranch:  true,
		ReferenceName: ref,
		Auth:          auth,
		Progress:      os.Stdout,
	})
	return err
}
