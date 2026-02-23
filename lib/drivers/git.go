package drivers

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"
)

type gitDriver struct {
	mu           sync.Mutex
	updatedRepos map[string]bool
}

func NewGitDriver() SyncDriver {
	return &gitDriver{updatedRepos: map[string]bool{}}
}

func (d *gitDriver) Sync(remote Remote) error {
	if strings.TrimSpace(remote.URL) == "" {
		return fmt.Errorf("git url is empty")
	}
	if strings.TrimSpace(remote.ResolvedLocalPath) == "" {
		return fmt.Errorf("destination is empty")
	}

	auth := gitAuth(remote.User, remote.Password)
	ref := strings.TrimSpace(remote.Tag)
	if ref == "" {
		ref = "main"
	}

	if strings.TrimSpace(remote.Path) == "" {
		return d.syncFullRepo(remote, ref, auth)
	}

	return d.syncSubpathViaCache(remote, ref, auth)
}

func (d *gitDriver) syncFullRepo(remote Remote, ref string, auth *githttp.BasicAuth) error {
	if err := d.syncOrCloneRepo(remote.ResolvedLocalPath, remote.URL, ref, auth); err != nil {
		return err
	}

	fmt.Printf("[xtra-sync][drivers/git] synced full repo %s -> %s\n", remote.URL, remote.ResolvedLocalPath)
	return nil
}

func (d *gitDriver) syncSubpathViaCache(remote Remote, ref string, auth *githttp.BasicAuth) error {
	cacheRepoDir, err := cacheRepoPath(remote.URL, ref)
	if err != nil {
		return err
	}

	if err := d.syncOrCloneRepo(cacheRepoDir, remote.URL, ref, auth); err != nil {
		return err
	}

	sourcePath, err := resolveRepoSubpath(cacheRepoDir, remote.Path)
	if err != nil {
		return err
	}

	if info, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("source path not found (%s): %w", sourcePath, err)
	} else if !info.IsDir() {
		return fmt.Errorf("remote.path must point to a directory, but got file: %s", remote.Path)
	}

	if err := syncPathMirror(sourcePath, remote.ResolvedLocalPath); err != nil {
		return fmt.Errorf("mirror sync to %s failed: %w", remote.ResolvedLocalPath, err)
	}

	fmt.Printf("[xtra-sync][drivers/git] synced cached subpath %s -> %s (cache: %s)\n", sourcePath, remote.ResolvedLocalPath, cacheRepoDir)
	return nil
}

func cacheRepoPath(url, ref string) (string, error) {
	base := filepath.Join(os.TempDir(), "xtra-sync-cache", "git")
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", fmt.Errorf("could not create cache base directory: %w", err)
	}

	h := sha1.Sum([]byte(strings.TrimSpace(url) + "|" + strings.TrimSpace(ref)))
	key := hex.EncodeToString(h[:])

	return filepath.Join(base, key), nil
}

func resolveRepoSubpath(repoDir, remotePath string) (string, error) {
	cleanPath := filepath.Clean(strings.TrimSpace(remotePath))
	if cleanPath == "." || cleanPath == "" {
		return repoDir, nil
	}
	if filepath.IsAbs(cleanPath) {
		return "", fmt.Errorf("remote path must be relative: %s", remotePath)
	}

	joined := filepath.Join(repoDir, cleanPath)
	rel, err := filepath.Rel(repoDir, joined)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("remote path escapes repository root: %s", remotePath)
	}

	return joined, nil
}

func (d *gitDriver) syncOrCloneRepo(repoDir, url, ref string, auth *githttp.BasicAuth) error {
	if _, err := os.Stat(filepath.Join(repoDir, ".git")); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("cannot inspect %s: %w", repoDir, err)
		}
		fmt.Printf("[DEBUG] Cloning repository to %s (URL: %s, Ref: %s)\n", repoDir, url, ref)
		if err := os.MkdirAll(filepath.Dir(repoDir), 0o755); err != nil {
			return fmt.Errorf("could not create parent directory for %s: %w", repoDir, err)
		}
		if err := cloneWithRefFallback(url, repoDir, ref, auth); err != nil {
			return err
		}
		d.markRepoUpdated(repoDir)
		return nil
	}

	if d.wasRepoUpdated(repoDir) {
		return nil
	}

	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		return fmt.Errorf("could not open local repository (%s): %w", repoDir, err)
	}

	if err := ensureOriginURL(repo, url); err != nil {
		return err
	}

	remoteHash, refName, err := resolveRemoteRefHash(url, ref, auth)
	if err != nil {
		return fmt.Errorf("could not resolve remote ref hash: %w", err)
	}

	localHash, err := resolveLocalRefHash(repo, refName)
	if err == nil && localHash == remoteHash {
		d.markRepoUpdated(repoDir)
		fmt.Printf("[DEBUG] Pull skipped (unchanged): %s @ %s\n", repoDir, remoteHash)
		return nil
	}

	if err := pullWithRefFallback(repo, ref, auth); err != nil {
		return fmt.Errorf("git pull failed in %s: %w", repoDir, err)
	}

	d.markRepoUpdated(repoDir)
	fmt.Printf("[DEBUG] Pull successful!\n")
	return nil
}

func (d *gitDriver) wasRepoUpdated(repoDir string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.updatedRepos[repoDir]
}

func (d *gitDriver) markRepoUpdated(repoDir string) {
	d.mu.Lock()
	d.updatedRepos[repoDir] = true
	d.mu.Unlock()
}

func resolveRemoteRefHash(url, ref string, auth *githttp.BasicAuth) (plumbing.Hash, plumbing.ReferenceName, error) {
	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{url},
	})

	refs, err := remote.List(&git.ListOptions{Auth: auth})
	if err != nil {
		return plumbing.ZeroHash, "", err
	}

	for _, candidate := range []plumbing.ReferenceName{
		plumbing.NewBranchReferenceName(ref),
		plumbing.NewTagReferenceName(ref),
	} {
		for _, r := range refs {
			if r.Name() == candidate {
				return r.Hash(), candidate, nil
			}
		}
	}

	return plumbing.ZeroHash, "", fmt.Errorf("ref %q not found as branch or tag", ref)
}

func resolveLocalRefHash(repo *git.Repository, refName plumbing.ReferenceName) (plumbing.Hash, error) {
	ref, err := repo.Reference(refName, true)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	return ref.Hash(), nil
}

func ensureOriginURL(repo *git.Repository, url string) error {
	remote, err := repo.Remote("origin")
	if err != nil {
		return fmt.Errorf("origin remote missing: %w", err)
	}

	if len(remote.Config().URLs) == 0 {
		return fmt.Errorf("origin remote has no URLs")
	}

	current := strings.TrimSpace(remote.Config().URLs[0])
	expected := strings.TrimSpace(url)
	if current == expected {
		return nil
	}

	return fmt.Errorf("origin URL mismatch: expected %q, got %q", expected, current)
}

func pullWithRefFallback(repo *git.Repository, ref string, auth *githttp.BasicAuth) error {
	branchErr := pullWithReference(repo, plumbing.NewBranchReferenceName(ref), auth)
	if branchErr == nil {
		return nil
	}

	tagErr := pullWithReference(repo, plumbing.NewTagReferenceName(ref), auth)
	if tagErr == nil {
		return nil
	}

	return fmt.Errorf("pull failed for ref %q as branch (%v) and tag (%v)", ref, branchErr, tagErr)
}

func pullWithReference(repo *git.Repository, ref plumbing.ReferenceName, auth *githttp.BasicAuth) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}

	err = wt.Pull(&git.PullOptions{
		RemoteName:    "origin",
		Depth:         1,
		SingleBranch:  true,
		ReferenceName: ref,
		Auth:          auth,
	})
	if err == nil || errors.Is(err, git.NoErrAlreadyUpToDate) {
		return nil
	}
	return err
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
	if err := os.MkdirAll(filepath.Dir(repoDir), 0o755); err != nil {
		return fmt.Errorf("could not create parent directory for clone target: %w", err)
	}

	if _, err := os.Stat(repoDir); err == nil {
		entries, readErr := os.ReadDir(repoDir)
		if readErr != nil {
			return fmt.Errorf("could not inspect clone target directory: %w", readErr)
		}
		if len(entries) > 0 {
			return fmt.Errorf("clone target directory is not empty: %s", repoDir)
		}
	}

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
