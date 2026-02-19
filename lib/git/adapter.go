package git

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Adapter struct{}

func NewAdapter() *Adapter {
	return &Adapter{}
}

// Fetch clones a Git repository (MVP) and optionally copies a sub-path
// into the requested local destination.
func (a *Adapter) Fetch(url, tag, subPath, destination, user, password string) error {
	if strings.TrimSpace(url) == "" {
		return fmt.Errorf("git url is empty")
	}
	if strings.TrimSpace(destination) == "" {
		return fmt.Errorf("destination is empty")
	}

	repoDir, err := os.MkdirTemp("", "xtra-sync-git-*")
	if err != nil {
		return fmt.Errorf("could not create temp directory: %w", err)
	}
	defer os.RemoveAll(repoDir)

	branch := strings.TrimSpace(tag)
	if branch == "" {
		branch = "main"
	}

	cloneCmd := exec.Command("git", "clone", "--depth", "1", "--branch", branch, url, repoDir)
	cloneCmd.Stdout = os.Stdout
	cloneCmd.Stderr = os.Stderr
	if err := cloneCmd.Run(); err != nil {
		return fmt.Errorf("git clone failed: %w", err)
	}

	sourcePath := repoDir
	if strings.TrimSpace(subPath) != "" {
		sourcePath = filepath.Join(repoDir, filepath.Clean(subPath))
	}

	if _, err := os.Stat(sourcePath); err != nil {
		return fmt.Errorf("source path not found (%s): %w", sourcePath, err)
	}

	if err := os.RemoveAll(destination); err != nil {
		return fmt.Errorf("could not clean destination path (%s): %w", destination, err)
	}

	if err := copyPath(sourcePath, destination); err != nil {
		return fmt.Errorf("copy to %s failed: %w", destination, err)
	}

	fmt.Printf("[xtra-sync][lib/git] synced %s -> %s\n", sourcePath, destination)
	return nil
}

func copyPath(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return copyDir(src, dst)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return copyFile(src, dst)
}

func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
			continue
		}

		if err := copyFile(srcPath, dstPath); err != nil {
			return err
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}

	return out.Sync()
}
