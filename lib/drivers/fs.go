package drivers

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	dirsync "github.com/Varjelus/dirsync"
)

// syncPathMirror synchronizes a source directory into dst.
func syncPathMirror(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{Op: "sync", Path: src, Err: fs.ErrInvalid}
	}

	if dstInfo, err := os.Stat(dst); err == nil && !dstInfo.IsDir() {
		return fmt.Errorf("destination must be a directory: %s", dst)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := removeDestinationTypeConflicts(src, dst); err != nil {
		return err
	}

	return dirsync.Sync(src, dst)
}

func removeDestinationTypeConflicts(srcDir, dstDir string) error {
	return filepath.WalkDir(srcDir, func(srcPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}

		dstPath := filepath.Join(dstDir, rel)
		dstInfo, err := os.Lstat(dstPath)
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}

		if d.IsDir() != dstInfo.IsDir() {
			return os.RemoveAll(dstPath)
		}

		return nil
	})
}

func writeReaderToFile(r io.Reader, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, r); err != nil {
		return err
	}

	return out.Sync()
}
