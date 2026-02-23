package drivers

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

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

// syncPathMirror synchronizes a source directory into dst.
// Files present in dst but missing in src are deleted.
func syncPathMirror(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return &os.PathError{Op: "sync", Path: src, Err: fs.ErrInvalid}
	}

	if err := ensureDir(dst); err != nil {
		return err
	}
	if err := syncDirContents(src, dst); err != nil {
		return err
	}
	if err := deleteMissingEntries(src, dst); err != nil {
		return err
	}

	return nil
}

func syncDirContents(srcDir, dstDir string) error {
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

		if d.IsDir() {
			if err := ensureDir(dstPath); err != nil {
				return err
			}
			return nil
		}

		if err := ensureParentDir(dstPath); err != nil {
			return err
		}
		if dstInfo, err := os.Stat(dstPath); err == nil && dstInfo.IsDir() {
			if err := os.RemoveAll(dstPath); err != nil {
				return err
			}
		}

		return copyFile(srcPath, dstPath)
	})
}

func deleteMissingEntries(srcDir, dstDir string) error {
	return filepath.WalkDir(dstDir, func(dstPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if dstPath == dstDir {
			return nil
		}

		rel, err := filepath.Rel(dstDir, dstPath)
		if err != nil {
			return err
		}
		srcPath := filepath.Join(srcDir, rel)

		if _, err := os.Lstat(srcPath); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}

		if err := os.RemoveAll(dstPath); err != nil {
			return err
		}
		if d.IsDir() {
			return filepath.SkipDir
		}

		return nil
	})
}

func ensureDir(path string) error {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			return os.MkdirAll(path, 0o755)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	return os.MkdirAll(path, 0o755)
}

func ensureParentDir(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0o755)
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
