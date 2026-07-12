package drivers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/rs/zerolog"
)

const (
	lfsPointerVersion = "version https://git-lfs.github.com/spec/v1"
	lfsMediaType      = "application/vnd.git-lfs+json"

	// lfsPointerMaxSize caps how many bytes we read when deciding whether a
	// file is an LFS pointer. Real pointers are tiny (a handful of short
	// lines); anything larger is certainly actual content.
	lfsPointerMaxSize = 1024

	// lfsBatchChunk limits how many objects we request per batch call so we
	// stay well under any server-side batch size limits.
	lfsBatchChunk = 100
)

// lfsPointer is a resolved LFS pointer found in the working tree.
type lfsPointer struct {
	absPath string
	oid     string // sha256 hex
	size    int64
}

// resolveLFS scans scanRoot for Git LFS pointer files and replaces each with
// its real content, fetched from the remote's LFS server via the batch API
// (basic transfer over HTTP(S)).
//
// go-git does not run the LFS smudge filter, so LFS-tracked files land on disk
// as pointer stubs. This step reconstitutes them after clone/pull. It is a
// no-op for repositories that do not use LFS.
func resolveLFS(logger zerolog.Logger, gitURL string, auth *githttp.BasicAuth, scanRoot string) error {
	endpoint, err := lfsEndpoint(gitURL)
	if err != nil {
		return err
	}

	pointers, err := findLFSPointers(scanRoot)
	if err != nil {
		return fmt.Errorf("could not scan for LFS pointers: %w", err)
	}
	if len(pointers) == 0 {
		return nil
	}

	logger.Debug().Str("endpoint", endpoint).Int("count", len(pointers)).Msg("resolving LFS pointers")

	client := &http.Client{}
	byOID := make(map[string]lfsPointer, len(pointers))
	for _, p := range pointers {
		byOID[p.oid] = p
	}

	for chunk := range chunkPointers(pointers, lfsBatchChunk) {
		resp, err := lfsBatch(client, endpoint, auth, chunk)
		if err != nil {
			return fmt.Errorf("LFS batch request failed: %w", err)
		}

		for _, obj := range resp.Objects {
			if obj.Error != nil {
				return fmt.Errorf("LFS server rejected object %s: %s (code %d)", obj.OID, obj.Error.Message, obj.Error.Code)
			}
			if obj.Actions.Download == nil {
				return fmt.Errorf("LFS server returned no download action for object %s", obj.OID)
			}

			p, ok := byOID[obj.OID]
			if !ok {
				continue
			}

			if err := downloadLFSObject(client, endpoint, auth, obj.Actions.Download, p); err != nil {
				return fmt.Errorf("could not download LFS object %s: %w", obj.OID, err)
			}
		}
	}

	logger.Info().Int("count", len(pointers)).Str("scan_root", scanRoot).Msg("resolved LFS pointers")
	return nil
}

// lfsEndpoint derives the LFS server base URL from a Git HTTP(S) remote URL,
// following the guessing rule from the LFS spec: append "/info/lfs", inserting
// ".git" first when the URL does not already end in it.
func lfsEndpoint(gitURL string) (string, error) {
	raw := strings.TrimSpace(gitURL)
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid git url %q: %w", gitURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("LFS is only supported over http(s), got scheme %q", u.Scheme)
	}

	trimmed := strings.TrimSuffix(raw, "/")
	if strings.HasSuffix(trimmed, ".git") {
		return trimmed + "/info/lfs", nil
	}
	return trimmed + ".git/info/lfs", nil
}

// findLFSPointers walks scanRoot and returns every file that parses as an LFS
// pointer. The .git directory is skipped.
func findLFSPointers(scanRoot string) ([]lfsPointer, error) {
	var pointers []lfsPointer

	err := filepath.WalkDir(scanRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() > lfsPointerMaxSize {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		oid, size, ok := parseLFSPointer(data)
		if !ok {
			return nil
		}

		pointers = append(pointers, lfsPointer{absPath: path, oid: oid, size: size})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pointers, nil
}

// parseLFSPointer reports whether data is a valid LFS pointer and, if so,
// returns its sha256 oid (hex) and size.
func parseLFSPointer(data []byte) (oid string, size int64, ok bool) {
	if !bytes.HasPrefix(data, []byte(lfsPointerVersion)) {
		return "", 0, false
	}

	var (
		haveOID  bool
		haveSize bool
	)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		key, value, found := strings.Cut(line, " ")
		if !found {
			continue
		}
		switch key {
		case "oid":
			raw, isSHA256 := strings.CutPrefix(value, "sha256:")
			if !isSHA256 {
				return "", 0, false
			}
			if _, err := hex.DecodeString(raw); err != nil || len(raw) != 64 {
				return "", 0, false
			}
			oid = raw
			haveOID = true
		case "size":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return "", 0, false
			}
			size = n
			haveSize = true
		}
	}

	if !haveOID || !haveSize {
		return "", 0, false
	}
	return oid, size, true
}

// chunkPointers yields slices of pointers no larger than size.
func chunkPointers(pointers []lfsPointer, size int) func(func([]lfsPointer) bool) {
	return func(yield func([]lfsPointer) bool) {
		for start := 0; start < len(pointers); start += size {
			end := start + size
			if end > len(pointers) {
				end = len(pointers)
			}
			if !yield(pointers[start:end]) {
				return
			}
		}
	}
}

type lfsObjectID struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

type lfsBatchRequest struct {
	Operation string        `json:"operation"`
	Transfers []string      `json:"transfers"`
	Objects   []lfsObjectID `json:"objects"`
}

type lfsAction struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header"`
}

type lfsResponseObject struct {
	OID     string `json:"oid"`
	Size    int64  `json:"size"`
	Actions struct {
		Download *lfsAction `json:"download"`
	} `json:"actions"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type lfsBatchResponse struct {
	Transfer string              `json:"transfer"`
	Objects  []lfsResponseObject `json:"objects"`
}

// lfsBatch performs a single "download" batch request for the given pointers.
func lfsBatch(client *http.Client, endpoint string, auth *githttp.BasicAuth, pointers []lfsPointer) (*lfsBatchResponse, error) {
	objects := make([]lfsObjectID, 0, len(pointers))
	for _, p := range pointers {
		objects = append(objects, lfsObjectID{OID: p.oid, Size: p.size})
	}

	body, err := json.Marshal(lfsBatchRequest{
		Operation: "download",
		Transfers: []string{"basic"},
		Objects:   objects,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, endpoint+"/objects/batch", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", lfsMediaType)
	req.Header.Set("Content-Type", lfsMediaType)
	applyBasicAuth(req, auth)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(snippet)))
	}

	var parsed lfsBatchResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("could not decode batch response: %w", err)
	}
	return &parsed, nil
}

// downloadLFSObject fetches a single LFS object and atomically replaces the
// pointer file at p.absPath with the verified content.
func downloadLFSObject(client *http.Client, endpoint string, auth *githttp.BasicAuth, action *lfsAction, p lfsPointer) error {
	req, err := http.NewRequest(http.MethodGet, action.Href, nil)
	if err != nil {
		return err
	}
	for k, v := range action.Header {
		req.Header.Set(k, v)
	}
	// Only fall back to the git credentials when the server did not supply its
	// own auth header and the download stays on the LFS host (avoids leaking
	// credentials to a redirected/presigned URL on a different host).
	if _, hasAuth := action.Header["Authorization"]; !hasAuth && sameHost(action.Href, endpoint) {
		applyBasicAuth(req, auth)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("unexpected status %s: %s", resp.Status, strings.TrimSpace(string(snippet)))
	}

	return writeVerifiedLFSObject(resp.Body, p)
}

// writeVerifiedLFSObject streams r into a temp file alongside p.absPath, checks
// its sha256 against the expected oid, and renames it into place on success.
// The replacement inherits the pointer file's permissions, since os.CreateTemp
// would otherwise leave the content readable only by the owner (0600).
func writeVerifiedLFSObject(r io.Reader, p lfsPointer) error {
	mode := os.FileMode(0o644)
	if info, err := os.Stat(p.absPath); err == nil {
		mode = info.Mode().Perm()
	}

	dir := filepath.Dir(p.absPath)
	tmp, err := os.CreateTemp(dir, ".lfs-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		tmp.Close()
		os.Remove(tmpPath)
	}()

	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(tmp, hasher), r)
	if err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	if written != p.size {
		return fmt.Errorf("size mismatch for %s: expected %d, got %d", p.oid, p.size, written)
	}
	if got := hex.EncodeToString(hasher.Sum(nil)); got != p.oid {
		return fmt.Errorf("oid mismatch: expected %s, got %s", p.oid, got)
	}

	if err := os.Chmod(tmpPath, mode); err != nil {
		return err
	}

	return os.Rename(tmpPath, p.absPath)
}

func applyBasicAuth(req *http.Request, auth *githttp.BasicAuth) {
	if auth == nil {
		return
	}
	req.SetBasicAuth(auth.Username, auth.Password)
}

func sameHost(a, b string) bool {
	ua, err := url.Parse(a)
	if err != nil {
		return false
	}
	ub, err := url.Parse(b)
	if err != nil {
		return false
	}
	return strings.EqualFold(ua.Host, ub.Host)
}
