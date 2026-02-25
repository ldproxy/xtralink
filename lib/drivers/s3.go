package drivers

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rhnvrm/simples3"
)

type s3Driver struct{}

func NewS3Driver() SyncDriver {
	return &s3Driver{}
}

func (d *s3Driver) Sync(remote Remote) error {
	bucket, key, endpointFromURL, err := parseS3Location(remote.URL)
	if err != nil {
		return err
	}
	if strings.TrimSpace(remote.Path) != "" {
		key = strings.TrimPrefix(strings.TrimSpace(remote.Path), "/")
	}

	region := firstEnv("XTRA_SYNC_S3_REGION", "AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	access := strings.TrimSpace(remote.User)
	secret := strings.TrimSpace(remote.Password)
	if access == "" {
		access = firstEnv("XTRA_SYNC_S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "accessKey", "ACCESS_KEY")
	}
	if secret == "" {
		secret = firstEnv("XTRA_SYNC_S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY", "secretKey", "SECRET_KEY")
	}
	if access == "" || secret == "" {
		return fmt.Errorf("s3 requires credentials in remote.user/remote.password or environment")
	}

	client := simples3.New(region, access, secret)
	endpoint := firstEnv("XTRA_SYNC_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = endpointFromURL
	}
	if endpoint != "" {
		client.SetEndpoint(endpoint)
	}

	objects, prefix, err := d.resolveObjects(client, bucket, key)
	if err != nil {
		return err
	}

	signature := objectsSignature(objects)
	cacheRoot := filepath.Join(os.TempDir(), "xtra-sync-cache", "s3", hashString(remote.URL+"|"+key))
	cacheState := filepath.Join(cacheRoot, ".state")

	cacheDataDir := filepath.Join(cacheRoot, "data")
	cacheFresh := cacheHasSignature(cacheState, signature)
	if _, err := os.Stat(cacheDataDir); os.IsNotExist(err) {
		cacheFresh = false
	}

	if !cacheFresh {
		if err := os.RemoveAll(cacheDataDir); err != nil {
			return fmt.Errorf("could not clear s3 cache dir (%s): %w", cacheDataDir, err)
		}
		if err := os.MkdirAll(cacheDataDir, 0o755); err != nil {
			return fmt.Errorf("could not create s3 cache dir (%s): %w", cacheDataDir, err)
		}

		for _, obj := range objects {
			if strings.HasSuffix(obj.Key, "/") {
				continue
			}

			rel := strings.TrimPrefix(obj.Key, prefix)
			if prefix != "" && rel == obj.Key {
				return fmt.Errorf("s3 object key is outside resolved prefix: key=%q prefix=%q", obj.Key, prefix)
			}
			if strings.TrimSpace(rel) == "" {
				continue
			}

			rel = filepath.FromSlash(rel)
			if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return fmt.Errorf("resolved relative key escapes destination: key=%q rel=%q", obj.Key, rel)
			}
			dst := filepath.Join(cacheDataDir, rel)
			if err := d.downloadSingleObject(client, bucket, obj.Key, dst); err != nil {
				return fmt.Errorf("s3 download failed for %s: %w", obj.Key, err)
			}
		}

		if err := os.MkdirAll(cacheRoot, 0o755); err != nil {
			return fmt.Errorf("could not create s3 cache root (%s): %w", cacheRoot, err)
		}
		if err := os.WriteFile(cacheState, []byte(signature), 0o644); err != nil {
			return fmt.Errorf("could not write s3 cache state (%s): %w", cacheState, err)
		}
		fmt.Printf("[xtra-sync][drivers/s3] refreshed cache for s3://%s/%s\n", bucket, prefix)
	} else {
		fmt.Printf("[xtra-sync][drivers/s3] cache unchanged for s3://%s/%s\n", bucket, prefix)
	}

	if err := syncPathMirror(cacheDataDir, remote.ResolvedLocalPath); err != nil {
		return fmt.Errorf("could not mirror s3 cache to target (%s -> %s): %w", cacheDataDir, remote.ResolvedLocalPath, err)
	}

	fmt.Printf("[xtra-sync][drivers/s3] synced s3://%s/%s* -> %s\n", bucket, prefix, remote.ResolvedLocalPath)
	return nil
}

func (d *s3Driver) resolveObjects(client *simples3.S3, bucket, key string) ([]simples3.Object, string, error) {
	if key == "" {
		list, err := client.List(simples3.ListInput{Bucket: bucket, Prefix: ""})
		if err != nil {
			return nil, "", fmt.Errorf("s3 list failed for s3://%s/: %w", bucket, err)
		}
		if len(list.Objects) == 0 {
			return nil, "", fmt.Errorf("no s3 objects found for s3://%s/", bucket)
		}
		return list.Objects, "", nil
	}

	listNoSlash, err := client.List(simples3.ListInput{Bucket: bucket, Prefix: key})
	if err != nil {
		return nil, "", fmt.Errorf("s3 list failed for s3://%s/%s: %w", bucket, key, err)
	}
	for _, obj := range listNoSlash.Objects {
		if obj.Key == key {
			return nil, "", fmt.Errorf("remote.path must point to a directory, but got file: %s", key)
		}
	}

	if !strings.HasSuffix(key, "/") {
		pref := key + "/"
		listSlash, err := client.List(simples3.ListInput{Bucket: bucket, Prefix: pref})
		if err != nil {
			return nil, "", fmt.Errorf("s3 list failed for s3://%s/%s: %w", bucket, pref, err)
		}
		if len(listSlash.Objects) > 0 {
			return listSlash.Objects, pref, nil
		}
	}

	if len(listNoSlash.Objects) > 0 {
		return listNoSlash.Objects, key, nil
	}

	return nil, "", fmt.Errorf("no s3 objects found for s3://%s/%s", bucket, key)
}

func (d *s3Driver) downloadSingleObject(client *simples3.S3, bucket, key, destination string) error {
	rc, err := client.FileDownload(simples3.DownloadInput{
		Bucket:    bucket,
		ObjectKey: key,
	})
	if err != nil {
		return err
	}
	defer rc.Close()

	return writeReaderToFile(rc, destination)
}

func objectsSignature(objects []simples3.Object) string {
	parts := make([]string, 0, len(objects))
	for _, o := range objects {
		parts = append(parts, fmt.Sprintf("%s|%s|%d|%s", o.Key, o.ETag, o.Size, o.LastModified))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

func cacheHasSignature(stateFile, signature string) bool {
	raw, err := os.ReadFile(stateFile)
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(raw)) == strings.TrimSpace(signature)
}

func hashString(v string) string {
	s := sha1.Sum([]byte(v))
	return hex.EncodeToString(s[:])
}

func parseS3Location(raw string) (bucket, key, endpoint string, err error) {
	u := strings.TrimSpace(raw)
	if u == "" {
		return "", "", "", fmt.Errorf("s3 url is empty")
	}

	lower := strings.ToLower(u)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		parsed, parseErr := url.Parse(u)
		if parseErr != nil {
			return "", "", "", fmt.Errorf("invalid s3 http url %q: %w", raw, parseErr)
		}
		path := strings.TrimPrefix(parsed.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		bucket = strings.TrimSpace(parts[0])
		if bucket == "" {
			return "", "", "", fmt.Errorf("s3 bucket is empty in url %q", raw)
		}
		if len(parts) == 2 {
			key = strings.TrimPrefix(parts[1], "/")
		}
		endpoint = parsed.Scheme + "://" + parsed.Host
		return bucket, key, endpoint, nil
	}

	if !strings.HasPrefix(lower, "s3://") {
		return "", "", "", fmt.Errorf("s3 url must start with s3:// or http(s)://")
	}

	trimmed := strings.TrimPrefix(u, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = strings.TrimSpace(parts[0])
	if bucket == "" {
		return "", "", "", fmt.Errorf("s3 bucket is empty in url %q", raw)
	}

	if len(parts) == 2 {
		key = strings.TrimPrefix(parts[1], "/")
	}

	return bucket, key, "", nil
}

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}
