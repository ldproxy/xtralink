package drivers

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
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
		access = firstEnv("accessKey")
	}
	if secret == "" {
		secret = firstEnv("secretKey")
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

	if err := os.RemoveAll(remote.ResolvedLocalPath); err != nil {
		return fmt.Errorf("could not clean destination path (%s): %w", remote.ResolvedLocalPath, err)
	}

	if key != "" {
		if err := d.downloadSingleObject(client, bucket, key, remote.ResolvedLocalPath); err == nil {
			fmt.Printf("[xtra-sync][drivers/s3] synced s3://%s/%s -> %s\n", bucket, key, remote.ResolvedLocalPath)
			return nil
		}
	}

	prefix := key
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	list, err := client.List(simples3.ListInput{Bucket: bucket, Prefix: prefix})
	if err != nil {
		return fmt.Errorf("s3 list failed for s3://%s/%s: %w", bucket, prefix, err)
	}
	if len(list.Objects) == 0 {
		return fmt.Errorf("no s3 objects found for s3://%s/%s", bucket, prefix)
	}

	for _, obj := range list.Objects {
		if strings.HasSuffix(obj.Key, "/") {
			continue
		}

		rel := strings.TrimPrefix(obj.Key, prefix)
		if rel == obj.Key {
			rel = filepath.Base(obj.Key)
		}
		dst := filepath.Join(remote.ResolvedLocalPath, rel)
		if err := d.downloadSingleObject(client, bucket, obj.Key, dst); err != nil {
			return fmt.Errorf("s3 download failed for %s: %w", obj.Key, err)
		}
	}

	fmt.Printf("[xtra-sync][drivers/s3] synced s3://%s/%s* -> %s\n", bucket, prefix, remote.ResolvedLocalPath)
	return nil
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
