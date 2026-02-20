package drivers

import (
	"fmt"
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
	bucket, key, err := parseS3Location(remote.URL)
	if err != nil {
		return err
	}
	if strings.TrimSpace(remote.Path) != "" {
		key = strings.TrimPrefix(strings.TrimSpace(remote.Path), "/")
	}
	if key == "" {
		return fmt.Errorf("s3 path is empty; set url key or remote.path")
	}

	region := strings.TrimSpace(os.Getenv("XTRA_SYNC_S3_REGION"))
	if region == "" {
		region = "us-east-1"
	}

	access := strings.TrimSpace(remote.User)
	secret := strings.TrimSpace(remote.Password)
	if access == "" || secret == "" {
		return fmt.Errorf("s3 requires credentials in remote.user/remote.password")
	}

	client := simples3.New(region, access, secret)
	if endpoint := strings.TrimSpace(os.Getenv("XTRA_SYNC_S3_ENDPOINT")); endpoint != "" {
		client.SetEndpoint(endpoint)
	}

	if err := os.RemoveAll(remote.ResolvedLocalPath); err != nil {
		return fmt.Errorf("could not clean destination path (%s): %w", remote.ResolvedLocalPath, err)
	}

	if err := d.downloadSingleObject(client, bucket, key, remote.ResolvedLocalPath); err == nil {
		fmt.Printf("[xtra-sync][drivers/s3] synced s3://%s/%s -> %s\n", bucket, key, remote.ResolvedLocalPath)
		return nil
	}

	prefix := key
	if !strings.HasSuffix(prefix, "/") {
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

func parseS3Location(raw string) (bucket, key string, err error) {
	u := strings.TrimSpace(raw)
	if u == "" {
		return "", "", fmt.Errorf("s3 url is empty")
	}
	if !strings.HasPrefix(strings.ToLower(u), "s3://") {
		return "", "", fmt.Errorf("s3 url must start with s3://")
	}

	trimmed := strings.TrimPrefix(u, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = strings.TrimSpace(parts[0])
	if bucket == "" {
		return "", "", fmt.Errorf("s3 bucket is empty in url %q", raw)
	}

	if len(parts) == 2 {
		key = strings.TrimPrefix(parts[1], "/")
	}

	return bucket, key, nil
}
