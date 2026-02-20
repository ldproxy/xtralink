package drivers

import (
	"fmt"
	"strings"
)

// SyncDriver syncs a single remote source into a local destination.
type SyncDriver interface {
	Sync(remote Remote) error
}

type Factory struct {
	git SyncDriver
	s3  SyncDriver
	oci SyncDriver
}

func NewFactory() *Factory {
	return &Factory{
		git: NewGitDriver(),
		s3:  NewS3Driver(),
		oci: NewOCIDriver(),
	}
}

func (f *Factory) For(remoteType string) (SyncDriver, error) {
	switch strings.ToUpper(strings.TrimSpace(remoteType)) {
	case "GIT":
		return f.git, nil
	case "S3":
		return f.s3, nil
	case "OCI":
		return f.oci, nil
	default:
		return nil, fmt.Errorf("unsupported remote type: %s", remoteType)
	}
}
