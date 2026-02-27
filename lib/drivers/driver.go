package drivers

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"
)

// SyncDriver syncs a single remote source into a local destination.
type SyncDriver interface {
	Sync(remote Remote) error
}

// PushDriver pushes prepared content to a remote target.
type PushDriver interface {
	Push(push PushRequest) error
}

type Factory struct {
	git     SyncDriver
	s3      SyncDriver
	oci     SyncDriver
	ociPush PushDriver
}

func NewFactory() *Factory {
	return NewFactoryWithLogger(zerolog.Nop())
}

func NewFactoryWithLogger(logger zerolog.Logger) *Factory {
	oci := NewOCIDriver(logger.With().Str("driver", "oci").Logger())
	return &Factory{
		git:     NewGitDriver(logger.With().Str("driver", "git").Logger()),
		s3:      NewS3Driver(logger.With().Str("driver", "s3").Logger()),
		oci:     oci,
		ociPush: oci,
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

func (f *Factory) PusherFor(targetType string) (PushDriver, error) {
	switch strings.ToUpper(strings.TrimSpace(targetType)) {
	case "OCI":
		return f.ociPush, nil
	default:
		return nil, fmt.Errorf("unsupported push target type: %s", targetType)
	}
}
