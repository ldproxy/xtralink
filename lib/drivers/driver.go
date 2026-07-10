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

// PushDriver pushes prepared content to a remote target, building a new,
// self-contained artifact (e.g. an OCI image) - the target isn't modified
// in place, a new version of it is created.
type PushDriver interface {
	Push(push PushRequest) error
}

// SyncBackDriver mirrors a local change (including deletions) back to the
// remote it came from, in place - the counterpart to SyncDriver, used where
// PushDriver's "build a new artifact" semantics don't fit (e.g. a workflow
// step moving a file between two packages).
type SyncBackDriver interface {
	SyncBack(remote Remote) error
}

type Factory struct {
	git     SyncDriver
	s3      SyncDriver
	oci     SyncDriver
	fs      SyncDriver
	ociPush PushDriver

	fsSyncBack SyncBackDriver
	s3SyncBack SyncBackDriver
}

func NewFactory() *Factory {
	return NewFactoryWithLogger(zerolog.Nop())
}

func NewFactoryWithLogger(logger zerolog.Logger) *Factory {
	oci := NewOCIDriver(logger.With().Str("driver", "oci").Logger())
	fs := NewFSDriver(logger.With().Str("driver", "fs").Logger())
	s3 := NewS3Driver(logger.With().Str("driver", "s3").Logger())
	return &Factory{
		git:        NewGitDriver(logger.With().Str("driver", "git").Logger()),
		s3:         s3,
		oci:        oci,
		fs:         fs,
		ociPush:    oci,
		fsSyncBack: fs,
		s3SyncBack: s3,
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
	case "FS":
		return f.fs, nil
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

// SyncBackFor returns the SyncBackDriver for remoteType. Only FS and S3
// support syncing a local change back to the remote in place - GIT and OCI
// deliberately don't.
func (f *Factory) SyncBackFor(remoteType string) (SyncBackDriver, error) {
	switch strings.ToUpper(strings.TrimSpace(remoteType)) {
	case "FS":
		return f.fsSyncBack, nil
	case "S3":
		return f.s3SyncBack, nil
	default:
		return nil, fmt.Errorf("unsupported sync-back type: %s", remoteType)
	}
}
