package drivers

import "fmt"

type ociDriver struct{}

func NewOCIDriver() SyncDriver {
	return &ociDriver{}
}

func (d *ociDriver) Sync(remote Remote) error {
	return fmt.Errorf("oci driver not implemented yet")
}
