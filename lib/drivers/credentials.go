package drivers

import (
	"os"
	"strings"
)

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}

func firstEnvWithRemoteID(remoteID, base string) string {
	if id := strings.TrimSpace(remoteID); id != "" && strings.TrimSpace(base) != "" {
		if v := strings.TrimSpace(os.Getenv(base + "_" + id)); v != "" {
			return v
		}
	}
	return ""
}
