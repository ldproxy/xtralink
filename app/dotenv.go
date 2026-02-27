package app

import "github.com/ldproxy/xtrasync/lib/envutil"

func loadDotEnvIfPresent() error {
	return envutil.LoadDotEnvIfPresent(".env")
}
