package app

import (
	"fmt"
	"os"

	"github.com/mew-sh/dotenv"
)

// loadDotEnvIfPresent loads .env in the current working directory.
func loadDotEnvIfPresent() error {
	err := dotenv.Load()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("could not load .env: %w", err)
	}

	return nil
}
