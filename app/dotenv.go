package app

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// loadDotEnvIfPresent loads simple KEY=VALUE entries from .env in the current
// working directory. Existing process environment variables are not overridden.
func loadDotEnvIfPresent() error {
	f, err := os.Open(".env")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("could not open .env: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("invalid .env line %d", lineNo)
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key == "" {
			return fmt.Errorf("empty key in .env line %d", lineNo)
		}

		value = strings.Trim(value, "\"'")
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("could not set env %q from .env: %w", key, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("could not read .env: %w", err)
	}

	return nil
}
