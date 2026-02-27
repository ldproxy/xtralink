package app

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mew-sh/dotenv"
)

// loadDotEnvIfPresent loads .env in the current working directory.
func loadDotEnvIfPresent() error {
	err := dotenv.Load()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		if fallbackErr := loadDotEnvLenient(".env"); fallbackErr != nil {
			return fmt.Errorf("could not load .env: %w", err)
		}
	}

	return nil
}

// loadDotEnvLenient is a fallback loader that ignores malformed lines (e.g. "-") and does not override existing environment variables.
func loadDotEnvLenient(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}

		key := strings.TrimSpace(kv[0])
		if key == "" {
			continue
		}
		value := parseLenientEnvValue(kv[1])

		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		if setErr := os.Setenv(key, value); setErr != nil {
			return setErr
		}
	}

	return scanner.Err()
}

// handles cases where comments are present at the end of the line, e.g. "VAR=foo # comment"
func parseLenientEnvValue(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return ""
	}

	// Quoted value: keep inner content as-is (without surrounding quotes).
	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1]
		}
	}

	// Unquoted value: support inline comments (e.g. "VAR=foo # comment").
	// A '#' starts a comment only when outside quotes and preceded by whitespace.
	inSingle := false
	inDouble := false
	for i := 0; i < len(v); i++ {
		ch := v[i]
		switch ch {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '#':
			if !inSingle && !inDouble {
				if i == 0 || v[i-1] == ' ' || v[i-1] == '\t' {
					return strings.TrimSpace(v[:i])
				}
			}
		}
	}

	return strings.TrimSpace(v)
}
