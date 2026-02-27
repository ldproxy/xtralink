package envutil

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/mew-sh/dotenv"
)

// LoadDotEnvIfPresent loads the first existing .env candidate.
// Existing environment variables are not overwritten.
func LoadDotEnvIfPresent(candidates ...string) error {
	if len(candidates) == 0 {
		candidates = []string{".env"}
	}

	for _, candidate := range candidates {
		path := strings.TrimSpace(candidate)
		if path == "" {
			continue
		}

		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		err := dotenv.Load(path)
		if err == nil {
			return nil
		}

		if fallbackErr := loadDotEnvLenient(path); fallbackErr != nil {
			return fmt.Errorf("could not load %s: %w", path, err)
		}
		return nil
	}

	return nil
}

// loadDotEnvLenient ignores malformed lines and does not override existing env vars.
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

func parseLenientEnvValue(raw string) string {
	v := strings.TrimSpace(raw)
	if v == "" {
		return ""
	}

	if len(v) >= 2 {
		if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
			return v[1 : len(v)-1]
		}
	}

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
