package drivers

import "testing"

func TestDecideOCICacheAction(t *testing.T) {
	tests := []struct {
		name             string
		cacheHit         bool
		validatorMatches bool
		extractedExists  bool
		want             ociCacheAction
	}{
		{"fresh and extracted -> reuse", true, true, true, ociCacheReuseLocal},
		{"fresh but not extracted locally -> extract from cache", true, true, false, ociCacheExtractFromCached},
		{"cache hit but stale digest -> fetch fresh", true, false, true, ociCacheFetchFresh},
		{"cache hit but stale digest, not extracted -> fetch fresh", true, false, false, ociCacheFetchFresh},
		{"no cache entry at all -> fetch fresh", false, false, false, ociCacheFetchFresh},
		{"no cache entry, extracted stale copy lying around -> fetch fresh", false, false, true, ociCacheFetchFresh},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decideOCICacheAction(tt.cacheHit, tt.validatorMatches, tt.extractedExists)
			if got != tt.want {
				t.Errorf("decideOCICacheAction(%v, %v, %v) = %v, want %v",
					tt.cacheHit, tt.validatorMatches, tt.extractedExists, got, tt.want)
			}
		})
	}
}
