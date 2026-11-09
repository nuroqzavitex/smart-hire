package app

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
)

const refreshSecretBytes = 32

// newRefreshSecret returns a random opaque token and its sha256 hash; only the
// hash is stored.
func newRefreshSecret() (raw string, hash []byte, err error) {
	b := make([]byte, refreshSecretBytes)
	if _, err := rand.Read(b); err != nil {
		return "", nil, fmt.Errorf("read random: %w", err)
	}
	raw = base64.RawURLEncoding.EncodeToString(b)
	return raw, hashRefresh(raw), nil
}

func hashRefresh(raw string) []byte {
	h := sha256.Sum256([]byte(raw))
	return h[:]
}
