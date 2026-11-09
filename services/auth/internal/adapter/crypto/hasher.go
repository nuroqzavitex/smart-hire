// Package crypto holds the auth service's cryptographic adapters: password
// hashing, JWT signing and the JWKS document.
package crypto

import "github.com/alexedwards/argon2id"

// Hasher hashes passwords with argon2id and implements app.PasswordHasher.
type Hasher struct{ params *argon2id.Params }

// NewHasher returns a Hasher using p.
func NewHasher(p argon2id.Params) *Hasher { return &Hasher{params: &p} }

// Hash returns the PHC-encoded argon2id hash of pw.
func (h *Hasher) Hash(pw string) (string, error) {
	return argon2id.CreateHash(pw, h.params)
}

// Verify reports whether pw matches the PHC-encoded hash.
func (h *Hasher) Verify(pw, hash string) (bool, error) {
	return argon2id.ComparePasswordAndHash(pw, hash)
}
