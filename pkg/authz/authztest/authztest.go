// Package authztest provides a test JWT issuer serving a JWKS endpoint.
package authztest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
)

// Issuer and Audience are the iss/aud claims the test tokens carry.
const (
	Issuer   = "smarthire-auth"
	Audience = "smarthire-api"
)

// KeyIssuer signs RS256 tokens and serves the matching JWKS.
type KeyIssuer struct {
	key *rsa.PrivateKey
	kid string
	srv *httptest.Server
}

// NewIssuer generates a key and starts the JWKS server (closed on cleanup).
func NewIssuer(t *testing.T) *KeyIssuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("authztest: generate key: %v", err)
	}
	kidBytes := make([]byte, 8)
	_, _ = rand.Read(kidBytes)
	is := &KeyIssuer{key: key, kid: base64.RawURLEncoding.EncodeToString(kidBytes)}

	jwks, err := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "kid": is.kid, "use": "sig", "alg": "RS256",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}})
	if err != nil {
		t.Fatalf("authztest: marshal jwks: %v", err)
	}
	is.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwks)
	}))
	t.Cleanup(is.srv.Close)
	return is
}

// JWKSURL is the URL of the JWKS endpoint.
func (i *KeyIssuer) JWKSURL() string { return i.srv.URL }

// KID is the key id of the signing key.
func (i *KeyIssuer) KID() string { return i.kid }

// Close stops the JWKS server (idempotent with the test cleanup).
func (i *KeyIssuer) Close() { i.srv.Close() }

// Sign returns a signed access token; mods may alter the claims before signing.
func (i *KeyIssuer) Sign(t *testing.T, sub string, role authz.Role, ttl time.Duration, mods ...func(jwt.MapClaims)) string {
	t.Helper()
	now := time.Now()
	jti := make([]byte, 8)
	_, _ = rand.Read(jti)
	claims := jwt.MapClaims{
		"iss":  Issuer,
		"aud":  Audience,
		"sub":  sub,
		"role": string(role),
		"jti":  base64.RawURLEncoding.EncodeToString(jti),
		"iat":  now.Unix(),
		"exp":  now.Add(ttl).Unix(),
	}
	for _, m := range mods {
		m(claims)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = i.kid
	raw, err := tok.SignedString(i.key)
	if err != nil {
		t.Fatalf("authztest: sign: %v", err)
	}
	return raw
}
