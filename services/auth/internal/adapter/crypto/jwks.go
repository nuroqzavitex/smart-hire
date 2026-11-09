package crypto

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
)

const kidLen = 16

type publicKey struct {
	kid string
	key *rsa.PublicKey
}

// KeySet is the signing key plus the public keys that remain valid for
// verification (previous keys during rotation).
type KeySet struct {
	signer    *rsa.PrivateKey
	signerKID string
	public    []publicKey // signer first, then extras
}

// LoadKeys reads the PEM RSA private key at currentPath (PKCS#8 or PKCS#1) and
// the PEM public keys (PKIX, or an RSA private key) at extraPaths.
func LoadKeys(currentPath string, extraPaths ...string) (*KeySet, error) {
	raw, err := os.ReadFile(currentPath) //nolint:gosec // path comes from operator config
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}
	priv, err := parsePrivate(raw)
	if err != nil {
		return nil, fmt.Errorf("private key %s: %w", currentPath, err)
	}
	kid, err := keyID(&priv.PublicKey)
	if err != nil {
		return nil, err
	}
	ks := &KeySet{signer: priv, signerKID: kid, public: []publicKey{{kid, &priv.PublicKey}}}
	for _, p := range extraPaths {
		raw, err := os.ReadFile(p) //nolint:gosec // path comes from operator config
		if err != nil {
			return nil, fmt.Errorf("read public key: %w", err)
		}
		pub, err := parsePublic(raw)
		if err != nil {
			return nil, fmt.Errorf("public key %s: %w", p, err)
		}
		k, err := keyID(pub)
		if err != nil {
			return nil, err
		}
		if k == kid {
			continue // same key as the current one
		}
		ks.public = append(ks.public, publicKey{k, pub})
	}
	return ks, nil
}

func parsePrivate(raw []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("no PEM data")
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("not an RSA key")
		}
		return rk, nil
	}
	k, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("unsupported private key encoding")
	}
	return k, nil
}

func parsePublic(raw []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, errors.New("no PEM data")
	}
	if k, err := x509.ParsePKIXPublicKey(block.Bytes); err == nil {
		rk, ok := k.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("not an RSA key")
		}
		return rk, nil
	}
	if k, err := x509.ParsePKCS1PublicKey(block.Bytes); err == nil {
		return k, nil
	}
	return nil, errors.New("unsupported public key encoding")
}

// keyID is base64url(sha256(DER public key))[:16].
func keyID(k *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(k)
	if err != nil {
		return "", fmt.Errorf("marshal public key: %w", err)
	}
	sum := sha256.Sum256(der)
	return base64.RawURLEncoding.EncodeToString(sum[:])[:kidLen], nil
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// JWKSHandler serves the public keys as a JWKS document.
func JWKSHandler(keys *KeySet) http.Handler {
	set := make([]jwk, 0, len(keys.public))
	for _, p := range keys.public {
		set = append(set, jwk{
			Kty: "RSA", Kid: p.kid, Use: "sig", Alg: "RS256",
			N: base64.RawURLEncoding.EncodeToString(p.key.N.Bytes()),
			E: base64.RawURLEncoding.EncodeToString(big.NewInt(int64(p.key.E)).Bytes()),
		})
	}
	body, err := json.Marshal(struct {
		Keys []jwk `json:"keys"`
	}{set})
	if err != nil {
		// Marshalling plain strings cannot fail.
		panic("crypto: marshal jwks: " + err.Error())
	}
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, _ = w.Write(body)
	})
}
