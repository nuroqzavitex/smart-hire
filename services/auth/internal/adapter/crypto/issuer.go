package crypto

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// Issuer signs RS256 access tokens and implements app.TokenIssuer.
type Issuer struct {
	keys     *KeySet
	issuer   string
	audience string
	ttl      time.Duration
}

// NewIssuer returns an Issuer signing with keys' current key.
func NewIssuer(keys *KeySet, issuer, audience string, ttl time.Duration) *Issuer {
	return &Issuer{keys: keys, issuer: issuer, audience: audience, ttl: ttl}
}

type accessClaims struct {
	Role authz.Role `json:"role"`
	jwt.RegisteredClaims
}

// IssueAccess mints an access token for u valid for the configured TTL.
func (i *Issuer) IssueAccess(u domain.User, now time.Time) (string, time.Duration, error) {
	claims := accessClaims{
		Role: u.Role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   u.ID.String(),
			Issuer:    i.issuer,
			Audience:  jwt.ClaimStrings{i.audience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(i.ttl)),
			ID:        uuid.NewString(),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = i.keys.signerKID
	signed, err := tok.SignedString(i.keys.signer)
	if err != nil {
		return "", 0, fmt.Errorf("sign access token: %w", err)
	}
	return signed, i.ttl, nil
}
