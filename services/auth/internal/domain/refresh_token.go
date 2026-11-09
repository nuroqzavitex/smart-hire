package domain

import (
	"time"

	"github.com/google/uuid"
)

// RefreshToken is the stored (hashed) form of an opaque refresh token. Tokens
// issued from one login share a FamilyID so reuse can revoke the whole chain.
type RefreshToken struct {
	ID, UserID, FamilyID uuid.UUID
	Hash                 []byte
	ExpiresAt, CreatedAt time.Time
	RevokedAt            *time.Time
	ReplacedBy           *uuid.UUID
}
