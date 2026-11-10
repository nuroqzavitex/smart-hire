package app

import (
	"encoding/base64"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

// EncodeCursor serializes a cursor as base64url(JSON).
func EncodeCursor(c Cursor) string {
	b, _ := json.Marshal(c) // Cursor has only marshalable fields
	return base64.RawURLEncoding.EncodeToString(b)
}

// DecodeCursor parses a page token; any malformed token is ErrValidation.
func DecodeCursor(s string) (Cursor, error) {
	bad := apperr.New(apperr.ErrValidation, "invalid page token")
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, bad
	}
	var c Cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return Cursor{}, bad
	}
	if c.ID == uuid.Nil || c.At.IsZero() {
		return Cursor{}, bad
	}
	return c, nil
}
