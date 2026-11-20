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

var errBadToken = apperr.New(apperr.ErrValidation, "invalid page token")

// Search backends that issue ListOpen page tokens.
const (
	backendOpenSearch = "os"
	backendPostgres   = "pg"
)

// searchToken is a ListOpen page token: an OpenSearch search_after position
// (with whether it came from a text query) or a Postgres keyset cursor.
type searchToken struct {
	Backend string          `json:"b"`
	Text    bool            `json:"q,omitempty"`
	After   json.RawMessage `json:"sa,omitempty"`
	Cursor  *Cursor         `json:"c,omitempty"`
}

func encodeSearchToken(t searchToken) string {
	b, _ := json.Marshal(t) // only marshalable fields
	return base64.RawURLEncoding.EncodeToString(b)
}

// decodeSearchToken parses a ListOpen token; anything malformed is
// ErrValidation.
func decodeSearchToken(s string) (searchToken, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return searchToken{}, errBadToken
	}
	var t searchToken
	if err := json.Unmarshal(raw, &t); err != nil {
		return searchToken{}, errBadToken
	}
	switch t.Backend {
	case backendOpenSearch:
		var after []json.RawMessage
		if json.Unmarshal(t.After, &after) != nil || len(after) == 0 {
			return searchToken{}, errBadToken
		}
	case backendPostgres:
		if t.Cursor == nil || t.Cursor.ID == uuid.Nil || t.Cursor.At.IsZero() {
			return searchToken{}, errBadToken
		}
	default:
		return searchToken{}, errBadToken
	}
	return t, nil
}
