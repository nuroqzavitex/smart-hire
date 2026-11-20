package app

import (
	"bytes"
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
		if !validSearchAfter(t.After, t.Text) {
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

// maxSortID bounds the job_id sort key in a token (a UUID is 36 chars).
const maxSortID = 64

// validSearchAfter checks the client-supplied search_after against the sort
// keys ListOpen uses, so a crafted token is a 400 instead of an OpenSearch
// error that would count against the circuit breaker: [published_at, job_id]
// without a text query, [_score, published_at, job_id] with one.
func validSearchAfter(raw json.RawMessage, text bool) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var keys []any
	if dec.Decode(&keys) != nil {
		return false
	}
	if text {
		if len(keys) != 3 {
			return false
		}
		score, ok := keys[0].(json.Number)
		if !ok {
			return false
		}
		if _, err := score.Float64(); err != nil {
			return false
		}
		keys = keys[1:]
	}
	if len(keys) != 2 {
		return false
	}
	at, ok := keys[0].(json.Number)
	if !ok {
		return false
	}
	if _, err := at.Int64(); err != nil {
		return false
	}
	id, ok := keys[1].(string)
	return ok && id != "" && len(id) <= maxSortID
}
