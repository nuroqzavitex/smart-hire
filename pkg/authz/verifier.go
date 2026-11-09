package authz

import (
	"context"
	"fmt"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

// Verifier validates RS256 access tokens against a JWKS endpoint.
type Verifier struct {
	kf     keyfunc.Keyfunc
	parser *jwt.Parser
	cancel context.CancelFunc
}

// Close stops the background JWKS refresh. The Verifier must not be used for
// unknown key ids afterwards. It is safe to call more than once.
func (v *Verifier) Close() { v.cancel() }

// NewVerifier fetches the JWKS from jwksURL and returns a verifier. The initial
// fetch is done eagerly and its failure is returned: starting with an empty
// key set would reject every request (or hide a misconfigured URL) until the
// background refresh happened to succeed, so we fail fast at startup instead.
// Unknown kids trigger an automatic refresh (rate limited by keyfunc).
//
// Lifetime contract: ctx only bounds the initial fetch. The background refresh
// and key lookups run on an internal context owned by the Verifier, so a
// short-lived startup ctx cannot silently disable key rotation. Call Close to
// stop the refresh goroutine when the Verifier is no longer needed.
func NewVerifier(ctx context.Context, jwksURL, issuer, audience string) (*Verifier, error) {
	life, cancel := context.WithCancel(context.Background())
	failOnFirst := false // NoErrorReturnFirstHTTPReq=false: surface the first fetch error
	type result struct {
		kf  keyfunc.Keyfunc
		err error
	}
	ch := make(chan result, 1)
	go func() {
		kf, err := keyfunc.NewDefaultOverrideCtx(life, []string{jwksURL}, keyfunc.Override{
			NoErrorReturnFirstHTTPReq: &failOnFirst,
			HTTPTimeout:               10 * time.Second,
		})
		ch <- result{kf, err}
	}()
	var kf keyfunc.Keyfunc
	select {
	case r := <-ch:
		if r.err != nil {
			cancel()
			return nil, fmt.Errorf("authz: load jwks: %w", r.err)
		}
		kf = r.kf
	case <-ctx.Done():
		cancel()
		return nil, fmt.Errorf("authz: load jwks: %w", ctx.Err())
	}
	p := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(issuer),
		jwt.WithAudience(audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30*time.Second),
	)
	return &Verifier{kf: kf, parser: p, cancel: cancel}, nil
}

type tokenClaims struct {
	Role Role `json:"role"`
	jwt.RegisteredClaims
}

var errInvalid = func() error { return apperr.New(apperr.ErrUnauthenticated, "invalid token") }

// Verify validates raw and returns its claims. Every failure maps to
// ErrUnauthenticated with a generic message (no detail leaked).
func (v *Verifier) Verify(raw string) (Claims, error) {
	var tc tokenClaims
	tok, err := v.parser.ParseWithClaims(raw, &tc, v.kf.Keyfunc)
	if err != nil || !tok.Valid {
		return Claims{}, errInvalid()
	}
	if tc.Subject == "" || !tc.Role.valid() || tc.ExpiresAt == nil {
		return Claims{}, errInvalid()
	}
	return Claims{
		Subject:   tc.Subject,
		Role:      tc.Role,
		TokenID:   tc.ID,
		ExpiresAt: tc.ExpiresAt.Time,
	}, nil
}
