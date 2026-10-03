package gateway

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

// Authenticator turns a caller's bearer JWT into the Caller the gateway
// vouches for. Tokens are EdDSA-signed by an identity provider whose public
// keys the gateway trusts by key id; the gateway never holds a private key
// able to mint caller tokens.
//
// The token itself never goes further than this struct (D5): the command
// carries the derived Caller, signed with the gateway's own key.
type Authenticator struct {
	Keys     identity.Keyring
	Issuer   string
	Audience string
	// Leeway absorbs clock skew between the IdP and the gateway.
	Leeway time.Duration
	Now    func() time.Time // nil: time.Now
}

// InstanceClaim names the calling application instance. The application
// itself is the standard "sub" claim.
const InstanceClaim = "instance"

type callerClaims struct {
	Instance string `json:"instance"`
	jwt.RegisteredClaims
}

// authError is a refused authentication, rendered as RFC 6750 401.
type authError struct {
	// code is the RFC 6750 error code; empty when no credentials were sent,
	// in which case the challenge carries no error attribute (§3.1).
	code   string
	detail string
}

func (e *authError) Error() string { return e.detail }

func (a *Authenticator) Authenticate(r *http.Request) (wire.Caller, error) {
	authz := r.Header.Values("Authorization")
	if len(authz) == 0 {
		return wire.Caller{}, &authError{detail: "missing bearer token"}
	}
	if len(authz) > 1 {
		return wire.Caller{}, &authError{code: "invalid_request", detail: "multiple Authorization headers"}
	}
	scheme, token, ok := strings.Cut(authz[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
		return wire.Caller{}, &authError{code: "invalid_request", detail: "Authorization must be \"Bearer <jwt>\""}
	}
	opts := []jwt.ParserOption{
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(a.Issuer),
		jwt.WithAudience(a.Audience),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
		jwt.WithLeeway(a.Leeway),
	}
	if a.Now != nil {
		opts = append(opts, jwt.WithTimeFunc(a.Now))
	}
	var claims callerClaims
	_, err := jwt.ParseWithClaims(token, &claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		pub, ok := a.Keys[kid]
		if !ok {
			return nil, fmt.Errorf("unknown key id %q", kid)
		}
		return ed25519.PublicKey(pub), nil
	}, opts...)
	if err != nil {
		return wire.Caller{}, &authError{code: "invalid_token", detail: tokenErrorDetail(err)}
	}
	switch {
	case claims.Subject == "":
		return wire.Caller{}, &authError{code: "invalid_token", detail: "token has no sub (calling application)"}
	case claims.Instance == "":
		return wire.Caller{}, &authError{code: "invalid_token", detail: "token has no " + InstanceClaim + " claim (calling instance)"}
	}
	return wire.Caller{Application: claims.Subject, Instance: claims.Instance}, nil
}

// tokenErrorDetail names the failed check without echoing token contents. The
// callers are internal services: a precise reason saves an investigation.
func tokenErrorDetail(err error) string {
	for _, c := range []struct {
		err  error
		text string
	}{
		{jwt.ErrTokenMalformed, "token is malformed"},
		{jwt.ErrTokenExpired, "token is expired"},
		{jwt.ErrTokenNotValidYet, "token is not valid yet"},
		{jwt.ErrTokenUsedBeforeIssued, "token issued in the future"},
		{jwt.ErrTokenInvalidAudience, "token audience does not include this gateway"},
		{jwt.ErrTokenInvalidIssuer, "token issuer is not trusted"},
		{jwt.ErrTokenRequiredClaimMissing, "token is missing a required claim (exp, iss or aud)"},
		{jwt.ErrTokenSignatureInvalid, "token signature is invalid or algorithm is not EdDSA"},
		{jwt.ErrTokenUnverifiable, "token signing key is unknown"},
	} {
		if errors.Is(err, c.err) {
			return c.text
		}
	}
	return "token is invalid"
}

func (e *authError) challenge() string {
	if e.code == "" {
		return `Bearer realm="kafka-backbone"`
	}
	return fmt.Sprintf(`Bearer realm="kafka-backbone", error=%q, error_description=%q`, e.code, e.detail)
}
