// Package devidp mints caller JWTs the way an identity provider would, for
// local development and tests. Production tokens come from a real IdP; the
// gateway only ever holds the public half (HOK_JWT_KEYS).
package devidp

import (
	"crypto/ed25519"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Token struct {
	KeyID       string
	Key         ed25519.PrivateKey
	Issuer      string
	Audience    string
	Application string // sub
	Instance    string // "instance" claim
	IssuedAt    time.Time
	TTL         time.Duration
}

func (t Token) Sign() (string, error) {
	claims := jwt.MapClaims{
		"iss": t.Issuer,
		"aud": t.Audience,
		"sub": t.Application,
		"iat": t.IssuedAt.Unix(),
		"exp": t.IssuedAt.Add(t.TTL).Unix(),
	}
	if t.Instance != "" {
		claims["instance"] = t.Instance
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	tok.Header["kid"] = t.KeyID
	return tok.SignedString(t.Key)
}
