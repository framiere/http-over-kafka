// Package identity signs and verifies internal messages with Ed25519.
//
// Keys belong to a role. The gateway signs commands, the bridge signs
// responses and results (D10). Each process holds only its own private key
// and the public keys of the role it reads from, so the bridge cannot mint
// commands and the gateway cannot mint results. Trust is typed by role
// (TrustedKeys): a verifier for results cannot be built from gateway keys.
// Key ids allow rotation: verifiers trust a keyring, a signer uses one key.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Role is who produced a signed message.
type Role string

const (
	RoleGateway Role = "gateway" // commands
	RoleBridge  Role = "bridge"  // responses, results
)

func (r Role) valid() error {
	if r != RoleGateway && r != RoleBridge {
		return fmt.Errorf("identity: unknown role %q", r)
	}
	return nil
}

// SigningKeyEnv names the private key of role, "<kid>:<base64 32-byte seed>",
// e.g. KB_GATEWAY_SIGNING_KEY. Only processes of that role may have it.
func SigningKeyEnv(r Role) string { return "KB_" + strings.ToUpper(string(r)) + "_SIGNING_KEY" }

// TrustedKeysEnv names the public keys of role,
// "<kid>:<base64>,<kid>:<base64>", e.g. KB_TRUSTED_BRIDGE_KEYS.
func TrustedKeysEnv(r Role) string { return "KB_TRUSTED_" + strings.ToUpper(string(r)) + "_KEYS" }

var ErrBadSignature = errors.New("identity: bad signature")

type Signer struct {
	role Role
	kid  string
	key  ed25519.PrivateKey
}

func NewSigner(role Role, kid string, seed []byte) (*Signer, error) {
	if err := role.valid(); err != nil {
		return nil, err
	}
	if err := validKid(kid); err != nil {
		return nil, err
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("identity: seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	return &Signer{role: role, kid: kid, key: ed25519.NewKeyFromSeed(seed)}, nil
}

func (s *Signer) Role() Role    { return s.role }
func (s *Signer) KeyID() string { return s.kid }

// Self returns the trust containing only this signer's public key, for a
// process that must verify what it wrote itself (e.g. the bridge reading
// back its own state).
func (s *Signer) Self() TrustedKeys {
	return TrustedKeys{role: s.role, keys: Keyring{s.kid: s.Public()}}
}

func (s *Signer) Public() ed25519.PublicKey { return s.key.Public().(ed25519.PublicKey) }

// Sign binds msg to a domain string so a signature made for one message type
// can never verify as another.
func (s *Signer) Sign(domain string, msg []byte) []byte {
	return ed25519.Sign(s.key, signedBytes(domain, msg))
}

// Keyring maps key id to a public key. It carries no role: use it for
// foreign keys (e.g. an IdP's), and TrustedKeys for internal messages.
type Keyring map[string]ed25519.PublicKey

func (k Keyring) Verify(kid, domain string, msg, sig []byte) error {
	pub, ok := k[kid]
	if !ok {
		return fmt.Errorf("%w: unknown key id %q", ErrBadSignature, kid)
	}
	if !ed25519.Verify(pub, signedBytes(domain, msg), sig) {
		return ErrBadSignature
	}
	return nil
}

func signedBytes(domain string, msg []byte) []byte {
	b := make([]byte, 0, len(domain)+1+len(msg))
	b = append(b, domain...)
	b = append(b, 0)
	return append(b, msg...)
}

// TrustedKeys are the public keys of one role.
type TrustedKeys struct {
	role Role
	keys Keyring
}

func NewTrustedKeys(role Role, keys Keyring) (TrustedKeys, error) {
	if err := role.valid(); err != nil {
		return TrustedKeys{}, err
	}
	if len(keys) == 0 {
		return TrustedKeys{}, fmt.Errorf("identity: no trusted %s keys", role)
	}
	return TrustedKeys{role: role, keys: keys}, nil
}

func ParseTrustedKeys(role Role, s string) (TrustedKeys, error) {
	k, err := ParseKeyring(s)
	if err != nil {
		return TrustedKeys{}, err
	}
	return NewTrustedKeys(role, k)
}

// Role is the role these keys vouch for; the zero value has none and
// verifies nothing.
func (t TrustedKeys) Role() Role { return t.role }

func (t TrustedKeys) IsZero() bool { return t.role == "" }

func (t TrustedKeys) Verify(kid, domain string, msg, sig []byte) error {
	if t.role == "" {
		return fmt.Errorf("%w: no trusted keys configured", ErrBadSignature)
	}
	return t.keys.Verify(kid, domain, msg, sig)
}

func SignerFromEnv(role Role) (*Signer, error) {
	name := SigningKeyEnv(role)
	v := os.Getenv(name)
	if v == "" {
		return nil, fmt.Errorf("identity: %s is not set (run `go run ./cmd/keygen -role %s`)", name, role)
	}
	kid, seed, err := parsePair(v)
	if err != nil {
		return nil, fmt.Errorf("identity: %s: %w", name, err)
	}
	return NewSigner(role, kid, seed)
}

func TrustedKeysFromEnv(role Role) (TrustedKeys, error) {
	name := TrustedKeysEnv(role)
	v := os.Getenv(name)
	if v == "" {
		return TrustedKeys{}, fmt.Errorf("identity: %s is not set", name)
	}
	t, err := ParseTrustedKeys(role, v)
	if err != nil {
		return TrustedKeys{}, fmt.Errorf("identity: %s: %w", name, err)
	}
	return t, nil
}

func ParseKeyring(s string) (Keyring, error) {
	k := Keyring{}
	for part := range strings.SplitSeq(s, ",") {
		kid, pub, err := parsePair(strings.TrimSpace(part))
		if err != nil {
			return nil, err
		}
		if len(pub) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("identity: key %q: public key must be %d bytes", kid, ed25519.PublicKeySize)
		}
		if _, dup := k[kid]; dup {
			return nil, fmt.Errorf("identity: duplicate key id %q", kid)
		}
		k[kid] = ed25519.PublicKey(pub)
	}
	return k, nil
}

// Generate returns a fresh key pair in env-var format.
func Generate(kid string) (signingKey, trustedKey string, err error) {
	if err := validKid(kid); err != nil {
		return "", "", err
	}
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return "", "", err
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	enc := base64.StdEncoding.EncodeToString
	return kid + ":" + enc(seed), kid + ":" + enc(pub), nil
}

func parsePair(s string) (string, []byte, error) {
	kid, b64, ok := strings.Cut(s, ":")
	if !ok {
		return "", nil, errors.New("expected <kid>:<base64>")
	}
	if err := validKid(kid); err != nil {
		return "", nil, err
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", nil, fmt.Errorf("key %q: %w", kid, err)
	}
	return kid, raw, nil
}

func validKid(kid string) error {
	if kid == "" || len(kid) > 64 || strings.ContainsAny(kid, ":, \t\r\n") {
		return fmt.Errorf("identity: invalid key id %q", kid)
	}
	return nil
}
