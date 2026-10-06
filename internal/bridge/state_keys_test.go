package bridge

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/twmb/franz-go/pkg/kgo"
)

func stateTestSigner(t *testing.T, kid string, seed byte) *identity.Signer {
	t.Helper()
	s, err := identity.NewSigner(identity.RoleBridge, kid, bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func stateTestKeys(t *testing.T, role identity.Role, signers ...*identity.Signer) identity.TrustedKeys {
	t.Helper()
	k := identity.Keyring{}
	for _, s := range signers {
		k[s.KeyID()] = s.Public()
	}
	keys, err := identity.NewTrustedKeys(role, k)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestStateKeysConfiguration(t *testing.T) {
	old := stateTestSigner(t, "old", 1)
	active := stateTestSigner(t, "active", 2)
	impostor := stateTestSigner(t, "active", 3)
	spec, err := apispec.Load("payments", api.Payments)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name      string
		keys      identity.TrustedKeys
		wantError string
		trustOld  bool
	}{
		{name: "default trusts active only"},
		{name: "explicit current key", keys: active.Self()},
		{name: "rotation overlap", keys: stateTestKeys(t, identity.RoleBridge, old, active), trustOld: true},
		{name: "wrong role", keys: stateTestKeys(t, identity.RoleGateway, active), wantError: "state keys must be trusted bridge keys"},
		{name: "active key absent", keys: old.Self(), wantError: "state keys must trust the active signing key"},
		{name: "active key id with wrong public key", keys: impostor.Self(), wantError: "state keys must trust the active signing key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, err := New(Config{Service: "payments", Spec: spec, Brokers: []string{"localhost:9092"},
				Keys: stateTestKeys(t, identity.RoleGateway, old), Signer: active, StateKeys: tt.keys, Upstream: "http://localhost:8082"})
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("got %v, want %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyStateRecord(b.cfg.StateKeys, signedStateRecord(active, StateTopic("payments"), 0, "r:key", nil)); err != nil {
				t.Fatalf("active state key rejected: %v", err)
			}
			err = verifyStateRecord(b.cfg.StateKeys, signedStateRecord(old, StateTopic("payments"), 0, "r:key", nil))
			if (err == nil) != tt.trustOld {
				t.Fatalf("old key: error %v, want trusted=%v", err, tt.trustOld)
			}
		})
	}
}

// Rotation changes the accepted public keys, never the signature boundary.
func TestStateKeyRotationPreservesSignatureBinding(t *testing.T) {
	old, active := stateTestSigner(t, "old", 1), stateTestSigner(t, "active", 2)
	trust := stateTestKeys(t, identity.RoleBridge, old, active)
	for _, signer := range []*identity.Signer{old, active} {
		t.Run(signer.KeyID(), func(t *testing.T) {
			for _, value := range [][]byte{nil, []byte(`{"phase":"started"}`)} {
				if err := verifyStateRecord(trust, signedStateRecord(signer, StateTopic("payments"), 1, "i:key", value)); err != nil {
					t.Fatalf("trusted record rejected: %v", err)
				}
			}
			for name, alter := range map[string]func(*kgo.Record){
				"topic":     func(r *kgo.Record) { r.Topic = StateTopic("orders") },
				"partition": func(r *kgo.Record) { r.Partition++ },
				"key":       func(r *kgo.Record) { r.Key = []byte("i:victim") },
				"value":     func(r *kgo.Record) { r.Value = []byte(`{"phase":"done"}`) },
				"tombstone": func(r *kgo.Record) { r.Value = nil },
				"unsigned":  func(r *kgo.Record) { r.Headers = nil },
				"unknown key": func(r *kgo.Record) {
					*r = *signedStateRecord(stateTestSigner(t, "unknown", 3), r.Topic, r.Partition, string(r.Key), r.Value)
				},
				"wrong domain": func(r *kgo.Record) {
					sig := signer.Sign("http-over-kafka/response/v1", stateSigned(r.Topic, r.Partition, string(r.Key), r.Value))
					r.Headers[1].Value = []byte(base64.StdEncoding.EncodeToString(sig))
				},
			} {
				t.Run(name, func(t *testing.T) {
					r := signedStateRecord(signer, StateTopic("payments"), 1, "i:key", []byte(`{"phase":"started"}`))
					alter(r)
					if err := verifyStateRecord(trust, r); !errors.Is(err, errStateTampered) {
						t.Fatalf("altered state must be rejected: %v", err)
					}
				})
			}
		})
	}
}
