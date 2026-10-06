package bridge_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Pre-distribute public keys to response consumers, as required during a
// rolling rotation. State trust is independently configured on each bridge.
func rotationKeys(t *testing.T, e *env) (*identity.Signer, identity.TrustedKeys) {
	t.Helper()
	active, err := identity.NewSigner(identity.RoleBridge, "rotated-bridge", bytes.Repeat([]byte{42}, ed25519.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	keys, err := identity.NewTrustedKeys(identity.RoleBridge, identity.Keyring{
		e.bridgeSigner.KeyID(): e.bridgeSigner.Public(), active.KeyID(): active.Public(),
	})
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.bridgeKeys = keys
	e.mu.Unlock()
	return active, keys
}

func TestBridgeSigningKeyRotationRestoresMixedState(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "rotation", api.Payments, srv.URL)
	active, trust := rotationKeys(t, e)
	oldConfig := e.config()
	oldConfig.StateKeys = trust
	stop := e.start(oldConfig)
	oldCmd := e.charge("old-account", "old-key")
	e.send(oldCmd)
	oldResponse := e.await(oldCmd.RequestID, 30*time.Second)
	if oldResponse.Status != 201 {
		t.Fatalf("old signer: %+v", oldResponse)
	}
	stop()

	newConfig := oldConfig
	newConfig.Signer = active
	stop = e.start(newConfig)
	replay := func(original wire.Command, response wire.Response) {
		t.Helper()
		retry := original
		retry.RequestID = wire.NewRequestID()
		e.send(retry)
		got := e.await(retry.RequestID, 30*time.Second)
		if got.ReplayOf != original.RequestID || got.Status != response.Status || !bytes.Equal(got.Body.Bytes(), response.Body.Bytes()) {
			t.Fatalf("stored response changed across key rotation: got %+v, original %+v", got, response)
		}
	}
	replay(oldCmd, oldResponse)
	newCmd := e.charge("new-account", "new-key")
	e.send(newCmd)
	newResponse := e.await(newCmd.RequestID, 30*time.Second)
	if newResponse.Status != 201 {
		t.Fatalf("new signer: %+v", newResponse)
	}
	stop()

	// A still-old instance can take over state written by a new instance
	// once both public keys are distributed. This also permits rollback.
	e.start(oldConfig)
	replay(oldCmd, oldResponse)
	replay(newCmd, newResponse)
	for _, account := range []string{"old-account", "new-account"} {
		if n := pay.Account(account).DebitCount; n != 1 {
			t.Fatalf("%s executed %d times across rotation, want 1", account, n)
		}
	}

	// Verify that rotating actually changed newly published signatures;
	// restored state must never cause the old signing key to be reused.
	for topic, key := range map[string]string{
		wire.ReplyTopic(e.gw):       newCmd.RequestID,
		wire.ResultTopic(e.service): newCmd.DedupKey(),
	} {
		kafkatest.Consume(t, topic, 20*time.Second, func(rs []*kgo.Record) bool {
			for _, r := range rs {
				if string(r.Key) != key {
					continue
				}
				if err := wire.VerifySignature(r, active.Self()); err != nil {
					t.Fatalf("new record on %s not signed by active key: %v", topic, err)
				}
				return true
			}
			return false
		})
	}
}

func TestBridgeSigningKeyRotationPreservesUncertainEffect(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "rotation-crash", api.Payments, srv.URL)
	active, trust := rotationKeys(t, e)
	process := e.proc("payments", "after-upstream", 0)
	cmd := e.charge("uncertain-account", "uncertain-key")
	e.send(cmd)
	kafkatest.Eventually(t, 60*time.Second, process.exited, "old bridge killed after the upstream effect")
	if n := pay.Account("uncertain-account").DebitCount; n != 1 {
		t.Fatalf("debits before rotation: %d, want 1", n)
	}
	cfg := e.config()
	cfg.Signer, cfg.StateKeys = active, trust
	e.start(cfg)
	got := e.await(cmd.RequestID, 60*time.Second)
	if got.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("uncertain effect lost across rotation: %+v", got)
	}
	retry := e.charge("uncertain-account", "uncertain-key")
	e.send(retry)
	got = e.await(retry.RequestID, 30*time.Second)
	if got.ReplayOf != cmd.RequestID || got.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("uncertain effect was not retained for retry: %+v", got)
	}
	if n := pay.Account("uncertain-account").DebitCount; n != 1 {
		t.Fatalf("uncertain effect repeated across rotation: %d debits", n)
	}
}

func TestBridgeSigningKeyRotationRefusesUntrustedHistory(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "rotation-refuse", api.Payments, srv.URL)
	active, _ := rotationKeys(t, e)
	stop := e.start(e.config())
	cmd := e.charge("account", "key")
	e.send(cmd)
	if got := e.await(cmd.RequestID, 30*time.Second); got.Status != 201 {
		t.Fatalf("before rotation: %+v", got)
	}
	stop()
	cfg := e.config()
	cfg.Signer = active // no old public key: trusting the current key is not enough
	b, err := bridge.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err = b.Run(ctx); err == nil || !strings.Contains(err.Error(), "dedup state tampered") || !strings.Contains(err.Error(), "unknown key id") {
		t.Fatalf("untrusted history must stop restoration: %v", err)
	}
	if n := pay.Account("account").DebitCount; n != 1 {
		t.Fatalf("unexpected execution after refused rotation: %d debits", n)
	}
}
