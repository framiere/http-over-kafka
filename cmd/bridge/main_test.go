package main

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
)

func TestConfigStateKeys(t *testing.T) {
	activePrivate, activePublic, err := identity.Generate("active")
	if err != nil {
		t.Fatal(err)
	}
	_, oldPublic, err := identity.Generate("old")
	if err != nil {
		t.Fatal(err)
	}
	_, gatewayPublic, err := identity.Generate("gateway")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_BROKERS", "localhost:9092")
	t.Setenv("HOK_TRUSTED_GATEWAY_KEYS", gatewayPublic)
	t.Setenv("HOK_BRIDGE_SIGNING_KEY", activePrivate)
	t.Setenv("HOK_SERVICE", "payments")
	t.Setenv("HOK_UPSTREAM", "http://localhost:8082")
	for _, name := range []string{"HOK_SPEC_DIR", "HOK_UPSTREAM_CREDENTIALS", "HOK_UPSTREAM_TIMEOUT", "HOK_IDEMPOTENCY_RETENTION", "HOK_SESSION_TIMEOUT"} {
		t.Setenv(name, "")
	}
	for _, tt := range []struct {
		name, value, wantError string
	}{
		{name: "omitted"},
		{name: "active only", value: activePublic},
		{name: "old and active", value: oldPublic + "," + activePublic},
		{name: "malformed", value: "not-a-key", wantError: "HOK_TRUSTED_BRIDGE_KEYS"},
		{name: "invalid public key length", value: "old:" + base64.StdEncoding.EncodeToString([]byte("short")), wantError: "public key must be 32 bytes"},
		{name: "duplicate key id", value: activePublic + "," + activePublic, wantError: "duplicate key id"},
		{name: "missing active", value: oldPublic, wantError: "state keys must trust the active signing key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HOK_TRUSTED_BRIDGE_KEYS", tt.value)
			cfg, err := config(nil)
			if err == nil {
				_, err = bridge.New(cfg)
			}
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("got %v, want %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.StateKeys.IsZero() != (tt.value == "") {
				t.Fatal("configured state keys were not propagated")
			}
		})
	}
}
