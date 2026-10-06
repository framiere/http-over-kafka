package main

import (
	"log/slog"
	"strings"
	"testing"
)

func TestServiceConfigurationErrorsOmitSecrets(t *testing.T) {
	const secret = "startup-secret"
	for _, entry := range []string{
		"http://user:" + secret + "@127.0.0.1",
		"http://user:" + secret + "@127.0.0.1=http://127.0.0.1",
		secret + "=http://127.0.0.1",
		"payments=http://user:" + secret + "@127.0.0.1/\n",
		"payments=http://127.0.0.1/%zz?access_token=" + secret,
	} {
		_, err := service(entry)
		if err == nil {
			t.Fatalf("invalid entry accepted: %q", entry)
		}
		if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "127.0.0.1") {
			t.Errorf("configuration error exposes input: %v", err)
		}
	}
	t.Setenv("HOK_SPEC_DIR", t.TempDir())
	if _, err := service(secret + "=http://127.0.0.1"); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("missing spec error exposes malformed service name: %v", err)
	}
}

func TestServiceConfigurationKeepsValidUserinfo(t *testing.T) {
	t.Setenv("HOK_SPEC_DIR", "")
	svc, err := service("payments=http://startup-user:startup-secret@127.0.0.1/base")
	if err != nil {
		t.Fatal(err)
	}
	password, _ := svc.Upstream.User.Password()
	if svc.Spec.Name() != "payments" || password != "startup-secret" {
		t.Fatal("valid configuration changed")
	}
}

func TestGatewayConfigurationReportsEntryIndexWithoutInput(t *testing.T) {
	for _, name := range []string{"HOK_UPSTREAM_CREDENTIALS", "HOK_JWT_KEYS", "HOK_GATEWAY_SIGNING_KEY", "HOK_TRUSTED_BRIDGE_KEYS"} {
		t.Setenv(name, "")
	}
	t.Setenv("HOK_SPEC_DIR", "")
	t.Setenv("HOK_SERVICES", "payments=http://127.0.0.1, http://user:startup-secret@127.0.0.1")
	_, err := config(slog.Default())
	if err == nil || !strings.Contains(err.Error(), "HOK_SERVICES entry 2:") {
		t.Fatalf("missing entry context: %v", err)
	}
	if strings.Contains(err.Error(), "startup-secret") {
		t.Fatalf("configuration exposes secret: %v", err)
	}
}
