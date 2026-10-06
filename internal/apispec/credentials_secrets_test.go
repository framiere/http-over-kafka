package apispec_test

import (
	"strings"
	"testing"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
)

func TestCredentialConfigurationErrorsOmitInput(t *testing.T) {
	for _, input := range []string{
		"startup-secret",
		"payments.startup-secret",
		"startup-secret=value",
		"payments.=startup-secret",
		"payments.startup-secret=",
	} {
		_, err := apispec.ParseCredentials("payments", "payments.key=valid-value,"+input)
		if err == nil || !strings.Contains(err.Error(), "credential entry 2:") {
			t.Fatalf("missing entry context: %v", err)
		}
		if strings.Contains(err.Error(), "startup-secret") || strings.Contains(err.Error(), "valid-value") {
			t.Errorf("credential input exposed: %v", err)
		}
	}
	svc := mustLoad(t, "payments", []byte(header+`
  /public:
    get: {operationId: read, responses: {'200': {description: ok}}}
`))
	creds, err := apispec.ParseCredentials("payments", "payments.startup-secret=provider-value")
	if err != nil {
		t.Fatal(err)
	}
	err = creds.Check(svc, func(*apispec.Operation) bool { return true })
	if err == nil || strings.Contains(err.Error(), "startup-secret") || strings.Contains(err.Error(), "provider-value") {
		t.Fatalf("unknown scheme exposes credential input: %v", err)
	}
	if !strings.Contains(err.Error(), `service "payments"`) {
		t.Fatalf("service context lost: %v", err)
	}
}
