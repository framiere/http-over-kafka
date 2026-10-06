package gateway_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/gateway"
)

func TestUnnamedPassthroughRequiresAndInjectsCredentials(t *testing.T) {
	for _, security := range []string{"document", "operation"} {
		t.Run(security, func(t *testing.T) {
			documentSecurity, operationSecurity := "security: [{key: []}]\n", ""
			if security == "operation" {
				documentSecurity = ""
				operationSecurity = "      security: [{key: []}]\n"
			}
			spec := `openapi: 3.0.3
info: {title: secure, version: '1'}
components:
  securitySchemes:
    key: {type: apiKey, in: header, name: X-Service-Key}
` + documentSecurity + `paths:
  /private:
    get:
` + operationSecurity + `      responses: {'204': {description: ok}}
`
			received := make(chan string, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received <- r.Header.Get("X-Service-Key")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer upstream.Close()
			svc := loadService(t, "secure", []byte(spec), upstream.URL)
			k := keys()
			cfg := gateway.Config{Instance: "gw", Services: []gateway.Service{svc},
				Auth: &gateway.Authenticator{Keys: k.idpRing, Issuer: testIssuer, Audience: testAudience}, Signer: k.gateway, BridgeKeys: k.bridgeTrust,
				Brokers: []string{"127.0.0.1:1"}, Timeout: time.Second, CommandTTL: time.Minute, Partitions: 1}
			if _, err := gateway.New(cfg); err == nil || !strings.Contains(err.Error(), "GET /private") {
				t.Fatalf("missing unnamed operation credential: %v", err)
			}
			cfg.Services[0].Credentials = apispec.Credentials{"key": "provider-key"}
			g, err := gateway.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, "http://secure/private", nil)
			req.Header.Set("Authorization", "Bearer "+token(t))
			req.Header.Set("X-Service-Key", "callers-key")
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			select {
			case got := <-received:
				if got != "provider-key" {
					t.Fatalf("upstream credential %q", got)
				}
			default:
				t.Fatal("service was not called")
			}
		})
	}
}
