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
	for _, route := range []struct{ request, declared, extra string }{
		{http.MethodGet, "get", ""},
		{http.MethodHead, "head", ""},
		{http.MethodHead, "head", "    get: {security: [], responses: {'200': {description: ok}}}\n"},
		{http.MethodHead, "get", ""}, // HEAD inherits GET when no HEAD is declared.
		{http.MethodOptions, "options", ""},
	} {
		for _, security := range []string{"document", "operation"} {
			name := route.request + "_via_" + route.declared
			if route.extra != "" {
				name += "_with_public_GET"
			}
			t.Run(name+"/"+security, func(t *testing.T) {
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
    ` + route.declared + `:
` + operationSecurity + `      responses: {'204': {description: ok}}
` + route.extra

				received := make(chan string, 1)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != route.request {
						t.Errorf("upstream method %s, want %s", r.Method, route.request)
					}
					received <- r.Header.Get("X-Service-Key")
					w.WriteHeader(http.StatusNoContent)
				}))
				defer upstream.Close()
				svc := loadService(t, "secure", []byte(spec), upstream.URL)
				k := keys()
				cfg := gateway.Config{Instance: "gw", Services: []gateway.Service{svc},
					Auth: &gateway.Authenticator{Keys: k.idpRing, Issuer: testIssuer, Audience: testAudience}, Signer: k.gateway, BridgeKeys: k.bridgeTrust,
					Brokers: []string{"127.0.0.1:1"}, Timeout: time.Second, CommandTTL: time.Minute, Partitions: 1}
				if _, err := gateway.New(cfg); err == nil || !strings.Contains(err.Error(), strings.ToUpper(route.declared)+" /private") {
					t.Fatalf("missing unnamed operation credential: %v", err)
				}
				cfg.Services[0].Credentials = apispec.Credentials{"key": "provider-key"}
				g, err := gateway.New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(route.request, "http://secure/private", nil)
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
				if route.extra != "" {
					// Explicit HEAD and implicit GET support must not duplicate
					// HEAD in the advertised methods for a rejected request.
					req := httptest.NewRequest(http.MethodDelete, "http://secure/private", nil)
					req.Header.Set("Authorization", "Bearer "+token(t))
					rec := httptest.NewRecorder()
					g.ServeHTTP(rec, req)
					if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, HEAD" {
						t.Fatalf("status=%d Allow=%q", rec.Code, rec.Header().Get("Allow"))
					}
				}
			})
		}
	}
}
