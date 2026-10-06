package gateway_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/gateway"
)

type failingTransport func(*http.Request) (*http.Response, error)

func (f failingTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type secretReadFailure struct{ err error }

func (s secretReadFailure) Read([]byte) (int, error) { return 0, s.err }
func (s secretReadFailure) Close() error             { return nil }

func TestPassthroughErrorsDoNotLogCredentials(t *testing.T) {
	for _, tc := range []struct {
		name        string
		bodyFailure bool
		status      int
		diagnostic  string
	}{
		{"transport", false, http.StatusServiceUnavailable, "upstream HTTP exchange failed"},
		{"response body", true, http.StatusOK, "passthrough response forwarding failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const spec = `openapi: 3.0.3
info: {title: secured, version: '1'}
components:
  securitySchemes:
    key: {type: apiKey, in: query, name: access_token}
security: [{key: []}]
paths:
  /private:
    get: {operationId: readPrivate, responses: {'200': {description: ok}}}
`
			svc := loadService(t, "secure", []byte(spec), "http://provider.invalid")
			svc.Credentials = apispec.Credentials{"key": "provider-secret+/=&value"}
			var logs, standardLogs bytes.Buffer
			originalLogOutput := log.Writer()
			log.SetOutput(&standardLogs)
			defer log.SetOutput(originalLogOutput)
			called := false
			k := keys()
			g, err := gateway.New(gateway.Config{Instance: "gw", Services: []gateway.Service{svc},
				Auth: &gateway.Authenticator{Keys: k.idpRing, Issuer: testIssuer, Audience: testAudience}, Signer: k.gateway, BridgeKeys: k.bridgeTrust,
				Brokers: []string{"127.0.0.1:1"}, Timeout: time.Second, CommandTTL: time.Minute, Partitions: 1,
				Logger: slog.New(slog.NewTextHandler(&logs, nil)),
				Transport: failingTransport(func(r *http.Request) (*http.Response, error) {
					called = true
					if r.URL.Query().Get("access_token") != svc.Credentials["key"] {
						t.Error("provider credential was not injected")
					}
					err := fmt.Errorf("transport failed at %s", r.URL.String())
					if tc.bodyFailure {
						return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Request: r,
							Body: io.NopCloser(io.MultiReader(strings.NewReader("partial response"), secretReadFailure{err}))}, nil
					}
					return nil, err
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, "http://secure/private", nil)
			req.Header.Set("Authorization", "Bearer "+token(t))
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, req)
			if rec.Code != tc.status || !called {
				t.Fatalf("status %d called=%t", rec.Code, called)
			}
			for _, text := range []string{logs.String(), standardLogs.String(), rec.Body.String()} {
				for _, forbidden := range []string{"provider-secret", "access_token", "provider.invalid"} {
					if strings.Contains(text, forbidden) {
						t.Errorf("error exposes request data: %s", text)
					}
				}
			}
			if !strings.Contains(logs.String(), tc.diagnostic) {
				t.Fatalf("safe diagnostic missing: %s", logs.String())
			}
			if !strings.Contains(logs.String(), "service=secure") {
				t.Fatalf("service missing from diagnostic: %s", logs.String())
			}
		})
	}
}
