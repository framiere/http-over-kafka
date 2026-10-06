package gateway_test

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/gateway"
)

const pooledCredentialSpec = `openapi: 3.0.3
info: {title: secure, version: '1'}
components:
  securitySchemes:
    key: {type: apiKey, in: query, name: access_token}
security: [{key: []}]
paths:
  /private:
    get: {operationId: readPrivate, responses: {'204': {description: ok}}}
  /public:
    get: {operationId: readPublic, security: [], responses: {'204': {description: ok}}}
`

type passthroughLogCapture chan string

func (c passthroughLogCapture) Write(p []byte) (int, error) { c <- string(p); return len(p), nil }

func credentialGateway(t *testing.T, spec, target string, transport http.RoundTripper) *gateway.Gateway {
	t.Helper()
	svc := loadService(t, "secure", []byte(spec), target)
	svc.Credentials = apispec.Credentials{"key": "provider-secret-204-reflection"}
	k := keys()
	g, err := gateway.New(gateway.Config{Instance: "gw", Services: []gateway.Service{svc},
		Auth: &gateway.Authenticator{Keys: k.idpRing, Issuer: testIssuer, Audience: testAudience}, Signer: k.gateway, BridgeKeys: k.bridgeTrust,
		Brokers: []string{"127.0.0.1:1"}, Timeout: time.Second, CommandTTL: time.Minute, Partitions: 1, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func invokeCredentialGateway(t *testing.T, g *gateway.Gateway, path string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://secure"+path, nil)
	req.Header.Set("Authorization", "Bearer "+token(t))
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestPassthroughMalformedResponseCannotLogCredentials(t *testing.T) {
	for _, mode := range []string{"query", "header", "cookie", "bearer"} {
		t.Run(mode, func(t *testing.T) {
			logs := make(passthroughLogCapture, 32)
			previous := log.Writer()
			log.SetOutput(logs)
			defer log.SetOutput(previous)
			finished := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(finished)
				_, _ = io.Copy(io.Discard, r.Body)
				reflected := r.URL.String() + " " + r.Header.Get("X-Service-Key") + " " + r.Header.Get("Authorization") + " " + r.Header.Get("Cookie")
				if !strings.Contains(reflected, "provider-secret-204-reflection") {
					t.Error("upstream did not receive credential")
				}
				if !r.Close {
					t.Error("credential-bearing request permits connection reuse")
				}
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err := fmt.Fprintf(conn, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\nreflected: %s", reflected); err != nil {
					t.Error(err)
					return
				}
				var b [1]byte
				if _, err := conn.Read(b[:]); err != io.EOF {
					t.Errorf("upstream connection was not closed: %v", err)
				}
			}))
			defer srv.Close()
			scheme := map[string]string{"query": "{type: apiKey, in: query, name: access_token}", "header": "{type: apiKey, in: header, name: X-Service-Key}", "cookie": "{type: apiKey, in: cookie, name: session}", "bearer": "{type: http, scheme: bearer}"}[mode]
			spec := strings.Replace(pooledCredentialSpec, "{type: apiKey, in: query, name: access_token}", scheme, 1)
			invokeCredentialGateway(t, credentialGateway(t, spec, srv.URL, nil), "/private")
			select {
			case <-finished:
			case <-time.After(4 * time.Second):
				t.Fatal("upstream connection still open")
			}
			select {
			case line := <-logs:
				t.Fatalf("unexpected transport log: %s", line)
			default:
			}
		})
	}
}

func TestPassthroughPoolsOnlyAnonymousRequests(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%t", h2), func(t *testing.T) {
			type observed struct {
				address string
				proto   int
			}
			received := make(chan observed, 4)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received <- observed{r.RemoteAddr, r.ProtoMajor}
				w.WriteHeader(http.StatusNoContent)
			}))
			srv.EnableHTTP2 = h2
			srv.StartTLS()
			defer srv.Close()
			tr := srv.Client().Transport.(*http.Transport).Clone()
			defer tr.CloseIdleConnections()
			g := credentialGateway(t, pooledCredentialSpec, srv.URL, tr)
			call := func(path string) observed {
				t.Helper()
				invokeCredentialGateway(t, g, path)
				got := <-received
				wantProto := 1
				if h2 {
					wantProto = 2
				}
				if got.proto != wantProto {
					t.Fatalf("HTTP/%d, want HTTP/%d", got.proto, wantProto)
				}
				return got
			}
			first := call("/public")
			second := call("/public")
			if first.address != second.address {
				t.Fatal("anonymous requests lost connection pooling")
			}
			private := call("/private")
			next := call("/public")
			if private.address == next.address {
				t.Fatal("credential-bearing connection was reused")
			}
		})
	}
}
