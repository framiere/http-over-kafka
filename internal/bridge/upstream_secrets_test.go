package bridge

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

const queryCredentialSpec = `openapi: 3.0.3
info: {title: payments, version: '1'}
components:
  securitySchemes:
    key: {type: apiKey, in: query, name: access_token}
security: [{key: []}]
paths:
  /charges:
    post:
      operationId: createCharge
      responses: {'201': {description: ok}}
`

func TestUpstreamErrorsDoNotExposeProviderQuery(t *testing.T) {
	spec, err := apispec.Load("payments", []byte(queryCredentialSpec))
	if err != nil {
		t.Fatal(err)
	}
	op, _ := spec.Operation("createCharge")
	const credential = "provider-secret+/=&value"
	for _, mode := range []string{"dial", "lost", "incomplete", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			received := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				received <- r.URL.Query().Get("access_token")
				if mode == "timeout" {
					<-r.Context().Done()
					return
				}
				if mode == "incomplete" {
					w.Header().Set("Content-Length", "100")
					w.WriteHeader(201)
					_, _ = w.Write([]byte("partial"))
					_ = http.NewResponseController(w).Flush()
				}
				conn, _, err := http.NewResponseController(w).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				conn.Close()
			}))
			defer srv.Close()
			timeout := time.Second
			if mode == "dial" {
				srv.Close()
			}
			if mode == "timeout" {
				timeout = 100 * time.Millisecond
			}
			up, err := newUpstream(srv.URL, timeout)
			if err != nil {
				t.Fatal(err)
			}
			up.spec, up.creds = spec, apispec.Credentials{"key": credential}
			resp := up.call(t.Context(), command("/charges", []byte(`{}`), nil), op)
			want := wire.FaultOutcomeUnknown
			if mode == "dial" {
				want = wire.FaultUpstreamUnavailable
			}
			if mode == "incomplete" {
				want = wire.FaultResponseIncomplete
			}
			if resp.Fault != want {
				t.Fatalf("fault %s, want %s", resp.Fault, want)
			}
			if mode != "dial" {
				select {
				case got := <-received:
					if got != credential {
						t.Fatal("provider credential was not injected")
					}
				default:
					t.Fatal("upstream was not called")
				}
			}
			for _, forbidden := range []string{"provider-secret", "access_token", srv.URL} {
				if strings.Contains(string(resp.Body.Bytes()), forbidden) {
					t.Errorf("transport error exposes request data %q: %s", forbidden, resp.Body.Bytes())
				}
			}
		})
	}
}
