package bridge

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

type transportLogCapture chan string

func (c transportLogCapture) Write(p []byte) (int, error) { c <- string(p); return len(p), nil }

// A real transport must consume the malformed response: a fake RoundTripper
// never enters the idle-connection read loop that logs the reflected secret.
func TestUpstreamMalformedResponseCannotLogCredentials(t *testing.T) {
	const secret = "provider-secret-204-reflection"
	for _, mode := range []string{"query", "header", "cookie", "bearer", "userinfo", "authorization"} {
		t.Run(mode, func(t *testing.T) {
			logs := make(transportLogCapture, 32)
			previous := log.Writer()
			log.SetOutput(logs)
			defer log.SetOutput(previous)
			finished := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(finished)
				_, _ = io.Copy(io.Discard, r.Body)
				reflected := r.URL.String() + " " + r.Header.Get("X-Service-Key") + " " + r.Header.Get("Authorization") + " " + r.Header.Get("Cookie")
				if mode == "userinfo" {
					_, password, _ := r.BasicAuth()
					reflected += " " + password
				}
				if !strings.Contains(reflected, secret) {
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
				// Wait for the transport to close its read loop, including any log write.
				var b [1]byte
				if _, err := conn.Read(b[:]); err != io.EOF {
					t.Errorf("upstream connection was not closed: %v", err)
				}
			}))
			defer srv.Close()
			target := srv.URL
			if mode == "userinfo" {
				u, _ := url.Parse(target)
				u.User = url.UserPassword("provider-user", secret)
				target = u.String()
			}
			up, err := newUpstream(target, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer up.client.CloseIdleConnections()
			var op *apispec.Operation
			headers := wire.Headers{}
			if mode == "authorization" {
				headers["authorization"] = []string{"Bearer " + secret}
			}
			if mode != "userinfo" && mode != "authorization" {
				scheme := map[string]string{"query": "{type: apiKey, in: query, name: access_token}", "header": "{type: apiKey, in: header, name: X-Service-Key}", "cookie": "{type: apiKey, in: cookie, name: session}", "bearer": "{type: http, scheme: bearer}"}[mode]
				src := strings.Replace(queryCredentialSpec, "{type: apiKey, in: query, name: access_token}", scheme, 1)
				up.spec, err = apispec.Load("payments", []byte(src))
				if err != nil {
					t.Fatal(err)
				}
				up.creds = apispec.Credentials{"key": secret}
				op, _ = up.spec.Operation("createCharge")
			}
			resp := up.call(t.Context(), command("/charges", []byte(`{}`), headers), op)
			if resp.Status != http.StatusNoContent || resp.Fault != "" || len(resp.Body.Bytes()) != 0 {
				t.Fatalf("response %+v", resp)
			}
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

func TestUpstreamPoolsOnlyAnonymousRequests(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%t", h2), func(t *testing.T) {
			type observed struct {
				address string
				proto   int
			}
			received := make(chan observed, 4)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				received <- observed{r.RemoteAddr, r.ProtoMajor}
				w.WriteHeader(http.StatusNoContent)
			}))
			srv.EnableHTTP2 = h2
			srv.StartTLS()
			defer srv.Close()
			up, err := newUpstream(srv.URL, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			tr := up.client.Transport.(*http.Transport)
			tr.TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
			defer tr.CloseIdleConnections()
			up.spec, err = apispec.Load("payments", []byte(queryCredentialSpec+`  /public:
    post:
      operationId: publicCharge
      security: []
      responses: {'204': {description: ok}}
`))
			if err != nil {
				t.Fatal(err)
			}
			up.creds = apispec.Credentials{"key": "private-key"}
			call := func(path, id string) observed {
				t.Helper()
				op, _ := up.spec.Operation(id)
				resp := up.call(t.Context(), command(path, []byte(`{}`), nil), op)
				if resp.Status != http.StatusNoContent || resp.Fault != "" {
					t.Fatalf("response %+v", resp)
				}
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
			first := call("/public", "publicCharge")
			second := call("/public", "publicCharge")
			if first.address != second.address {
				t.Fatal("anonymous requests lost connection pooling")
			}
			private := call("/charges", "createCharge")
			next := call("/public", "publicCharge")
			if private.address == next.address {
				t.Fatal("credential-bearing connection was reused")
			}
		})
	}
}
