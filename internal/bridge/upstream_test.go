package bridge

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// flaky executes every POST /effect, then drops the connection without
// answering: the side effect happened, the client sees a broken connection.
func flaky(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var effects atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /warm", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /effect", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		effects.Add(1)
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &effects
}

func command(path string, body []byte, headers wire.Headers) wire.Command {
	now := time.Now()
	if headers == nil {
		headers = wire.Headers{}
	}
	return wire.Command{
		V: wire.Version, RequestID: wire.NewRequestID(), Service: "payments", OperationID: "createCharge",
		Method: http.MethodPost, PathTemplate: path, Path: path,
		Caller:  wire.Caller{Application: "checkout", Instance: "checkout-1"},
		Headers: headers, Body: wire.NewBody(body), IdempotencyKey: "k-1",
		ReplyTo: wire.ReplyTopic("gw-1"), IssuedAt: now, Deadline: now.Add(time.Second), ExpiresAt: now.Add(time.Minute),
	}
}

// The trap this file exists for: Go's transport resends a request whose
// connection broke after it was written, if it deems the request replayable.
// A POST with an Idempotency-Key header and a rewindable body qualifies.
func TestGoTransportSilentlyResendsKeyedPost(t *testing.T) {
	srv, effects := flaky(t)
	cl := srv.Client()
	if _, err := cl.Get(srv.URL + "/warm"); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/effect", bytes.NewReader([]byte(`{"a":1}`)))
	req.Header.Set("Idempotency-Key", "k-1")
	_, err := cl.Do(req)
	if err == nil {
		t.Fatal("expected a broken connection")
	}
	if effects.Load() != 2 {
		t.Fatalf("expected the plain client to resend (2 executions), got %d: the guard rationale changed", effects.Load())
	}
	t.Logf("plain net/http client: 1 request, %d executions on B", effects.Load())
}

func TestUpstreamNeverResendsAndReportsUnknown(t *testing.T) {
	for name, body := range map[string][]byte{"with body": []byte(`{"a":1}`), "empty body": nil} {
		t.Run(name, func(t *testing.T) {
			srv, effects := flaky(t)
			up, err := newUpstream(srv.URL, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := up.client.Get(srv.URL + "/warm"); err != nil {
				t.Fatal(err)
			}
			h := wire.Headers{"idempotency-key": {"k-1"}, "content-type": {"application/json"}}
			resp := up.call(t.Context(), command("/effect", body, h), nil)
			if resp.Fault != wire.FaultOutcomeUnknown {
				t.Fatalf("fault %q, want outcome_unknown: B ran and the connection broke", resp.Fault)
			}
			if n := effects.Load(); n != 1 {
				t.Fatalf("B executed %d times", n)
			}
		})
	}
}

func TestUpstreamUnavailableOnlyWhenNothingWasSent(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // connection refused from now on
	up, _ := newUpstream(url, time.Second)
	resp := up.call(t.Context(), command("/charges", []byte(`{}`), nil), nil)
	if resp.Fault != wire.FaultUpstreamUnavailable || wire.OutcomeOf(resp) != wire.OutcomeNotExecuted {
		t.Fatalf("got %q", resp.Fault)
	}
}

func TestUpstreamTimeoutAfterSendIsUnknown(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	up, _ := newUpstream(srv.URL, 200*time.Millisecond)
	resp := up.call(t.Context(), command("/charges", []byte(`{}`), nil), nil)
	if resp.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("got %q, want outcome_unknown", resp.Fault)
	}
}

func TestUpstreamRequestShape(t *testing.T) {
	var got *http.Request
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(r.Context())
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Location", "/elsewhere")
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	up, _ := newUpstream(srv.URL, time.Second)
	h := wire.Headers{
		"content-type":         {"application/json"},
		"x-caller-application": {"admin"}, // spoof attempt by the caller
		"accept":               {"a", "b"},
	}
	cmd := command("/orders/a%2Fb", []byte(`{"x":1}`), h)
	cmd.RawQuery = "q=1&q=2"
	cmd.TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	resp := up.call(t.Context(), cmd, nil)

	if resp.Status != http.StatusTemporaryRedirect || resp.Headers.Get("location") != "/elsewhere" {
		t.Fatalf("redirect must be relayed, not followed: %+v", resp)
	}
	checks := map[string][2]string{
		"escaped path":     {got.URL.EscapedPath(), "/orders/a%2Fb"},
		"query":            {got.URL.RawQuery, "q=1&q=2"},
		"caller app":       {got.Header.Get(HeaderCallerApplication), "checkout"},
		"caller instance":  {got.Header.Get(HeaderCallerInstance), "checkout-1"},
		"request id":       {got.Header.Get(wire.RequestIDHeader), cmd.RequestID},
		"idempotency key":  {got.Header.Get(wire.IdempotencyKeyHeader), "k-1"},
		"traceparent":      {got.Header.Get("Traceparent"), cmd.TraceParent},
		"no go user agent": {got.Header.Get("User-Agent"), ""},
		"no gzip added":    {got.Header.Get("Accept-Encoding"), ""},
		"body":             {string(gotBody), `{"x":1}`},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s: got %q want %q", name, c[0], c[1])
		}
	}
	if v := got.Header.Values(HeaderCallerApplication); len(v) != 1 {
		t.Errorf("caller header must be replaced, got %v", v)
	}
	if v := got.Header.Values("Accept"); len(v) != 2 {
		t.Errorf("multi-valued header lost: %v", v)
	}
}

func TestUpstreamResponseCutOffIsDoneNotUnknown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"partial":`))
		_ = http.NewResponseController(w).Flush()
		conn, _, _ := http.NewResponseController(w).Hijack()
		conn.Close()
	}))
	defer srv.Close()
	up, _ := newUpstream(srv.URL, time.Second)
	resp := up.call(t.Context(), command("/charges", []byte(`{}`), nil), nil)
	if resp.Fault != wire.FaultResponseIncomplete || resp.UpstreamStatus != 201 || wire.OutcomeOf(resp) != wire.OutcomeSucceeded {
		t.Fatalf("got %+v", resp)
	}
}

const securedSpec = `openapi: 3.0.3
info: {title: secured, version: "1"}
components:
  securitySchemes:
    key: {type: apiKey, in: header, name: X-Api-Key}
    tok: {type: http, scheme: bearer}
paths:
  /charges:
    post:
      operationId: createCharge
      security: [{key: []}, {tok: []}]
      responses: {'201': {description: ok}}
`

// D12: A's credential never reaches B; B gets the provider's own, and a
// mutation whose requirement cannot be met is refused at startup.
func TestUpstreamInjectsProviderCredential(t *testing.T) {
	spec, err := apispec.Load("payments", []byte(securedSpec))
	if err != nil {
		t.Fatal(err)
	}
	op, _ := spec.Operation("createCharge")
	if err := (apispec.Credentials{}).Check(spec, (*apispec.Operation).Transported); err == nil {
		t.Fatal("a mutation requiring a credential must not start without one")
	}
	creds, err := apispec.ParseCredentials("payments", "payments.tok=provider-token,orders.key=other")
	if err != nil {
		t.Fatal(err)
	}
	if err := creds.Check(spec, (*apispec.Operation).Transported); err != nil {
		t.Fatal(err)
	}
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.WriteHeader(201)
	}))
	defer srv.Close()
	up, _ := newUpstream(srv.URL, time.Second)
	up.spec, up.creds = spec, creds
	cmd := command("/charges", []byte(`{}`), wire.Headers{"x-api-key": {"callers-own-key"}, "content-type": {"application/json"}})
	if r := up.call(t.Context(), cmd, op); r.Status != 201 {
		t.Fatalf("%+v", r)
	}
	if got.Get("X-Api-Key") != "" || got.Get("Authorization") != "Bearer provider-token" {
		t.Fatalf("B received X-Api-Key=%q Authorization=%q", got.Get("X-Api-Key"), got.Get("Authorization"))
	}
}
