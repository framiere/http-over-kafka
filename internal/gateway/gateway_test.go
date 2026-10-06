package gateway_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/demo/orders"
	"github.com/sderosiaux/http-over-kafka/internal/devidp"
	"github.com/sderosiaux/http-over-kafka/internal/gateway"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func problemOf(t testing.TB, resp *http.Response, body []byte, status int, typ string) wire.Problem {
	t.Helper()
	if resp.StatusCode != status {
		t.Fatalf("status %d, want %d: %s", resp.StatusCode, status, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != wire.ProblemContentType {
		t.Fatalf("content-type %q: %s", ct, body)
	}
	var p wire.Problem
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Type != typ || p.Status != status || p.RequestID == "" || p.RequestID != resp.Header.Get(wire.RequestIDHeader) {
		t.Fatalf("problem %+v, header requestId %q", p, resp.Header.Get(wire.RequestIDHeader))
	}
	return p
}

// Criterion 1 on the real demo service: what A gets through the gateway is
// what B produced, byte for byte, with the same significant headers a
// direct call returns.
func TestCreateOrderMatchesDirectCall(t *testing.T) {
	name := kafkatest.Service(t, "orders")
	b := httptest.NewServer(orders.New().Handler())
	defer b.Close()
	g := startGateway(t, gwOpts{}, loadService(t, name, api.Orders, b.URL))
	g.waitReady(t)
	p := startPeer(t, name, b.URL, nil)

	body := []byte(`{"customerId":"c1","items":[{"sku":"A","quantity":2}]}`)
	direct, directBody := do(t, http.MethodPost, b.URL+"/orders", "application/json", body)
	via, viaBody := call(t, g, name, http.MethodPost, "/orders", "application/json", body)

	if via.StatusCode != direct.StatusCode || via.StatusCode != http.StatusCreated {
		t.Fatalf("status via=%d direct=%d: %s", via.StatusCode, direct.StatusCode, viaBody)
	}
	requestID := via.Header.Get(wire.RequestIDHeader)
	fromB, ok := p.bodyFromB(requestID)
	if !ok || !bytes.Equal(fromB, viaBody) {
		t.Fatalf("body not byte-identical to B's:\nB:   %q\nvia: %q", fromB, viaBody)
	}
	for _, h := range []string{"Content-Type", "Location", "ETag", "Content-Length", "Date"} {
		if (direct.Header.Get(h) == "") != (via.Header.Get(h) == "") {
			t.Errorf("header %s: direct %q, via %q", h, direct.Header.Get(h), via.Header.Get(h))
		}
	}
	if via.Header.Get("Content-Type") != direct.Header.Get("Content-Type") {
		t.Errorf("content-type direct %q via %q", direct.Header.Get("Content-Type"), via.Header.Get("Content-Type"))
	}
	var o orders.Order
	if err := json.Unmarshal(viaBody, &o); err != nil || via.Header.Get("Location") != "/orders/"+o.ID {
		t.Fatalf("location %q for %s", via.Header.Get("Location"), viaBody)
	}
	if !bytes.Equal(normalizeOrder(t, directBody), normalizeOrder(t, viaBody)) {
		t.Fatalf("shape differs:\ndirect %s\nvia    %s", directBody, viaBody)
	}

	// A 4xx from B is B's answer, relayed as is.
	for _, c := range []struct {
		ct, body string
		status   int
	}{
		{"application/json", `{"customerId":"","items":[]}`, http.StatusBadRequest},
		{"text/plain", `not json at all`, http.StatusUnsupportedMediaType},
		{"application/json", ``, http.StatusBadRequest},
	} {
		d, dBody := do(t, http.MethodPost, b.URL+"/orders", c.ct, []byte(c.body))
		v, vBody := call(t, g, name, http.MethodPost, "/orders", c.ct, []byte(c.body))
		if d.StatusCode != c.status || v.StatusCode != c.status || !bytes.Equal(dBody, vBody) ||
			v.Header.Get("Content-Type") != d.Header.Get("Content-Type") {
			t.Errorf("%s %q: direct %d %s, via %d %s", c.ct, c.body, d.StatusCode, dBody, v.StatusCode, vBody)
		}
	}
}

func normalizeOrder(t testing.TB, b []byte) []byte {
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	m["id"], m["createdAt"] = "", ""
	out, _ := json.Marshal(m)
	return out
}

func do(t testing.TB, method, url, ct string, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, url, bytes.NewReader(body))
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

// Bodies and paths that a re-serializing or normalizing proxy would alter.
func TestTransportIsByteExact(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, rec := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	startPeer(t, name, b.URL, nil)

	binary := make([]byte, 64<<10)
	_, _ = rand.Read(binary)
	big := bytes.Repeat([]byte("x"), wire.MaxBodyBytes)
	cases := []struct {
		name, method, target, ct string
		body                     []byte
		status                   int
	}{
		{"pretty json kept pretty", "POST", "/echo/1", "application/json", []byte("{\n  \"a\" : 1,\"html\":\"<b>&</b>\"\n}"), 200},
		{"binary", "POST", "/echo/2", "application/octet-stream", binary, 200},
		{"empty body", "POST", "/echo/3", "", nil, 200},
		{"invalid utf8 text", "PATCH", "/echo/4", "text/plain", []byte{0xff, 0xfe, 'a'}, 200},
		{"encoded slash stays one segment", "PUT", "/echo/a%2Fb", "text/plain", []byte("x"), 200},
		{"query kept raw", "POST", "/echo/5?b=2&a=1&a=%20&status=201", "text/plain", []byte("q"), 201},
		{"max size body", "POST", "/echo/6", "application/octet-stream", big, 200},
		{"delete 204", "DELETE", "/echo/7", "", nil, 204},
		{"B 500 relayed", "POST", "/echo/8?status=500", "text/plain", []byte("boom"), 500},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, body := call(t, g, name, c.method, c.target, c.ct, c.body, "X-Multi", "one", "X-Multi", "two")
			if resp.StatusCode != c.status {
				t.Fatalf("status %d want %d: %.200s", resp.StatusCode, c.status, body)
			}
			if c.status != 204 && !bytes.Equal(body, c.body) {
				t.Fatalf("body altered: sent %d bytes, got %d (%.80q)", len(c.body), len(body), body)
			}
			if got := resp.Header.Get("X-Echo-Uri"); got != c.target {
				t.Fatalf("B received %q, A sent %q", got, c.target)
			}
			if got := resp.Header.Values("Set-Cookie"); !slices.Equal(got, []string{"a=1", "b=2"}) {
				t.Fatalf("set-cookie %q", got)
			}
			if resp.Header.Get("Content-Type") != c.ct && c.ct != "" {
				t.Fatalf("content-type %q", resp.Header.Get("Content-Type"))
			}
			if _, err := ulid.ParseStrict(resp.Header.Get(wire.RequestIDHeader)); err != nil {
				t.Fatalf("X-Request-Id %q is not the gateway's", resp.Header.Get(wire.RequestIDHeader))
			}
		})
	}
	for _, r := range rec.all() {
		if !slices.Equal(r.Header.Values("X-Multi"), []string{"one", "two"}) {
			t.Errorf("%s %s: B got X-Multi %q", r.Method, r.RequestURI, r.Header.Values("X-Multi"))
		}
		if r.Header.Get("Authorization") != "" {
			t.Errorf("B received the caller's Authorization")
		}
	}
}

// Requests the gateway refuses itself never reach Kafka.
func TestGatewayRefusals(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	p := startPeer(t, name, b.URL, nil)

	resp, body := call(t, g, name, "POST", "/echo/1", "application/octet-stream", make([]byte, wire.MaxBodyBytes+1))
	problemOf(t, resp, body, 413, gateway.ProblemPayloadTooLarge)

	resp, body = call(t, g, name, "POST", "/nope", "", nil)
	problemOf(t, resp, body, 404, gateway.ProblemNoOperation)

	for _, path := range []string{"/echo/1/", "/echo/../echo/1", "/echo/%2E%2E", "//echo/1"} {
		resp, body = call(t, g, name, "POST", path, "", nil)
		problemOf(t, resp, body, 404, gateway.ProblemNoOperation)
	}

	resp, body = call(t, g, "unknown-svc.internal", "POST", "/echo/1", "", nil)
	problemOf(t, resp, body, 404, gateway.ProblemUnknownService)

	resp, body = call(t, g, name+".internal:443", "POST", "/echo/1?status=201", "", nil)
	if resp.StatusCode != 201 {
		t.Fatalf("host with domain and port: %d %s", resp.StatusCode, body)
	}

	resp, body = call(t, g, name, "OPTIONS", "/echo/1", "", nil)
	problemOf(t, resp, body, 405, gateway.ProblemMethodNotAllowed)
	if got := resp.Header.Get("Allow"); got != "DELETE, GET, HEAD, PATCH, POST, PUT" {
		t.Fatalf("Allow %q", got)
	}

	resp, body = call(t, g, name, "POST", "/echo/1", "", nil, wire.IdempotencyKeyHeader, strings.Repeat("k", 256))
	problemOf(t, resp, body, 400, gateway.ProblemInvalidRequest)
	resp, body = call(t, g, name, "POST", "/echo/1", "", nil, wire.IdempotencyKeyHeader, "k1", wire.IdempotencyKeyHeader, "k2")
	problemOf(t, resp, body, 400, gateway.ProblemInvalidRequest)

	if n := len(p.records()); n != 1 {
		t.Fatalf("%d commands reached Kafka, want only the valid one", n)
	}
}

func TestAuthentication(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	p := startPeer(t, name, b.URL, nil)

	_, otherKey, _ := ed25519.GenerateKey(nil)
	cases := []struct {
		name, authz, code string
	}{
		{"no header", "", ""},
		{"not bearer", "Basic dXNlcjpwYXNz", "invalid_request"},
		{"garbage", "Bearer not.a.jwt", "invalid_token"},
		{"expired", "Bearer " + token(t, func(tk *devidp.Token) { tk.IssuedAt = time.Now().Add(-2 * time.Hour) }), "invalid_token"},
		{"wrong audience", "Bearer " + token(t, func(tk *devidp.Token) { tk.Audience = "someone-else" }), "invalid_token"},
		{"wrong issuer", "Bearer " + token(t, func(tk *devidp.Token) { tk.Issuer = "evil" }), "invalid_token"},
		{"unknown kid", "Bearer " + token(t, func(tk *devidp.Token) { tk.KeyID = "nope" }), "invalid_token"},
		{"forged signature", "Bearer " + token(t, func(tk *devidp.Token) { tk.Key = otherKey }), "invalid_token"},
		{"no instance", "Bearer " + token(t, func(tk *devidp.Token) { tk.Instance = "" }), "invalid_token"},
		{"alg none", "Bearer " + unsignedToken(), "invalid_token"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, method := range []string{"POST", "GET"} {
				resp, body := call(t, g, name, method, "/echo/1", "", nil, "Authorization", "", "Authorization", c.authz)
				problemOf(t, resp, body, 401, gateway.ProblemUnauthorized)
				chal := resp.Header.Get("WWW-Authenticate")
				if !strings.HasPrefix(chal, "Bearer ") || (c.code != "") != strings.Contains(chal, `error="`+c.code+`"`) {
					t.Fatalf("challenge %q", chal)
				}
			}
		})
	}

	// D5: the token never enters Kafka, the derived caller does.
	tok := token(t)
	resp, body := call(t, g, name, "POST", "/echo/1", "", nil, "Authorization", "", "Authorization", "Bearer "+tok,
		"Cookie", "session=secret-cookie", "Proxy-Authorization", "Basic c2VjcmV0")
	if resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	recs := p.records()
	if len(recs) != 1 {
		t.Fatalf("%d commands", len(recs))
	}
	for _, secret := range []string{tok, "secret-cookie", "c2VjcmV0"} {
		if bytes.Contains(recs[0].Value, []byte(secret)) {
			t.Fatalf("caller secret in command: %s", recs[0].Value)
		}
	}
	cmd, err := wire.DecodeCommandUnverified(recs[0])
	if err != nil || cmd.Caller != (wire.Caller{Application: "checkout", Instance: "checkout-1"}) {
		t.Fatalf("caller %+v %v", cmd.Caller, err)
	}
}

// A client-chosen X-Request-Id is never the correlation id.
func TestRequestIDIsMintedByGateway(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	p := startPeer(t, name, b.URL, nil)

	forged := ulid.Make().String()
	resp, _ := call(t, g, name, "POST", "/echo/1", "", nil, wire.RequestIDHeader, forged)
	got := resp.Header.Get(wire.RequestIDHeader)
	if got == forged || got == "set-by-b" {
		t.Fatalf("X-Request-Id %q not minted by the gateway", got)
	}
	cmd, err := wire.DecodeCommandUnverified(p.records()[0])
	if err != nil || cmd.RequestID != got {
		t.Fatalf("command requestId %q, response %q (%v)", cmd.RequestID, got, err)
	}
}

// D4: the caller gets 504 + requestId at its deadline, the command still
// runs, and the late response is dropped without leaking a waiter.
func TestTimeoutAndLateResponse(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, rec := echoB(t)
	g := startGateway(t, gwOpts{timeout: 300 * time.Millisecond}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	release := make(chan struct{})
	var seen sync.WaitGroup
	seen.Add(1)
	p := startPeer(t, name, b.URL, func(cmd wire.Command) (wire.Response, bool) {
		seen.Done()
		<-release
		return wire.Response{}, false // then call B normally
	})

	resp, body := call(t, g, name, "POST", "/echo/1", "text/plain", []byte("late"), wire.IdempotencyKeyHeader, "k-1")
	pb := problemOf(t, resp, body, 504, wire.ProblemTypeTimeout)
	if !strings.Contains(pb.Detail, "Idempotency-Key") {
		t.Fatalf("504 detail does not tell how to recover: %q", pb.Detail)
	}
	seen.Wait()
	if g.Pending() != 0 {
		t.Fatalf("%d waiters leaked after 504", g.Pending())
	}
	close(release)
	kafkatest.Eventually(t, 10*time.Second, func() bool { return g.DroppedReplies() == 1 }, "late reply dropped")
	if _, ok := p.bodyFromB(pb.RequestID); !ok || len(rec.all()) != 1 || g.Pending() != 0 {
		t.Fatalf("B calls %d, pending %d", len(rec.all()), g.Pending())
	}
}

// A caller hanging up stops the wait, not the command.
func TestClientDisconnect(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, rec := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	release := make(chan struct{})
	got := make(chan string, 1)
	startPeer(t, name, b.URL, func(cmd wire.Command) (wire.Response, bool) {
		got <- cmd.RequestID
		<-release
		return wire.Response{}, false
	})

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", g.srv.URL+"/echo/1", strings.NewReader("bye"))
	req.Host = name
	req.Header.Set("Authorization", "Bearer "+token(t))
	errc := make(chan error, 1)
	go func() { _, err := g.srv.Client().Do(req); errc <- err }()
	<-got
	cancel()
	if err := <-errc; err == nil {
		t.Fatal("request should have been cancelled")
	}
	kafkatest.Eventually(t, 5*time.Second, func() bool { return g.Pending() == 0 }, "waiter removed after disconnect")
	close(release)
	kafkatest.Eventually(t, 10*time.Second, func() bool { return len(rec.all()) == 1 }, "command executed after disconnect")
}

// D6/D11: two instances, concurrent load, every response reaches the
// connection that sent the request.
func TestConcurrentTwoInstancesNoCrossDelivery(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	svc := loadService(t, name, []byte(echoSpec), b.URL)
	g1 := startGateway(t, gwOpts{}, svc)
	g2 := startGateway(t, gwOpts{}, svc)
	g1.waitReady(t)
	g2.waitReady(t)
	startPeer(t, name, b.URL, nil)

	const n = 400
	tok := token(t)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := g1
			if i%2 == 1 {
				g = g2
			}
			want := fmt.Sprintf("payload-%d-%s", i, ulid.Make())
			req, _ := http.NewRequest("POST", g.srv.URL+"/echo/"+fmt.Sprint(i), strings.NewReader(want))
			req.Host = name
			req.Header.Set("Authorization", "Bearer "+tok)
			resp, err := g.srv.Client().Do(req)
			if err != nil {
				errs <- err
				return
			}
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || string(got) != want || resp.Header.Get("X-Echo-Uri") != "/echo/"+fmt.Sprint(i) {
				errs <- fmt.Errorf("request %d: %d %q", i, resp.StatusCode, got)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if g1.Pending()+g2.Pending() != 0 {
		t.Fatalf("waiters leaked: %d %d", g1.Pending(), g2.Pending())
	}
}

// Restart with the same instance id while a request is in flight: the
// connection dies with the process, the command still executes, its late
// reply reaches the new process and is dropped, never handed to a new
// connection; new requests work on the reused reply topic.
func TestRestartWithRequestInFlight(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	instance := kafkatest.Service(t, "gw")
	b, rec := echoB(t)
	svc := loadService(t, name, []byte(echoSpec), b.URL)
	g := startGateway(t, gwOpts{instance: instance}, svc)
	g.waitReady(t)

	held := make(chan string, 1)
	release := make(chan struct{})
	startPeer(t, name, b.URL, func(cmd wire.Command) (wire.Response, bool) {
		if string(cmd.Body.Bytes()) == "in-flight" {
			held <- cmd.RequestID
			<-release
		}
		return wire.Response{}, false
	})

	errc := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest("POST", g.srv.URL+"/echo/1", strings.NewReader("in-flight"))
		req.Host = name
		req.Header.Set("Authorization", "Bearer "+token(t))
		resp, err := g.srv.Client().Do(req)
		if err == nil {
			resp.Body.Close()
			err = fmt.Errorf("in-flight request answered %d by a stopped gateway", resp.StatusCode)
		}
		errc <- err
	}()
	inFlight := <-held
	g.srv.CloseClientConnections() // the process dies: its connections with it
	g.stop()
	if err := <-errc; err == nil || strings.Contains(err.Error(), "answered") {
		t.Fatalf("in-flight caller: %v", err)
	}

	g = startGateway(t, gwOpts{instance: instance}, svc)
	g.waitReady(t)
	close(release) // the command completes, its reply lands after the restart
	kafkatest.Eventually(t, 10*time.Second, func() bool { return g.DroppedReplies() == 1 }, "late reply of %s dropped by the new process", inFlight)

	resp, body := call(t, g, name, "POST", "/echo/2", "text/plain", []byte("fresh"))
	if resp.StatusCode != 200 || string(body) != "fresh" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if g.Pending() != 0 || g.DroppedReplies() != 1 || len(rec.all()) != 2 {
		t.Fatalf("pending %d, dropped %d, B calls %d", g.Pending(), g.DroppedReplies(), len(rec.all()))
	}
}

// Bridge-produced outcomes (faults, replays) reach A as the bridge wrote them.
func TestBridgeFaultsAndReplays(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	original := ulid.Make().String()
	startPeer(t, name, b.URL, func(cmd wire.Command) (wire.Response, bool) {
		switch cmd.PathParams["id"] {
		case "unknown":
			return wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown, "bridge crashed after calling B"), true
		case "reused":
			return wire.FaultResponse(cmd.RequestID, wire.FaultIdempotencyKeyReused, ""), true
		case "replay":
			return wire.Response{V: wire.Version, RequestID: cmd.RequestID, Status: 201, ReplayOf: original,
				Headers: wire.Headers{"content-type": {"application/json"}}, Body: wire.NewBody([]byte(`{"id":"x"}`))}, true
		}
		return wire.Response{}, false
	})

	resp, body := call(t, g, name, "POST", "/echo/unknown", "", nil)
	if p := problemOf(t, resp, body, 502, wire.ProblemType(wire.FaultOutcomeUnknown)); p.Detail != "bridge crashed after calling B" {
		t.Fatalf("%+v", p)
	}
	resp, body = call(t, g, name, "POST", "/echo/reused", "", nil)
	problemOf(t, resp, body, 422, wire.ProblemType(wire.FaultIdempotencyKeyReused))

	resp, body = call(t, g, name, "POST", "/echo/replay", "", nil)
	if resp.StatusCode != 201 || string(body) != `{"id":"x"}` || resp.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("%d %v %s", resp.StatusCode, resp.Header, body)
	}
	if resp.Header.Get(wire.RequestIDHeader) == original {
		t.Fatal("replay must carry the new requestId")
	}
}

// D8 + criterion 5: GET (and HEAD) go straight to B and write nothing to Kafka.
func TestGetIsPassthroughAndWritesNothing(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, rec := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)

	before := kafkatest.EndOffsets(t)
	resp, body := call(t, g, name, "GET", "/echo/a%2Fb?x=%20y", "", nil, "Cookie", "s=1", "X-Multi", "one", "X-Multi", "two",
		wire.CallerApplicationHeader, "forged")
	firstID := resp.Header.Get(wire.RequestIDHeader)
	if resp.StatusCode != 200 || resp.Header.Get("X-Echo-Uri") != "/echo/a%2Fb?x=%20y" || len(body) != 0 {
		t.Fatalf("%d %v %q", resp.StatusCode, resp.Header, body)
	}
	if _, err := ulid.ParseStrict(resp.Header.Get(wire.RequestIDHeader)); err != nil {
		t.Fatalf("X-Request-Id %q", resp.Header.Get(wire.RequestIDHeader))
	}
	resp, _ = call(t, g, name, "HEAD", "/echo/1", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("HEAD %d", resp.StatusCode)
	}
	if after := kafkatest.EndOffsets(t); after != before {
		t.Fatalf("GET wrote %d records to Kafka", after-before)
	}
	r := rec.all()[0]
	if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || !slices.Equal(r.Header.Values("X-Multi"), []string{"one", "two"}) {
		t.Fatalf("B got headers %v", r.Header)
	}
	// Same identity headers the bridge sets on mutations; caller-sent values replaced.
	if r.Header.Get(wire.CallerApplicationHeader) != "checkout" || r.Header.Get(wire.CallerInstanceHeader) != "checkout-1" ||
		r.Header.Get(wire.RequestIDHeader) != firstID || len(r.Header.Values(wire.CallerApplicationHeader)) != 1 {
		t.Fatalf("B got identity %v", r.Header)
	}
}

// Kafka unreachable: mutations are refused explicitly as not applied, reads
// keep working since they never needed Kafka.
func TestKafkaUnavailable(t *testing.T) {
	kafkatest.Brokers(t) // honours -short
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{brokers: []string{"127.0.0.1:1"}}, loadService(t, name, []byte(echoSpec), b.URL))

	resp, body := call(t, g, name, "POST", "/echo/1", "", nil)
	problemOf(t, resp, body, 503, gateway.ProblemTransportUnavailable)
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("no Retry-After")
	}
	resp, _ = call(t, g, name, "GET", "/echo/1", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GET without Kafka: %d", resp.StatusCode)
	}
}

func unsignedToken() string {
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	return enc(`{"alg":"none","typ":"JWT"}`) + "." + enc(fmt.Sprintf(`{"iss":%q,"aud":%q,"sub":"checkout","instance":"x","exp":%d}`,
		testIssuer, testAudience, time.Now().Add(time.Hour).Unix())) + "."
}

// Kafka hangs at startup and again mid-flight, on a broker owned by this test.
// Callers always get an explicit answer within their deadline, waiters never
// leak, reads keep working, and service resumes when Kafka does.
func TestKafkaHangsAndRecovers(t *testing.T) {
	broker := kafkatest.Dedicated(t)
	name := kafkatest.Service(t, "echo")
	b, rec := echoB(t)

	broker.Pause(t)
	g := startGateway(t, gwOpts{brokers: broker.Brokers, timeout: time.Second}, loadService(t, name, []byte(echoSpec), b.URL))
	resp, body := call(t, g, name, "POST", "/echo/1", "", nil)
	problemOf(t, resp, body, 503, gateway.ProblemTransportUnavailable)
	broker.Unpause(t)
	kafkatest.Eventually(t, 60*time.Second, g.Ready, "gateway ready once Kafka is back")

	startPeerOn(t, broker.Brokers, name, b.URL)
	resp, body = call(t, g, name, "POST", "/echo/1", "", []byte("before"))
	if resp.StatusCode != 200 || string(body) != "before" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}

	broker.Pause(t)
	start := time.Now()
	resp, body = call(t, g, name, "POST", "/echo/2", "", []byte("during"))
	// The command is either refused before reaching Kafka (503, not applied)
	// or stuck in flight (504, may still apply). Both are explicit and on time.
	switch resp.StatusCode {
	case 503:
		problemOf(t, resp, body, 503, gateway.ProblemTransportUnavailable)
	default:
		problemOf(t, resp, body, 504, wire.ProblemTypeTimeout)
	}
	t.Logf("POST while Kafka hangs: %d after %s", resp.StatusCode, time.Since(start).Round(time.Millisecond))
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("answered after %s with a 1s deadline", elapsed)
	}
	if g.Pending() != 0 {
		t.Fatalf("%d waiters leaked", g.Pending())
	}
	resp, _ = call(t, g, name, "GET", "/echo/1", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("GET during Kafka outage: %d", resp.StatusCode)
	}
	broker.Unpause(t)

	kafkatest.Eventually(t, 60*time.Second, func() bool {
		resp, body := call(t, g, name, "POST", "/echo/3", "", []byte("after"))
		return resp.StatusCode == 200 && string(body) == "after"
	}, "service resumes after Kafka recovers")
	if g.Pending() != 0 {
		t.Fatalf("%d waiters leaked", g.Pending())
	}
	t.Logf("B saw %d mutations; the one sent during the outage may run after recovery, as D4 allows", len(rec.all()))
}

// A Response written in an aborted transaction (a fenced zombie bridge) never
// happened: A must only ever get the committed outcome.
func TestAbortedResponseIsNeverDelivered(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	zombie := kafkatest.Client(t, kgo.TransactionalID(kafkatest.Service(t, "zombie")))
	startPeer(t, name, b.URL, func(cmd wire.Command) (wire.Response, bool) {
		phantom := wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown, "phantom from an aborted transaction")
		rec, err := wire.EncodeResponse(phantom, cmd.ReplyTo, keys().bridge)
		if err != nil {
			t.Error(err)
			return wire.Response{}, false
		}
		ctx := context.Background()
		if err := zombie.BeginTransaction(); err != nil {
			t.Error(err)
		}
		if err := zombie.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Error(err)
		}
		if err := zombie.EndTransaction(ctx, kgo.TryAbort); err != nil {
			t.Error(err)
		}
		return wire.Response{}, false // then the real, committed outcome
	})

	resp, body := call(t, g, name, "POST", "/echo/1", "text/plain", []byte("real"))
	if resp.StatusCode != 200 || string(body) != "real" {
		t.Fatalf("A got an outcome that was never committed: %d %s", resp.StatusCode, body)
	}
}

// Commands land on the partition the bridge recomputes from the key; any
// other placement is rejected there as a possible duplicate.
func TestCommandsArePartitionedByKey(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	p := startPeer(t, name, b.URL, nil)
	for i := range 30 {
		hdr := []string{}
		if i%2 == 0 {
			hdr = []string{wire.IdempotencyKeyHeader, fmt.Sprintf("key-%d", i)}
		}
		if resp, body := call(t, g, name, "POST", "/echo/1", "", nil, hdr...); resp.StatusCode != 200 {
			t.Fatalf("%d %s", resp.StatusCode, body)
		}
	}
	hasher := kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(name))
	seen := map[int32]bool{}
	for _, rec := range p.records() {
		if want := int32(hasher.Partition(rec, 3)); rec.Partition != want {
			t.Fatalf("key %q on partition %d, bridge expects %d", rec.Key, rec.Partition, want)
		}
		seen[rec.Partition] = true
	}
	if len(p.records()) != 30 || len(seen) < 2 {
		t.Fatalf("%d records over partitions %v", len(p.records()), seen)
	}
}

// Kafka hangs long enough for startup attempts to time out, then comes back:
// the gateway becomes ready promptly (measured: ~0.6s; the resumed broker
// answers the attempt in flight).
func TestReadyPromptlyAfterKafkaReturns(t *testing.T) {
	broker := kafkatest.Dedicated(t)
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	failed := make(chan struct{}, 16)
	broker.Pause(t)
	g := startGateway(t, gwOpts{brokers: broker.Brokers, onLog: func(line string) {
		if strings.Contains(line, "kafka not ready") {
			failed <- struct{}{}
		}
	}}, loadService(t, name, []byte(echoSpec), b.URL))
	<-failed // one attempt hung for its whole timeout; the next may be hanging now
	<-failed
	broker.Unpause(t)
	back := time.Now()
	kafkatest.Eventually(t, 15*time.Second, g.Ready, "gateway ready after Kafka is back")
	took := time.Since(back)
	t.Logf("ready %s after Kafka returned", took.Round(time.Millisecond))
	if took > 6*time.Second { // prepareTimeout 3s + maxPrepareBackoff 2s + margin
		t.Fatalf("ready only %s after Kafka returned", took)
	}
}

// Each topic has one creator, its producer. The gateway creates its command
// and reply topics; results and bridge state belong to the bridge, which
// derives their partitioning from the command topic.
func TestCreatesOnlyTopicsItProduces(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	g := startGateway(t, gwOpts{}, loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	topics, err := kafkatest.Admin(t).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !topics.Has(wire.CommandTopic(name)) {
		t.Fatalf("command topic %s not created", wire.CommandTopic(name))
	}
	for _, tp := range topics.Names() {
		if strings.HasSuffix(tp, "."+name) && tp != wire.CommandTopic(name) {
			t.Errorf("gateway created %s, which it does not produce", tp)
		}
	}
}

// D10: only Responses signed by a trusted bridge key reach a caller. A forged
// or unsigned one is dropped and the wait continues for the genuine one.
func TestForgedResponsesNeverReachTheCaller(t *testing.T) {
	name := kafkatest.Service(t, "echo")
	b, _ := echoB(t)
	var mu sync.Mutex
	var logs []string
	g := startGateway(t, gwOpts{onLog: func(l string) { mu.Lock(); logs = append(logs, l); mu.Unlock() }},
		loadService(t, name, []byte(echoSpec), b.URL))
	g.waitReady(t)
	_, otherKey, _ := ed25519.GenerateKey(nil)
	impostor, err := identity.NewSigner(identity.RoleBridge, keys().bridge.KeyID(), otherKey.Seed()) // same kid, wrong key
	if err != nil {
		t.Fatal(err)
	}
	writer := kafkatest.Client(t)
	startPeer(t, name, b.URL, func(cmd wire.Command) (wire.Response, bool) {
		forged := wire.Response{V: wire.Version, RequestID: cmd.RequestID, Status: 201, Headers: wire.Headers{}, Body: wire.NewBody([]byte("forged"))}
		signed, err := wire.EncodeResponse(forged, cmd.ReplyTo, impostor)
		if err != nil {
			t.Error(err)
		}
		unsigned := &kgo.Record{Topic: cmd.ReplyTo, Key: signed.Key, Value: signed.Value,
			Headers: []kgo.RecordHeader{{Key: wire.HeaderType, Value: []byte(wire.TypeResponse)}}}
		if err := writer.ProduceSync(context.Background(), signed, unsigned).FirstErr(); err != nil {
			t.Error(err)
		}
		return wire.Response{}, false // then the genuine reply
	})
	resp, body := call(t, g, name, "POST", "/echo/1", "text/plain", []byte("genuine"))
	if resp.StatusCode != 200 || string(body) != "genuine" {
		t.Fatalf("caller got %d %q", resp.StatusCode, body)
	}
	if g.DroppedReplies() != 2 {
		t.Fatalf("dropped %d, want the 2 forgeries", g.DroppedReplies())
	}
	want := `msg="response not authentic, dropped" requestId=` + resp.Header.Get(wire.RequestIDHeader)
	mu.Lock()
	defer mu.Unlock()
	n := 0
	for _, l := range logs {
		if strings.Contains(l, want) {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("want 2 log lines containing %q, got %d in %q", want, n, logs)
	}
}

const secureEchoSpec = `
openapi: 3.0.3
info: { title: SecureEcho, version: 1.0.0 }
security: [ { token: [] } ]
components:
  securitySchemes:
    token:   { type: apiKey, in: header, name: X-Auth-Token }
    qtoken:  { type: apiKey, in: query,  name: access_token }
    session: { type: apiKey, in: cookie, name: sid }
paths:
  /echo/{id}:
    parameters:
      - { name: id, in: path, required: true, schema: { type: string } }
    get:  { operationId: echoGet,  responses: { '200': { description: ok } } }
    post: { operationId: echoPost, responses: { '200': { description: ok } } }
`

// D12: whatever A sends where B's contract declares a credential never
// enters Kafka, in any topic, in any record part. B never receives A's
// secret either; on passthrough it gets the service's own credential.
func TestContractSecretsNeverEnterKafka(t *testing.T) {
	name := kafkatest.Service(t, "secure")
	b, rec := echoB(t)
	svc := loadService(t, name, []byte(secureEchoSpec), b.URL)
	serviceCred := "service-cred-" + ulid.Make().String()
	svc.Credentials = apispec.Credentials{"token": serviceCred}
	g := startGateway(t, gwOpts{}, svc)
	g.waitReady(t)
	startPeer(t, name, b.URL, nil)

	n := ulid.Make().String()
	tok := token(t)
	needles := map[string]string{
		"X-Auth-Token (contract header)":  "xauth-" + n,
		"access_token (contract query)":   "query-" + n,
		"sid cookie (contract cookie)":    "cookie-" + n,
		"X-Api-Key (standard)":            "apikey-" + n,
		"Proxy-Authorization (standard)":  "proxy-" + n,
		"bearer JWT signature (standard)": tok[strings.LastIndex(tok, ".")+1:],
	}
	target := "/echo/1?keep=a%20b&access_token=" + needles["access_token (contract query)"] + "&z=1"
	secrets := []string{
		"X-Auth-Token", needles["X-Auth-Token (contract header)"],
		"Cookie", "sid=" + needles["sid cookie (contract cookie)"] + "; theme=dark",
		"X-Api-Key", needles["X-Api-Key (standard)"],
		"Proxy-Authorization", "Basic " + needles["Proxy-Authorization (standard)"],
	}
	for _, method := range []string{"POST", "GET"} {
		resp, body := call(t, g, name, method, target, "text/plain", []byte(`{"a":1}`), secrets...)
		if resp.StatusCode != 200 {
			t.Fatalf("%s: %d %s", method, resp.StatusCode, body)
		}
	}

	for what, needle := range needles {
		if where := scanKafka(t, needle); len(where) > 0 {
			t.Errorf("%s found in Kafka: %v", what, where)
		}
	}
	if scanned := scanKafka(t, "keep=a%20b&z=1"); len(scanned) == 0 {
		t.Fatal("scan saw no command at all: it proves nothing")
	}

	for _, r := range rec.all() {
		if r.RequestURI != "/echo/1?keep=a%20b&z=1" {
			t.Errorf("%s: B got %q (secret param must go, the rest unchanged)", r.Method, r.RequestURI)
		}
		for _, h := range []string{"Cookie", "X-Api-Key", "Proxy-Authorization", "Authorization"} {
			if r.Header.Get(h) != "" {
				t.Errorf("%s: B received caller secret %s", r.Method, h)
			}
		}
		want := "" // mutations: the bridge injects the service credential
		if r.Method == "GET" {
			want = serviceCred
		}
		if got := r.Header.Get("X-Auth-Token"); got != want {
			t.Errorf("%s: B got X-Auth-Token %q, want %q", r.Method, got, want)
		}
	}
}

// scanKafka reads every record of every topic on the broker, committed or
// not, and reports where needle appears in a key, value or header.
// Transaction markers are kept so every partition's last offset (often a
// marker) is seen and the scan knows it reached the end.
func scanKafka(t *testing.T, needle string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	adm := kafkatest.Admin(t)
	ends, err := adm.ListEndOffsets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Partitions emptied by retention (reply topics keep 1h) have nothing
	// left to read: start == end.
	starts, err := adm.ListStartOffsets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parts := map[string]map[int32]kgo.Offset{}
	remaining := map[string]map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil || o.Offset == 0 || strings.HasPrefix(o.Topic, "__") {
			return
		}
		if st, ok := starts.Lookup(o.Topic, o.Partition); ok && st.Offset >= o.Offset {
			return
		}
		if parts[o.Topic] == nil {
			parts[o.Topic], remaining[o.Topic] = map[int32]kgo.Offset{}, map[int32]int64{}
		}
		parts[o.Topic][o.Partition] = kgo.NewOffset().AtStart()
		remaining[o.Topic][o.Partition] = o.Offset
	})
	cl := kafkatest.Client(t, kgo.ConsumePartitions(parts), kgo.FetchIsolationLevel(kgo.ReadUncommitted()), kgo.KeepControlRecords())
	var where []string
	left := 0
	for _, ps := range remaining {
		left += len(ps)
	}
	for left > 0 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("scan: %d partitions not read to the end", left)
		}
		fs.EachRecord(func(r *kgo.Record) {
			raw := append(append([]byte{}, r.Key...), r.Value...)
			for _, h := range r.Headers {
				raw = append(append(raw, h.Key...), h.Value...)
			}
			if bytes.Contains(raw, []byte(needle)) {
				where = append(where, fmt.Sprintf("%s[%d]@%d", r.Topic, r.Partition, r.Offset))
			}
			if end, ok := remaining[r.Topic][r.Partition]; ok && r.Offset+1 >= end {
				delete(remaining[r.Topic], r.Partition)
				left--
			}
		})
	}
	return where
}

// A passthrough operation whose contract requires a credential the gateway
// does not hold is a configuration error, refused at startup rather than
// answered 401 by B on every call. Mutations are the bridge's to check.
func TestPassthroughNeedingACredentialFailsAtStartup(t *testing.T) {
	cfg := func(creds apispec.Credentials) gateway.Config {
		svc := loadService(t, "sec", []byte(secureEchoSpec), "http://b.invalid")
		svc.Credentials = creds
		k := keys()
		return gateway.Config{Instance: "gw-x", Services: []gateway.Service{svc},
			Auth: &gateway.Authenticator{Keys: k.idpRing}, Signer: k.gateway, BridgeKeys: k.bridgeTrust,
			Brokers: []string{"127.0.0.1:1"}, Timeout: time.Second, CommandTTL: time.Minute, Partitions: 1}
	}
	if _, err := gateway.New(cfg(nil)); err == nil || !strings.Contains(err.Error(), "GET /echo/{id}") {
		t.Fatalf("GET needing a credential with none configured: %v", err)
	}
	if _, err := gateway.New(cfg(apispec.Credentials{"token": "x"})); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.New(cfg(apispec.Credentials{"token": "x", "nope": "y"})); err == nil {
		t.Fatal("credential for an undeclared scheme must be refused")
	}
}
