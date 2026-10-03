package gateway_test

import (
	"context"
	"crypto/ed25519"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/devidp"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/gateway"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkatest"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) { kafkatest.Main(m) }

const (
	testIssuer   = "test-idp"
	testAudience = "kafka-backbone"
)

// keys are generated once per test binary: gateway and bridge signing keys
// (and their public halves), and the IdP's token signing key.
type testKeys struct {
	gateway      *identity.Signer
	bridge       *identity.Signer
	gatewayTrust identity.TrustedKeys
	bridgeTrust  identity.TrustedKeys
	idpKID       string
	idpKey       ed25519.PrivateKey
	idpRing      identity.Keyring
}

var keys = sync.OnceValue(func() (k testKeys) {
	signer := func(role identity.Role, kid string) (*identity.Signer, identity.TrustedKeys) {
		_, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			panic(err)
		}
		s, err := identity.NewSigner(role, kid, priv.Seed())
		if err != nil {
			panic(err)
		}
		return s, s.Self()
	}
	k.gateway, k.gatewayTrust = signer(identity.RoleGateway, "test-gw")
	k.bridge, k.bridgeTrust = signer(identity.RoleBridge, "test-bridge")
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
	k.idpKID, k.idpKey, k.idpRing = "test-idp-1", priv, identity.Keyring{"test-idp-1": pub}
	return k
})

func token(t testing.TB, mutate ...func(*devidp.Token)) string {
	t.Helper()
	k := keys()
	tok := devidp.Token{KeyID: k.idpKID, Key: k.idpKey, Issuer: testIssuer, Audience: testAudience,
		Application: "checkout", Instance: "checkout-1", IssuedAt: time.Now(), TTL: time.Hour}
	for _, m := range mutate {
		m(&tok)
	}
	s, err := tok.Sign()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// echoSpec is a service whose B reflects what it received, so tests see
// exactly what crossed Kafka in both directions.
const echoSpec = `
openapi: 3.0.3
info: { title: Echo, version: 1.0.0 }
paths:
  /echo/{id}:
    parameters:
      - { name: id, in: path, required: true, schema: { type: string } }
    get:    { operationId: echoGet,    responses: { '200': { description: ok } } }
    post:   { operationId: echoPost,   responses: { '200': { description: ok } } }
    put:    { operationId: echoPut,    responses: { '200': { description: ok } } }
    patch:  { operationId: echoPatch,  responses: { '200': { description: ok } } }
    delete: { operationId: echoDelete, responses: { '204': { description: gone } } }
`

// echoB answers with the request body and Content-Type, the raw request URI
// in X-Echo-Uri and the received header names in X-Echo-Headers. ?status=N
// picks the status. Two Set-Cookie values test multi-valued headers.
func echoB(t testing.TB) (*httptest.Server, *recorder) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.add(r)
		status := http.StatusOK
		if r.Method == http.MethodDelete {
			status = http.StatusNoContent
		}
		if s := r.URL.Query().Get("status"); s != "" {
			status, _ = strconv.Atoi(s)
		}
		h := w.Header()
		if ct := r.Header.Get("Content-Type"); ct != "" {
			h.Set("Content-Type", ct)
		}
		h.Set("X-Echo-Uri", r.RequestURI)
		h.Add("Set-Cookie", "a=1")
		h.Add("Set-Cookie", "b=2")
		h.Set("X-Request-Id", "set-by-b")
		w.WriteHeader(status)
		if status != http.StatusNoContent {
			_, _ = w.Write(body)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

type recorder struct {
	mu   sync.Mutex
	reqs []*http.Request
}

func (r *recorder) add(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, req)
}

func (r *recorder) all() []*http.Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*http.Request(nil), r.reqs...)
}

type gw struct {
	*gateway.Gateway
	srv    *httptest.Server
	cancel context.CancelFunc
	done   chan error
}

type gwOpts struct {
	instance string
	brokers  []string
	timeout  time.Duration
	onTiming func(gateway.Timing)
	onLog    func(line string)
}

// startGateway runs a gateway for the given services and waits until it can
// accept mutations (unless brokers are unreachable on purpose).
func startGateway(t testing.TB, o gwOpts, services ...gateway.Service) *gw {
	t.Helper()
	if o.instance == "" {
		o.instance = kafkatest.Service(t, "gw")
	}
	if o.brokers == nil {
		o.brokers = kafkatest.Brokers(t)
	}
	if o.timeout == 0 {
		o.timeout = 10 * time.Second
	}
	k := keys()
	g, err := gateway.New(gateway.Config{
		Instance:   o.instance,
		Services:   services,
		Auth:       &gateway.Authenticator{Keys: k.idpRing, Issuer: testIssuer, Audience: testAudience, Leeway: 30 * time.Second},
		Signer:     k.gateway,
		BridgeKeys: k.bridgeTrust,
		Brokers:    o.brokers,
		Timeout:    o.timeout,
		CommandTTL: 5 * time.Minute,
		Partitions: 3,
		Logger:     slog.New(slog.NewTextHandler(testWriter{t, o.onLog}, &slog.HandlerOptions{Level: slog.LevelWarn})),
		OnTiming:   o.onTiming,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	srv := httptest.NewServer(g)
	out := &gw{Gateway: g, srv: srv, cancel: cancel, done: done}
	t.Cleanup(out.stop)
	return out
}

func (g *gw) waitReady(t testing.TB) {
	t.Helper()
	kafkatest.Eventually(t, 30*time.Second, g.Ready, "gateway ready")
}

func (g *gw) stop() {
	g.srv.Close()
	g.cancel()
	<-g.done
	g.done <- nil // idempotent for t.Cleanup after an explicit stop
}

type testWriter struct {
	t     testing.TB
	onLog func(string)
}

func (w testWriter) Write(p []byte) (int, error) {
	line := strings.TrimSpace(string(p))
	w.t.Log(line)
	if w.onLog != nil {
		w.onLog(line)
	}
	return len(p), nil
}

func loadService(t testing.TB, name string, spec []byte, upstream string) gateway.Service {
	t.Helper()
	s, err := apispec.Load(name, spec)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(upstream)
	if err != nil {
		t.Fatal(err)
	}
	return gateway.Service{Spec: s, Upstream: u}
}

// peer is a minimal protocol counterpart of the bridge: it verifies commands,
// calls B, and replies on Command.ReplyTo, per the wire contract. Kafka is
// real. It does no dedup: tests needing D3 run against the real bridge.
type peer struct {
	t        testing.TB
	upstream string
	// respond overrides the default (call B) when it returns ok.
	respond  func(cmd wire.Command) (wire.Response, bool)
	mu       sync.Mutex
	commands []*kgo.Record
	fromB    map[string][]byte // requestId -> B's exact body
}

func startPeer(t testing.TB, service, upstream string, respond func(wire.Command) (wire.Response, bool)) *peer {
	t.Helper()
	return startPeerWith(t, kafkatest.Client(t, peerOpts(service)...), service, upstream, respond)
}

func startPeerOn(t testing.TB, brokers []string, service, upstream string) *peer {
	t.Helper()
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(brokers, peerOpts(service)...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return startPeerWith(t, cl, service, upstream, nil)
}

func peerOpts(service string) []kgo.Opt {
	return []kgo.Opt{kgo.ConsumeTopics(wire.CommandTopic(service)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.ProducerLinger(0)}
}

func startPeerWith(t testing.TB, cl *kgo.Client, service, upstream string, respond func(wire.Command) (wire.Response, bool)) *peer {
	t.Helper()
	p := &peer{t: t, upstream: upstream, respond: respond, fromB: map[string][]byte{}}
	verifier := wire.CommandVerifier{Keys: keys().gatewayTrust, Service: service}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			fs := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			fs.EachRecord(func(rec *kgo.Record) {
				p.mu.Lock()
				p.commands = append(p.commands, rec)
				p.mu.Unlock()
				cmd, err := verifier.Verify(rec)
				if err != nil {
					t.Errorf("peer: %v", err)
					return
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					resp := p.handle(ctx, cmd)
					out, err := wire.EncodeResponse(resp, cmd.ReplyTo, keys().bridge)
					if err != nil {
						t.Errorf("peer encode: %v", err)
						return
					}
					if err := cl.ProduceSync(ctx, out).FirstErr(); err != nil && ctx.Err() == nil {
						t.Errorf("peer produce: %v", err)
					}
				}()
			})
		}
	}()
	return p
}

func (p *peer) handle(ctx context.Context, cmd wire.Command) wire.Response {
	if p.respond != nil {
		if r, ok := p.respond(cmd); ok {
			return r
		}
	}
	req, err := http.NewRequestWithContext(ctx, cmd.Method, p.upstream+cmd.URL(), strings.NewReader(string(cmd.Body.Bytes())))
	if err != nil {
		return wire.FaultResponse(cmd.RequestID, wire.FaultUpstreamUnavailable, err.Error())
	}
	req.Header = cmd.Headers.HTTP()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown, err.Error())
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	p.mu.Lock()
	p.fromB[cmd.RequestID] = body
	p.mu.Unlock()
	return wire.Response{V: wire.Version, RequestID: cmd.RequestID, Status: resp.StatusCode,
		Headers: wire.ResponseHeaders(resp.Header), Body: wire.NewBody(body)}
}

func (p *peer) bodyFromB(id string) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.fromB[id]
	return b, ok
}

func (p *peer) records() []*kgo.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*kgo.Record(nil), p.commands...)
}

// call sends a request to the gateway as service host.
func call(t testing.TB, g *gw, host, method, target, contentType string, body []byte, hdr ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, g.srv.URL+target, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Authorization", "Bearer "+token(t))
	for i := 0; i+1 < len(hdr); i += 2 {
		if hdr[i+1] == "" {
			req.Header.Del(hdr[i])
			continue
		}
		req.Header.Add(hdr[i], hdr[i+1])
	}
	resp, err := g.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}
