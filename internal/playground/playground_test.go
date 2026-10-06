package playground_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/audit"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/demo/orders"
	"github.com/sderosiaux/http-over-kafka/internal/demo/payments"
	"github.com/sderosiaux/http-over-kafka/internal/events"
	"github.com/sderosiaux/http-over-kafka/internal/gateway"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/playground"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) { kafkatest.Main(m) }

const (
	issuer   = "test-idp"
	audience = "http-over-kafka"
)

type testKeys struct {
	gateway, bridge           *identity.Signer
	gatewayTrust, bridgeTrust identity.TrustedKeys
	idp                       playground.IdP
	idpRing                   identity.Keyring
}

var keys = sync.OnceValue(func() testKeys {
	signer := func(role identity.Role, kid string) *identity.Signer {
		_, priv, err := ed25519.GenerateKey(nil)
		if err != nil {
			panic(err)
		}
		s, err := identity.NewSigner(role, kid, priv.Seed())
		if err != nil {
			panic(err)
		}
		return s
	}
	k := testKeys{gateway: signer(identity.RoleGateway, "test-gw"), bridge: signer(identity.RoleBridge, "test-bridge")}
	k.gatewayTrust, k.bridgeTrust = k.gateway.Self(), k.bridge.Self()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		panic(err)
	}
	k.idp = playground.IdP{KeyID: "test-idp-1", Key: priv, Issuer: issuer, Audience: audience}
	k.idpRing = identity.Keyring{"test-idp-1": pub}
	return k
})

// specs loads the demo specs under unique names, with a unique event topic,
// so tests sharing a broker never read each other's records.
func specs(t *testing.T) (ord, pay *apispec.Service) {
	t.Helper()
	on, pn := kafkatest.Service(t, "orders"), kafkatest.Service(t, "payments")
	ordersYAML := bytes.ReplaceAll(api.Orders, []byte("topic: orders.events"), []byte("topic: "+on+".events"))
	ord, err := apispec.Load(on, ordersYAML)
	if err != nil {
		t.Fatal(err)
	}
	if pay, err = apispec.Load(pn, api.Payments); err != nil {
		t.Fatal(err)
	}
	return ord, pay
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func startPlayground(t *testing.T, cfg playground.Config) *httptest.Server {
	t.Helper()
	k := keys()
	cfg.IdP, cfg.Application = k.idp, "checkout"
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	pg, err := playground.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pg.Run(ctx) }()
	srv := httptest.NewServer(pg.Handler())
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("playground run: %v", err)
		}
	})
	return srv
}

// stream is an SSE client of /api/events. Records are kept in arrival
// order; next searches them all, so waiting for one never loses another.
type stream struct {
	t       *testing.T
	records chan playground.Item
	status  chan playground.Status
	seen    []playground.Item
}

func openStream(t *testing.T, base string, lastEventID string) *stream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/events", nil)
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if ct := resp.Header.Get("Content-Type"); resp.StatusCode != 200 || ct != "text/event-stream" {
		t.Fatalf("events: %d %s", resp.StatusCode, ct)
	}
	s := &stream{t: t, records: make(chan playground.Item, 1024), status: make(chan playground.Status, 64)}
	go func() {
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 1<<20), 4<<20)
		var event, data string
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			case line == "" && event != "":
				switch event {
				case "record":
					var it playground.Item
					if err := json.Unmarshal([]byte(data), &it); err != nil {
						t.Errorf("record: %v", err)
					}
					s.records <- it
				case "status":
					var st playground.Status
					if err := json.Unmarshal([]byte(data), &st); err != nil {
						t.Errorf("status: %v", err)
					}
					s.status <- st
				}
				event, data = "", ""
			}
		}
		if ctx.Err() == nil {
			t.Logf("event stream ended: %v", sc.Err())
		}
	}()
	return s
}

// next returns the first record matching pred, failing after timeout.
func (s *stream) next(timeout time.Duration, what string, pred func(playground.Item) bool) playground.Item {
	s.t.Helper()
	for _, it := range s.seen {
		if pred(it) {
			return it
		}
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case it := <-s.records:
			s.seen = append(s.seen, it)
			if pred(it) {
				return it
			}
		case <-deadline.C:
			s.t.Fatalf("no record within %s: %s", timeout, what)
		}
	}
}

func (s *stream) waitStatus(timeout time.Duration, what string, pred func(playground.Status) bool) playground.Status {
	s.t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case st := <-s.status:
			if pred(st) {
				return st
			}
		case <-deadline.C:
			s.t.Fatalf("status not reached within %s: %s", timeout, what)
		}
	}
}

func of(kind, rid string) func(playground.Item) bool {
	return func(it playground.Item) bool { return it.Kind == kind && it.RequestID == rid }
}

func command(t *testing.T, service, replyTo string, idemKey string) wire.Command {
	t.Helper()
	now := time.Now().UTC()
	return wire.Command{
		V: wire.Version, RequestID: wire.NewRequestID(), Service: service, OperationID: "createOrder",
		Method: http.MethodPost, PathTemplate: "/orders", Path: "/orders",
		Caller:  wire.Caller{Application: "checkout", Instance: "checkout-1"},
		Headers: wire.Headers{"content-type": {"application/json"}},
		Body:    wire.NewBody([]byte(`{"customerId":"c1","items":[{"sku":"A","quantity":1}]}`)),
		ReplyTo: replyTo, IdempotencyKey: idemKey,
		IssuedAt: now, Deadline: now.Add(10 * time.Second), ExpiresAt: now.Add(time.Minute),
	}
}

func produce(t *testing.T, cl *kgo.Client, recs ...*kgo.Record) {
	t.Helper()
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

// The feed shows what was committed after the playground started, with the
// authenticity of every signed record, and resumes from Last-Event-ID.
func TestFeedFromStartWithAuthenticity(t *testing.T) {
	k := keys()
	ord, pay := specs(t)
	gwInstance := kafkatest.Service(t, "gw")
	replyTopic := wire.ReplyTopic(gwInstance)
	eventTopic := ord.Name() + ".events"
	kafkatest.CreateTopics(t,
		kafkaenv.CommandTopic(ord.Name(), 1), kafkaenv.ResultTopic(ord.Name(), 1),
		kafkaenv.ReplyTopic(gwInstance), kafkaenv.EventTopic(eventTopic, 1))
	cl := kafkatest.Client(t)

	before, err := wire.EncodeCommand(command(t, ord.Name(), replyTopic, ""), k.gateway)
	if err != nil {
		t.Fatal(err)
	}
	produce(t, cl, before)
	since := before.Timestamp.Add(time.Millisecond)
	at := since.Add(time.Second) // explicit: the records below are after since

	srv := startPlayground(t, playground.Config{
		Brokers: kafkatest.Brokers(t), Orders: ord, Payments: pay, Since: since,
		Gateway: mustURL(t, "http://127.0.0.1:1"), GatewayAdmin: mustURL(t, "http://127.0.0.1:1"),
		GatewayKeys: k.gatewayTrust, BridgeKeys: k.bridgeTrust, PollInterval: 200 * time.Millisecond,
	})
	s := openStream(t, srv.URL, "")

	ok := command(t, ord.Name(), replyTopic, "key-1")
	okRec, err := wire.EncodeCommand(ok, k.gateway)
	if err != nil {
		t.Fatal(err)
	}
	other, err := identity.NewSigner(identity.RoleGateway, "test-gw", bytes.Repeat([]byte{7}, ed25519.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	forged := command(t, ord.Name(), replyTopic, "")
	forgedRec, err := wire.EncodeCommand(forged, other)
	if err != nil {
		t.Fatal(err)
	}
	created := wire.Response{V: wire.Version, RequestID: ok.RequestID, Status: 201,
		Headers: wire.Headers{"content-type": {"application/json"}}, Body: wire.NewBody([]byte(`{"id":"ord_1"}`))}
	res, err := wire.NewResult(ok, created, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	resRec, err := wire.EncodeResult(res, k.bridge)
	if err != nil {
		t.Fatal(err)
	}
	failedCmd := command(t, ord.Name(), replyTopic, "")
	failed, err := wire.NewResult(failedCmd, wire.Response{V: wire.Version, RequestID: failedCmd.RequestID, Status: 400,
		Headers: wire.Headers{}, Body: wire.NewBody([]byte(`{"title":"bad"}`))}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	failedRec, err := wire.EncodeResult(failed, k.bridge)
	if err != nil {
		t.Fatal(err)
	}
	retry := command(t, ord.Name(), replyTopic, "key-1")
	replay := created
	replay.RequestID, replay.ReplayOf = retry.RequestID, ok.RequestID
	replayRec, err := wire.EncodeResponse(replay, replyTopic, k.bridge)
	if err != nil {
		t.Fatal(err)
	}
	event := &kgo.Record{Topic: eventTopic, Key: []byte("ord_1"), Value: []byte(`{"id":"ord_1"}`),
		Headers: []kgo.RecordHeader{{Key: "ce_id", Value: []byte(ok.RequestID)}, {Key: "ce_type", Value: []byte("OrderCreated")}}}
	for _, r := range []*kgo.Record{okRec, forgedRec, resRec, failedRec, replayRec, event} {
		r.Timestamp = at
	}
	produce(t, cl, okRec, forgedRec, resRec, failedRec, replayRec, event)

	// Same partition, offsets in order: if "before" were read, it would come
	// first. (The pattern also matches other tests' topics, hence the topic.)
	first := s.next(30*time.Second, "first command", func(it playground.Item) bool { return it.Topic == wire.CommandTopic(ord.Name()) })
	if first.RequestID != ok.RequestID {
		t.Fatalf("first record shown on %s is %s at offset %d, want %s: the record before Since leaked", first.Topic, first.RequestID, first.Offset, ok.RequestID)
	}
	if first.Authenticity != audit.Authentic || first.Command.Caller.Application != "checkout" ||
		!first.Command.Idempotent || len(first.Command.CallerSecrets) != 0 || !strings.Contains(first.Value, `"requestId":"`+ok.RequestID) {
		t.Fatalf("command item: %+v", first)
	}
	if f := s.next(10*time.Second, "forged command", of("command", forged.RequestID)); f.Authenticity != audit.NotAuthentic || f.AuthError == "" {
		t.Fatalf("forged command shown as %q", f.Authenticity)
	}
	r := s.next(10*time.Second, "result", of("result", ok.RequestID))
	if r.Authenticity != audit.Authentic || r.Result.Type != "CreateOrderSucceeded" || !r.Result.Expect.Fires || r.Result.Expect.Type != "OrderCreated" || r.Result.Expect.Topic != eventTopic {
		t.Fatalf("result item: %+v expect %+v", r, r.Result.Expect)
	}
	fr := s.next(10*time.Second, "failed result", of("result", failedCmd.RequestID))
	if fr.Result.Outcome != wire.OutcomeFailed || fr.Result.Expect.Fires || !fr.Result.Expect.Declared ||
		!strings.Contains(fr.Result.Expect.Reason, "on 201 only") {
		t.Fatalf("failed result expect: %+v", fr.Result.Expect)
	}
	rp := s.next(10*time.Second, "replayed response", of("response", retry.RequestID))
	if rp.Authenticity != audit.Authentic || rp.Response.ReplayOf != ok.RequestID || rp.Response.Status != 201 {
		t.Fatalf("response item: %+v", rp)
	}
	ev := s.next(10*time.Second, "event", of("event", ok.RequestID))
	if ev.Authenticity != playground.Unsigned || ev.Event.Type != "OrderCreated" || ev.Key != "ord_1" {
		t.Fatalf("event item: %+v", ev)
	}

	// A reconnecting page gets only what it has not seen.
	again := openStream(t, srv.URL, "1")
	if it := again.next(10*time.Second, "resumed", func(playground.Item) bool { return true }); it.Seq != 2 {
		t.Fatalf("resume after Last-Event-ID 1 starts at seq %d", it.Seq)
	}
}

type stack struct {
	gw       *httptest.Server
	pg       *httptest.Server
	stream   *stream
	orders   *orders.Service
	payments *payments.Service
	ord, pay *apispec.Service
}

// startStack runs the real gateway, bridges and deriver in-process, against
// the demo services, and a playground in front of them.
func startStack(t *testing.T) stack {
	t.Helper()
	k := keys()
	brokers := kafkatest.Brokers(t)
	ord, pay := specs(t)
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	run := func(name string, f func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(ctx); err != nil && ctx.Err() == nil {
				t.Errorf("%s: %v", name, err)
			}
		}()
	}

	ordersSvc, paymentsSvc := orders.New(), payments.New()
	ordersB, paymentsB := httptest.NewServer(ordersSvc.Handler()), httptest.NewServer(paymentsSvc.Handler())
	t.Cleanup(ordersB.Close)
	t.Cleanup(paymentsB.Close)

	g, err := gateway.New(gateway.Config{
		Instance: kafkatest.Service(t, "gw"),
		Services: []gateway.Service{{Spec: ord, Upstream: mustURL(t, ordersB.URL)}, {Spec: pay, Upstream: mustURL(t, paymentsB.URL)}},
		Auth:     &gateway.Authenticator{Keys: k.idpRing, Issuer: issuer, Audience: audience, Leeway: 30 * time.Second},
		Signer:   k.gateway, BridgeKeys: k.bridgeTrust, Brokers: brokers,
		Timeout: 30 * time.Second, CommandTTL: 5 * time.Minute, Partitions: 1, Logger: quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	run("gateway", g.Run)
	gw := httptest.NewServer(g)
	t.Cleanup(gw.Close)
	admin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !g.Ready() {
			http.Error(w, "kafka not ready", http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(admin.Close)
	kafkatest.Eventually(t, 30*time.Second, g.Ready, "gateway ready")

	// A command written before a bridge's dedup memory starts is answered
	// outcome_unknown (D14): wait for every bridge partition to be ready.
	ready := &logCounter{match: `msg="partition ready"`}
	bridgeLog := slog.New(slog.NewTextHandler(ready, nil))
	for _, svc := range []struct {
		spec     *apispec.Service
		upstream string
	}{{ord, ordersB.URL}, {pay, paymentsB.URL}} {
		b, err := bridge.New(bridge.Config{Service: svc.spec.Name(), Brokers: brokers, Spec: svc.spec,
			Keys: k.gatewayTrust, Signer: k.bridge, Upstream: svc.upstream, Instance: "b1", Log: bridgeLog})
		if err != nil {
			t.Fatal(err)
		}
		run("bridge "+svc.spec.Name(), b.Run)
	}
	kafkatest.Eventually(t, 60*time.Second, func() bool { return ready.count() >= 2 }, "both bridges ready")
	// The deriver provisions its topics before Run, as cmd/deriver does.
	kafkatest.CreateTopics(t, events.Topics(ord, 1, 0)...)
	d, err := events.New(events.Config{Brokers: brokers, Service: ord, Instance: "d1", BridgeKeys: k.bridgeTrust, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	run("deriver", d.Run)

	pg := startPlayground(t, playground.Config{
		Brokers: brokers, Orders: ord, Payments: pay,
		Gateway: mustURL(t, gw.URL), GatewayAdmin: mustURL(t, admin.URL), PublicGateway: "http://localhost:8080",
		Console:     "http://localhost:8088",
		GatewayKeys: k.gatewayTrust, BridgeKeys: k.bridgeTrust, PollInterval: 200 * time.Millisecond,
	})
	s := openStream(t, pg.URL, "")
	up := s.waitStatus(30*time.Second, "gateway and kafka up", func(st playground.Status) bool {
		return st.Gateway.State == playground.Up && st.Kafka.State == playground.Up
	})
	if want := []string{wire.CommandTopic(ord.Name()), wire.ResultTopic(ord.Name()), ord.Name() + ".events"}; up.Console != "http://localhost:8088" || !slices.Equal(up.Watch, want) {
		t.Fatalf("console %q, watch %v, want %v", up.Console, up.Watch, want)
	}
	return stack{gw: gw, pg: pg, stream: s, orders: ordersSvc, payments: paymentsSvc, ord: ord, pay: pay}
}

// logCounter counts log lines containing match.
type logCounter struct {
	match string
	mu    sync.Mutex
	n     int
}

func (c *logCounter) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n += strings.Count(string(p), c.match)
	return len(p), nil
}

func (c *logCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func act(t *testing.T, base, name, body string) (int, playground.Exchange) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/api/actions/"+name, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", base)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var ex playground.Exchange
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(b, &ex); err != nil {
			t.Fatalf("%s: %v %s", name, err, b)
		}
	}
	return resp.StatusCode, ex
}

func header(hs []playground.Header, name string) string {
	for _, h := range hs {
		if strings.EqualFold(h.Key, name) {
			return h.Value
		}
	}
	return ""
}

func TestActionsThroughTheRealGateway(t *testing.T) {
	st := startStack(t)
	s, base := st.stream, st.pg.URL
	const wait = 60 * time.Second

	// 201: command, response, result and OrderCreated, all from Kafka.
	code, created := act(t, base, playground.ActionCreateOrder, "{}")
	if code != 200 || created.Error != nil || created.Response == nil || created.Response.Status != 201 || created.RequestID == "" {
		t.Fatalf("create order: %d %+v %+v", code, created.Error, created.Response)
	}
	if !strings.Contains(created.Curl, "-H 'Host: "+st.ord.Name()+"'") || !strings.Contains(created.Curl, "http://localhost:8080/orders") ||
		!strings.Contains(created.Curl, "Idempotency-Key: pg-") || !strings.Contains(created.Curl, `Bearer $TOKEN`) ||
		!strings.HasPrefix(created.Curl, "TOKEN=$(curl -s '"+base+"/token')") {
		t.Fatalf("curl: %s", created.Curl)
	}
	if created.Check == nil || !strings.HasPrefix(created.Check.Summary, "1 order for cust-") {
		t.Fatalf("check: %+v", created.Check)
	}
	rid := created.RequestID
	if c := s.next(wait, "command", of("command", rid)); c.Authenticity != audit.Authentic || len(c.Command.CallerSecrets) != 0 ||
		c.Command.Caller.Application != "checkout" || strings.Contains(c.Value, "Bearer") {
		t.Fatalf("command: %+v", c)
	}
	if r := s.next(wait, "response", of("response", rid)); r.Response.Status != 201 || r.Authenticity != audit.Authentic {
		t.Fatalf("response: %+v", r)
	}
	if r := s.next(wait, "result", of("result", rid)); r.Result.Type != "CreateOrderSucceeded" || !r.Result.Expect.Fires || r.Authenticity != audit.Authentic {
		t.Fatalf("result: %+v", r.Result)
	}
	if e := s.next(wait, "event", of("event", rid)); e.Event.Type != "OrderCreated" {
		t.Fatalf("event: %+v", e)
	}

	// 400: a failed result, and no event.
	_, invalid := act(t, base, playground.ActionInvalidOrder, "{}")
	if invalid.Response == nil || invalid.Response.Status != 400 || invalid.Retryable {
		t.Fatalf("invalid order: %+v", invalid.Response)
	}
	if r := s.next(wait, "failed result", of("result", invalid.RequestID)); r.Result.Outcome != wire.OutcomeFailed ||
		r.Result.Expect.Fires || !strings.Contains(r.Result.Expect.Reason, "on 201 only; the service answered 400") {
		t.Fatalf("failed result: %+v", r.Result)
	}
	// Results and events have one partition here and the deriver handles
	// them in order: once the next order's event is out, the 400 is settled.
	_, next := act(t, base, playground.ActionCreateOrder, "{}")
	s.next(wait, "next order's event", func(it playground.Item) bool {
		if it.Kind == "event" && it.RequestID == invalid.RequestID {
			t.Fatalf("a 400 produced an event: %+v", it)
		}
		return it.Kind == "event" && it.RequestID == next.RequestID
	})

	// Retry: the original answer, replayed; the service is not called.
	if n := st.orders.Creates(); n != 2 {
		t.Fatalf("orders created before retry: %d", n)
	}
	_, retry := act(t, base, playground.ActionRetry, `{"of":"`+created.ID+`"}`)
	if retry.Response == nil || retry.Response.Status != 201 || header(retry.Response.Headers, "Idempotent-Replayed") != "true" ||
		retry.Response.Body != created.Response.Body || retry.RetryOf != created.ID {
		t.Fatalf("retry: %+v", retry.Response)
	}
	if retry.Check == nil || retry.Check.Summary != created.Check.Summary {
		t.Fatalf("retry check: %+v, want %q", retry.Check, created.Check.Summary)
	}
	if r := s.next(wait, "replayed response", of("response", retry.RequestID)); r.Response.ReplayOf != rid {
		t.Fatalf("replayed response: %+v", r.Response)
	}
	if n := st.orders.Creates(); n != 2 {
		t.Fatalf("the retry executed the service: %d creations", n)
	}

	// Charge, then retry it: one debit in the ledger.
	_, charge := act(t, base, playground.ActionCharge, "{}")
	if charge.Response == nil || charge.Response.Status != 201 || charge.Check == nil || !strings.Contains(charge.Check.Summary, "debited 1 time, 500 cents") {
		t.Fatalf("charge: %+v %+v", charge.Response, charge.Check)
	}
	if r := s.next(wait, "charge result", of("result", charge.RequestID)); r.Result.Expect.Declared || r.Result.Expect.Reason == "" {
		t.Fatalf("charge result expect: %+v", r.Result.Expect)
	}
	_, again := act(t, base, playground.ActionRetry, `{"of":"`+charge.ID+`"}`)
	if again.Check == nil || again.Check.Summary != charge.Check.Summary {
		t.Fatalf("charge retry check: %+v", again.Check)
	}
	var account struct {
		AccountID string `json:"accountId"`
	}
	_ = json.Unmarshal([]byte(charge.Request.Body), &account)
	if a := st.payments.Account(account.AccountID); a.DebitCount != 1 {
		t.Fatalf("ledger after retry: %+v", a)
	}

	// Only the fixed actions, only from this origin.
	if code, _ := act(t, base, "anything", "{}"); code != 404 {
		t.Fatalf("unknown action: %d", code)
	}
	if code, _ := act(t, base, playground.ActionRetry, `{"of":"nope"}`); code != 404 {
		t.Fatalf("retry of unknown exchange: %d", code)
	}
	if code, _ := act(t, base, playground.ActionRetry, `{"of":"`+invalid.ID+`"}`); code != 404 {
		t.Fatalf("retry of a request without key: %d", code)
	}
	req, _ := http.NewRequest(http.MethodPost, base+"/api/actions/"+playground.ActionCreateOrder, nil)
	req.Header.Set("Origin", "https://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin action: %d", resp.StatusCode)
	}

	// GET /token: what a terminal uses. The gateway accepts it.
	tok := getToken(t, base)
	req, _ = http.NewRequest(http.MethodGet, st.gw.URL+"/orders", nil)
	req.Host = st.ord.Name()
	req.Header.Set("Authorization", "Bearer "+tok)
	if resp, err = http.DefaultClient.Do(req); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("gateway with the /token token: %d", resp.StatusCode)
	}
}

func getToken(t *testing.T, base string) string {
	t.Helper()
	resp, err := http.Get(base + "/token")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	tok := strings.TrimSuffix(string(b), "\n")
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") ||
		resp.Header.Get("Cache-Control") != "no-store" || strings.Count(tok, ".") != 2 || strings.ContainsAny(tok, " \n") {
		t.Fatalf("/token: %d %q %q", resp.StatusCode, resp.Header.Get("Content-Type"), b)
	}
	return tok
}

// With nothing to talk to, the page is told so, and an action says what
// failed and what to check.
func TestDownDependenciesAreReported(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	ord, pay := specs(t)
	srv := startPlayground(t, playground.Config{
		Brokers: []string{strings.TrimPrefix(deadURL, "http://")}, Orders: ord, Payments: pay,
		Gateway: mustURL(t, deadURL), GatewayAdmin: mustURL(t, deadURL), PollInterval: 300 * time.Millisecond,
	})
	s := openStream(t, srv.URL, "")
	got := s.waitStatus(30*time.Second, "both down", func(st playground.Status) bool {
		return st.Gateway.State == playground.Down && st.Kafka.State == playground.Down
	})
	if got.Gateway.Detail == "" || got.Kafka.Detail == "" {
		t.Fatalf("down without detail: %+v", got)
	}
	_, ex := act(t, srv.URL, playground.ActionCreateOrder, "{}")
	if ex.Error == nil || ex.Error.Title != "The gateway did not answer" || !strings.Contains(ex.Error.Hint, "docker compose logs gateway") || ex.Response != nil {
		t.Fatalf("action with the gateway down: %+v", ex)
	}

	// A token needs neither Kafka nor the gateway.
	getToken(t, srv.URL)

	page, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(page.Body)
	page.Body.Close()
	if page.StatusCode != 200 || !bytes.Contains(body, []byte("What Kafka recorded")) {
		t.Fatalf("index: %d", page.StatusCode)
	}
}
