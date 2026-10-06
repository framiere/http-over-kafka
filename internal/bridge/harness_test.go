package bridge_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/demo/payments"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) { kafkatest.Main(m) }

const partitions = 3

// env is one isolated world: a unique service on the shared broker, a real B,
// a key pair, and a fake gateway instance that signs commands and collects
// the Responses addressed to it.
type env struct {
	t             *testing.T
	service       string
	specData      []byte
	upstream      string
	signer        *identity.Signer // the gateway's: signs commands
	bridgeSigning string           // the bridge's private key, env format
	bridgeSigner  *identity.Signer
	bridgeKeys    identity.TrustedKeys // what the gateway trusts
	trusted       string               // gateway public key, env format
	keys          identity.TrustedKeys
	gw            string // gateway instance
	producer      *kgo.Client

	mu        sync.Mutex
	responses map[string][]wire.Response
	waiters   map[string]chan wire.Response
}

func newEnv(t *testing.T, prefix string, specData []byte, upstream string) *env {
	t.Helper()
	return newEnvWith(t, prefix, specData, upstream, true)
}

// newEnvWith lets a test leave the command topic to be created later, as a
// gateway started after the bridge would.
func newEnvWith(t *testing.T, prefix string, specData []byte, upstream string, commandTopic bool) *env {
	t.Helper()
	signing, trusted, err := identity.Generate("test-gw")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(identity.RoleGateway), signing)
	signer, err := identity.SignerFromEnv(identity.RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := identity.ParseTrustedKeys(identity.RoleGateway, trusted)
	if err != nil {
		t.Fatal(err)
	}
	bSigning, bTrusted, err := identity.Generate("test-bridge")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(identity.RoleBridge), bSigning)
	bSigner, err := identity.SignerFromEnv(identity.RoleBridge)
	if err != nil {
		t.Fatal(err)
	}
	bKeys, err := identity.ParseTrustedKeys(identity.RoleBridge, bTrusted)
	if err != nil {
		t.Fatal(err)
	}
	e := &env{
		t: t, service: kafkatest.Service(t, prefix), specData: specData, upstream: upstream,
		signer: signer, trusted: trusted, keys: keys,
		bridgeSigning: bSigning, bridgeSigner: bSigner, bridgeKeys: bKeys,
		responses: map[string][]wire.Response{},
		waiters:   map[string]chan wire.Response{},
	}
	e.gw = kafkatest.Service(t, "gw")
	kafkatest.CreateTopics(t, kafkaenv.ReplyTopic(e.gw))
	if commandTopic {
		kafkatest.CreateTopics(t, kafkaenv.CommandTopic(e.service, partitions))
	}
	e.producer = kafkatest.Client(t, kgo.ProducerLinger(0))

	cons := kafkatest.Client(t,
		kgo.ConsumeTopics(wire.ReplyTopic(e.gw)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() { cancel(); <-done })
	go func() {
		defer close(done)
		for {
			fs := cons.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			fs.EachRecord(func(r *kgo.Record) {
				e.mu.Lock()
				bridgeKeys := e.bridgeKeys
				e.mu.Unlock()
				resp, err := wire.DecodeResponse(r, bridgeKeys)
				if err != nil {
					t.Errorf("undecodable response: %v", err)
					return
				}
				e.mu.Lock()
				e.responses[resp.RequestID] = append(e.responses[resp.RequestID], resp)
				if ch := e.waiters[resp.RequestID]; ch != nil {
					delete(e.waiters, resp.RequestID)
					ch <- resp
				}
				e.mu.Unlock()
			})
		}
	}()
	return e
}

func (e *env) spec() *apispec.Service {
	s, err := apispec.Load(e.service, e.specData)
	if err != nil {
		e.t.Fatal(err)
	}
	return s
}

func (e *env) config() bridge.Config {
	return bridge.Config{
		Service: e.service, Brokers: kafkatest.Brokers(e.t), Spec: e.spec(), Keys: e.keys, Signer: e.bridgeSigner,
		Upstream: e.upstream, Instance: "test",
		UpstreamTimeout: 10 * time.Second,
		// Crashed processes leave the group only when their session expires;
		// 6s is the broker minimum.
		SessionTimeout: 6 * time.Second,
		Log:            slog.New(slog.NewTextHandler(testWriter{e.t}, &slog.HandlerOptions{Level: slog.LevelInfo})),
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

// start runs an in-process bridge until the returned stop is called (a
// graceful shutdown) or the test ends.
func (e *env) start(cfg bridge.Config) (stop func()) {
	e.t.Helper()
	b, err := bridge.New(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	e.awaitMemory()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				e.t.Errorf("bridge run: %v", err)
			}
		})
	}
	e.t.Cleanup(stop)
	return stop
}

// awaitMemory waits until the bridge has recorded where its dedup memory
// starts: commands produced before that are never executed.
func (e *env) awaitMemory() {
	e.t.Helper()
	// Short metadata age: the topic may not exist at the first poll.
	adm := kadm.NewClient(kafkatest.Client(e.t, kgo.MetadataMinAge(50*time.Millisecond)))
	topic := bridge.LayoutTopic(e.service)
	kafkatest.Eventually(e.t, 60*time.Second, func() bool {
		ends, err := adm.ListEndOffsets(e.t.Context(), topic)
		if err != nil {
			return false
		}
		o, ok := ends.Lookup(topic, 0)
		return ok && o.Err == nil && o.Offset > 0
	}, "bridge memory started for %s", e.service)
}

// command builds a valid command for op, as the gateway would.
func (e *env) command(method, op, tmpl, path string, params map[string]string, body, idemKey string) wire.Command {
	now := time.Now().UTC()
	return wire.Command{
		V: wire.Version, RequestID: wire.NewRequestID(), Service: e.service, OperationID: op,
		Method: method, PathTemplate: tmpl, Path: path, PathParams: params,
		Caller:  wire.Caller{Application: "checkout", Instance: "checkout-1"},
		Headers: wire.Headers{"content-type": {"application/json"}}, Body: wire.NewBody([]byte(body)),
		IdempotencyKey: idemKey, ReplyTo: wire.ReplyTopic(e.gw),
		IssuedAt: now, Deadline: now.Add(10 * time.Second), ExpiresAt: now.Add(5 * time.Minute),
	}
}

func (e *env) charge(account, idemKey string) wire.Command {
	return e.command(http.MethodPost, "createCharge", "/charges", "/charges", nil,
		fmt.Sprintf(`{"accountId":%q,"amountCents":1250,"currency":"EUR"}`, account), idemKey)
}

func (e *env) send(cmd wire.Command) *kgo.Record {
	e.t.Helper()
	rec, err := wire.EncodeCommand(cmd, e.signer)
	if err != nil {
		e.t.Fatal(err)
	}
	e.produce(rec)
	return rec
}

func (e *env) produce(rec *kgo.Record) {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) responsesFor(id string) []wire.Response {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]wire.Response(nil), e.responses[id]...)
}

// waiter registers interest in id before it is sent; for latency
// measurements, where polling would add its own interval.
func (e *env) waiter(id string) <-chan wire.Response {
	ch := make(chan wire.Response, 1)
	e.mu.Lock()
	e.waiters[id] = ch
	e.mu.Unlock()
	return ch
}

func (e *env) responsesFor0() map[string][]wire.Response {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string][]wire.Response{}
	for k, v := range e.responses {
		out[k] = append([]wire.Response(nil), v...)
	}
	return out
}

func (e *env) await(id string, timeout time.Duration) wire.Response {
	e.t.Helper()
	kafkatest.Eventually(e.t, timeout, func() bool { return len(e.responsesFor(id)) > 0 }, "response for %s", id)
	return e.responsesFor(id)[0]
}

// results returns every Result published so far. It first pushes one
// barrier command through each partition and waits for its Response:
// partitions are processed in order, so everything sent before is settled.
// The barriers' own Results are excluded.
func (e *env) results() []wire.Result {
	e.t.Helper()
	hasher := kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service))
	barriers := map[string]bool{}
	for p := range partitions {
		for {
			cmd := e.command(http.MethodPost, "createCharge", "/charges", "/charges", nil,
				`{"accountId":"barrier","amountCents":1,"currency":"EUR"}`, "")
			if hasher.Partition(&kgo.Record{Key: []byte(cmd.DedupKey())}, partitions) != p {
				continue
			}
			e.send(cmd)
			barriers[cmd.RequestID] = true
			break
		}
	}
	for id := range barriers {
		e.await(id, 30*time.Second)
	}
	var out []wire.Result
	seen := 0
	kafkatest.Consume(e.t, wire.ResultTopic(e.service), 20*time.Second, func(rs []*kgo.Record) bool {
		for _, r := range rs[len(out)+seen:] {
			res, err := wire.DecodeResult(r, e.bridgeKeys)
			if err != nil {
				e.t.Fatal(err)
			}
			if barriers[res.Command.RequestID] {
				seen++
				continue
			}
			out = append(out, res)
		}
		return seen == len(barriers)
	})
	return out
}

func resultsFor(rs []wire.Result, id string) []wire.Result {
	var out []wire.Result
	for _, r := range rs {
		if r.Command.RequestID == id {
			out = append(out, r)
		}
	}
	return out
}

// countRaw counts records for requestId on the reply topic including those
// of aborted transactions (read_uncommitted). It returns as soon as want are
// seen, or what it found after timeout.
func (e *env) countRaw(id string, want int, timeout time.Duration) int {
	e.t.Helper()
	cl := kafkatest.Client(e.t, kgo.ConsumeTopics(wire.ReplyTopic(e.gw)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	n := 0
	for n < want {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		fs.EachRecord(func(r *kgo.Record) {
			if string(r.Key) == id {
				n++
			}
		})
	}
	return n
}

// paymentsB is the real payments service with its debit counter.
func paymentsB(t *testing.T) (*payments.Service, *httptest.Server) {
	svc := payments.New()
	srv := httptest.NewServer(svc.Handler())
	t.Cleanup(srv.Close)
	return svc, srv
}

// --- subprocess bridge, for real crashes (SIGKILL) and freezes (SIGSTOP) ---

const helperEnv = "HOK_BRIDGE_TEST_HELPER"

// TestBridgeProcess is not a test: it is the body of a bridge subprocess
// started by proc. HOK_CRASH_AT names a hook at which the process kills
// itself with SIGKILL the first time it fires: no defers, no flush, no
// graceful anything.
func TestBridgeProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("subprocess body")
	}
	keys, err := identity.TrustedKeysFromEnv(identity.RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := identity.SignerFromEnv(identity.RoleBridge)
	if err != nil {
		t.Fatal(err)
	}
	service := os.Getenv("HOK_SERVICE")
	data := api.Payments
	if os.Getenv("HOK_SPEC") == "orders" {
		data = api.Orders
	}
	spec, err := apispec.Load(service, data)
	if err != nil {
		t.Fatal(err)
	}
	session, err := time.ParseDuration(os.Getenv("HOK_SESSION_TIMEOUT"))
	if err != nil || session == 0 {
		session = 6 * time.Second
	}
	die := func(string) {
		fmt.Fprintln(os.Stderr, "CRASHING at", os.Getenv("HOK_CRASH_AT"))
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	}
	maxInFlight, _ := strconv.Atoi(os.Getenv("HOK_MAX_INFLIGHT"))
	var hooks bridge.Hooks
	switch os.Getenv("HOK_CRASH_AT") {
	case "before-started-commit":
		hooks.BeforeStartedCommit = die
	case "after-started":
		hooks.AfterStarted = die
	case "after-upstream":
		hooks.AfterUpstream = die
	case "before-commit":
		hooks.BeforeCommit = die
	case "after-commit":
		hooks.AfterCommit = die
	}
	b, err := bridge.New(bridge.Config{
		Service: service, Brokers: kafkatest.Brokers(t), Spec: spec, Keys: keys, Signer: signer,
		Upstream: os.Getenv("HOK_UPSTREAM"), Instance: "proc",
		SessionTimeout: session, Hooks: hooks, MaxInFlight: maxInFlight,
		Log: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Exit when the parent closes stdin.
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	if err := b.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

type proc struct {
	cmd  *exec.Cmd
	exit chan error
	mu   sync.Mutex
	out  strings.Builder
}

func (p *proc) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.Write(b)
}

func (p *proc) log() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func (e *env) proc(spec, crashAt string, session time.Duration) *proc {
	e.t.Helper()
	c := exec.Command(os.Args[0], "-test.run=^TestBridgeProcess$", "-test.v")
	var base []string
	for _, kv := range os.Environ() {
		// The bridge must never hold the gateway's private key.
		if !strings.HasPrefix(kv, identity.SigningKeyEnv(identity.RoleGateway)+"=") {
			base = append(base, kv)
		}
	}
	c.Env = append(base,
		helperEnv+"=1",
		kafkaenv.BrokersEnv+"="+strings.Join(kafkatest.Brokers(e.t), ","),
		identity.TrustedKeysEnv(identity.RoleGateway)+"="+e.trusted,
		identity.SigningKeyEnv(identity.RoleBridge)+"="+e.bridgeSigning,
		"HOK_SERVICE="+e.service,
		"HOK_SPEC="+spec,
		"HOK_UPSTREAM="+e.upstream,
		"HOK_CRASH_AT="+crashAt,
		"HOK_SESSION_TIMEOUT="+session.String(),
		"HOK_MAX_INFLIGHT="+os.Getenv("HOK_MAX_INFLIGHT"),
	)
	p := &proc{cmd: c, exit: make(chan error, 1)}
	defer e.awaitMemory()
	c.Stdout = io.MultiWriter(testWriter{e.t}, p)
	c.Stderr = c.Stdout
	stdin, err := c.StdinPipe()
	if err != nil {
		e.t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		e.t.Fatal(err)
	}
	go func() { p.exit <- c.Wait() }()
	e.t.Cleanup(func() {
		_ = stdin.Close()
		_ = c.Process.Signal(syscall.SIGCONT)
		select {
		case <-p.exit:
		case <-time.After(30 * time.Second):
			_ = c.Process.Kill()
		}
	})
	return p
}

func (p *proc) exited() bool {
	select {
	case err := <-p.exit:
		p.exit <- err
		return true
	default:
		return false
	}
}

func decode[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("%v: %s", err, b)
	}
	return v
}
