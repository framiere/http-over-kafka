//go:build e2e

// Package e2e attacks the assembled system from the outside: real gateway,
// bridge, deriver and audit binaries, real demo services, a real broker
// (e2e/compose.yaml). Nothing here imports a component's internals to drive
// it; internal packages are used only to play a client (mint a JWT), an
// operator (read topics) or an attacker (forge records).
//
//	docker compose -f e2e/compose.yaml up -d --wait
//	go test -tags e2e ./e2e -count=1 -v -timeout 30m
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/devidp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers        = envOr("E2E_KAFKA", "localhost:19092")
	kafkaContainer = envOr("E2E_KAFKA_CONTAINER", "kb-e2e-kafka")
	repoRoot       string
	binDir         string
	devEnv         map[string]string // deploy/*.env merged
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// lockPath serializes every e2e run on this machine: tests pause, kill and
// restart the shared broker, so two concurrent runs would produce each
// other's failures. The lock is held until the process exits.
var lockPath = envOr("E2E_LOCK", "/tmp/kb-e2e.lock") // fixed path: TMPDIR differs between shells

func TestMain(m *testing.M) {
	lf, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		panic(err)
	}
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: another run holds %s, waiting for it\n", lockPath)
		if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
			panic(err)
		}
	}
	wd, _ := os.Getwd()
	repoRoot = filepath.Dir(wd)
	dir, err := os.MkdirTemp("", "kb-e2e-bin")
	if err != nil {
		panic(err)
	}
	binDir = dir
	for _, c := range []string{"gateway", "bridge", "deriver", "audit", "payments", "orders"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(binDir, c), "./cmd/"+c)
		cmd.Dir = repoRoot
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", c, err, out)
			os.Exit(2)
		}
	}
	devEnv = map[string]string{}
	envFiles, _ := filepath.Glob(filepath.Join(repoRoot, "deploy", "*.env"))
	for _, f := range envFiles {
		readEnvFile(f, devEnv)
	}
	for _, k := range []string{"KB_GATEWAY_SIGNING_KEY", "KB_TRUSTED_GATEWAY_KEYS", "KB_BRIDGE_SIGNING_KEY", "KB_TRUSTED_BRIDGE_KEYS", "KB_JWT_KEYS", "KB_DEV_IDP_KEY"} {
		if devEnv[k] == "" {
			fmt.Fprintf(os.Stderr, "e2e: %s not found in deploy/*.env\n", k)
			os.Exit(2)
		}
	}
	code := m.Run()
	// The lock lives as long as lf's descriptor: without this, the GC may
	// finalize lf mid-run, close it and silently release the lock.
	runtime.KeepAlive(lf)
	os.RemoveAll(binDir)
	os.Exit(code)
}

func readEnvFile(path string, into map[string]string) {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if k, v, ok := strings.Cut(line, "="); ok && !strings.HasPrefix(line, "#") {
			into[k] = v
		}
	}
}

// ---------- processes ----------

type proc struct {
	t    *testing.T
	name string
	bin  string
	env  []string

	mu     sync.Mutex
	cmd    *exec.Cmd
	out    bytes.Buffer // every generation's output, appended
	file   *os.File
	exited chan struct{}
}

type syncWriter struct{ p *proc }

func (w syncWriter) Write(b []byte) (int, error) {
	w.p.mu.Lock()
	defer w.p.mu.Unlock()
	w.p.out.Write(b)
	return w.p.file.Write(b)
}

func newProc(t *testing.T, name, bin string, env map[string]string) *proc {
	t.Helper()
	logDir := filepath.Join(repoRoot, "e2e", "out", t.Name())
	_ = os.MkdirAll(logDir, 0o755)
	f, err := os.Create(filepath.Join(logDir, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	p := &proc{t: t, name: name, bin: filepath.Join(binDir, bin), file: f}
	for k, v := range env {
		p.env = append(p.env, k+"="+v)
	}
	t.Cleanup(func() {
		p.kill()
		f.Close()
	})
	return p
}

func (p *proc) start() *proc {
	p.t.Helper()
	cmd := exec.Command(p.bin)
	cmd.Env = append(os.Environ(), p.env...)
	cmd.Stdout = syncWriter{p}
	cmd.Stderr = syncWriter{p}
	if err := cmd.Start(); err != nil {
		p.t.Fatalf("start %s: %v", p.name, err)
	}
	fmt.Fprintf(syncWriter{p}, "=== e2e: started %s pid %d at %s\n", p.name, cmd.Process.Pid, time.Now().Format(time.RFC3339Nano))
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()
	p.mu.Lock()
	p.cmd, p.exited = cmd, exited
	p.mu.Unlock()
	return p
}

func (p *proc) signal(s syscall.Signal) {
	p.mu.Lock()
	cmd := p.cmd
	p.mu.Unlock()
	if cmd != nil && cmd.Process != nil {
		_ = cmd.Process.Signal(s)
	}
}

// kill is kill -9: no cleanup, no graceful handover.
func (p *proc) kill() {
	p.mu.Lock()
	cmd, exited := p.cmd, p.exited
	p.mu.Unlock()
	if cmd == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGCONT) // a stopped process must die too
	_ = cmd.Process.Kill()
	<-exited
	fmt.Fprintf(syncWriter{p}, "=== e2e: SIGKILL %s at %s\n", p.name, time.Now().Format(time.RFC3339Nano))
	p.mu.Lock()
	p.cmd = nil
	p.mu.Unlock()
}

// term is a graceful SIGTERM, waited for up to d.
func (p *proc) term(d time.Duration) {
	p.mu.Lock()
	cmd, exited := p.cmd, p.exited
	p.mu.Unlock()
	if cmd == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-exited:
	case <-time.After(d):
		p.t.Logf("%s did not stop within %s after SIGTERM; killing", p.name, d)
		p.kill()
		return
	}
	p.mu.Lock()
	p.cmd = nil
	p.mu.Unlock()
}

// alive reports whether the process started last is still running.
func (p *proc) alive() bool {
	p.mu.Lock()
	cmd, exited := p.cmd, p.exited
	p.mu.Unlock()
	if cmd == nil {
		return false
	}
	select {
	case <-exited:
		return false
	default:
		return true
	}
}

func (p *proc) logs() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

func (p *proc) mark() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.Len()
}

// countSince counts occurrences of substr in output written after mark.
func (p *proc) countSince(mark int, substr string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Count(p.out.String()[mark:], substr)
}

// ---------- polling ----------

// eventually polls cond every 50ms until it holds or d elapses.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for: %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stable asserts cond holds continuously for d (used to prove "no further
// effect": a debit that does not appear within d after everything settled).
func stable(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("violated: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ---------- ports, names ----------

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func randName(prefix string) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 6)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return prefix + string(b)
}

// ---------- services and specs ----------

// svc is one B, under a unique service name so each test has its own
// topics, consumer groups and transactional ids.
type svc struct {
	name string
	kind string // payments, orders, echo
	url  string // B's base URL
	proc *proc  // nil for in-process echo
}

func writeSpec(t *testing.T, dir, name, kind string) {
	t.Helper()
	var data []byte
	switch kind {
	case "payments":
		data, _ = os.ReadFile(filepath.Join(repoRoot, "api", "payments.openapi.yaml"))
	case "orders":
		b, _ := os.ReadFile(filepath.Join(repoRoot, "api", "orders.openapi.yaml"))
		data = bytes.Replace(b, []byte("topic: orders.events"), []byte("topic: "+name+".events"), 1)
	case "payments-events":
		// Operator-side config only: B is the unchanged payments binary.
		b, _ := os.ReadFile(filepath.Join(repoRoot, "api", "payments.openapi.yaml"))
		mapping := "        '415': { $ref: '#/components/responses/Problem' }\n" +
			"      x-conduktor-event:\n        on: 201\n        topic: " + name + ".events\n        type: ChargeCreated\n" +
			"        key: $.response.body.id\n        value:\n          id: $.response.body.id\n          accountId: $.request.body.accountId\n"
		data = bytes.Replace(b, []byte("        '415': { $ref: '#/components/responses/Problem' }\n"), []byte(mapping), 1)
	case "echo":
		data = []byte(echoSpec)
	case "echo-secured":
		data = []byte(echoSecuredSpec)
	default:
		t.Fatalf("unknown kind %s", kind)
	}
	if err := os.WriteFile(filepath.Join(dir, name+".openapi.yaml"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func startPayments(t *testing.T, specDir string, delay time.Duration) *svc {
	return startPaymentsSpec(t, specDir, delay, "payments")
}

func startPaymentsSpec(t *testing.T, specDir string, delay time.Duration, kind string) *svc {
	name := randName("pay")
	writeSpec(t, specDir, name, kind)
	port := freePort(t)
	p := newProc(t, name, "payments", map[string]string{"ADDR": fmt.Sprintf("127.0.0.1:%d", port), "PAYMENTS_DELAY": delay.String()}).start()
	s := &svc{name: name, kind: "payments", url: fmt.Sprintf("http://127.0.0.1:%d", port), proc: p}
	waitHTTP(t, s.url+"/accounts/probe")
	return s
}

func startOrders(t *testing.T, specDir string) *svc {
	name := randName("ord")
	writeSpec(t, specDir, name, "orders")
	port := freePort(t)
	p := newProc(t, name, "orders", map[string]string{"ADDR": fmt.Sprintf("127.0.0.1:%d", port)}).start()
	s := &svc{name: name, kind: "orders", url: fmt.Sprintf("http://127.0.0.1:%d", port), proc: p}
	waitHTTP(t, s.url+"/orders")
	return s
}

func waitHTTP(t *testing.T, url string) {
	t.Helper()
	eventually(t, 10*time.Second, "HTTP up: "+url, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	})
}

// debits is the ground truth: how many times payments really ran a charge.
func debits(t *testing.T, s *svc, account string) int {
	t.Helper()
	resp, err := http.Get(s.url + "/accounts/" + account)
	if err != nil {
		t.Fatalf("payments ground truth unreachable: %v", err)
	}
	defer resp.Body.Close()
	var a struct {
		DebitCount int `json:"debitCount"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&a); err != nil {
		t.Fatal(err)
	}
	return a.DebitCount
}

// ---------- gateway, bridge, deriver, audit ----------

type gw struct {
	*proc
	instance string
	url      string
	admin    string
}

func newGateway(t *testing.T, specDir string, timeout time.Duration, services ...*svc) *gw {
	return newGatewayEnv(t, specDir, timeout, nil, services...)
}

func newGatewayEnv(t *testing.T, specDir string, timeout time.Duration, extra map[string]string, services ...*svc) *gw {
	t.Helper()
	var entries []string
	for _, s := range services {
		entries = append(entries, s.name+"="+s.url)
	}
	port, admin := freePort(t), freePort(t)
	inst := randName("gw-")
	env := map[string]string{
		"KAFKA_BROKERS":          brokers,
		"KB_GATEWAY_INSTANCE":    inst,
		"KB_GATEWAY_SIGNING_KEY": devEnv["KB_GATEWAY_SIGNING_KEY"],
		"KB_TRUSTED_BRIDGE_KEYS": devEnv["KB_TRUSTED_BRIDGE_KEYS"],
		"KB_JWT_ISSUER":          devEnv["KB_JWT_ISSUER"],
		"KB_JWT_AUDIENCE":        devEnv["KB_JWT_AUDIENCE"],
		"KB_JWT_KEYS":            devEnv["KB_JWT_KEYS"],
		"KB_SERVICES":            strings.Join(entries, ","),
		"KB_SPEC_DIR":            specDir,
		"KB_REQUEST_TIMEOUT":     timeout.String(),
		"ADDR":                   fmt.Sprintf("127.0.0.1:%d", port),
		"KB_ADMIN_ADDR":          fmt.Sprintf("127.0.0.1:%d", admin),
	}
	for k, v := range extra {
		env[k] = v
	}
	p := newProc(t, inst, "gateway", env)
	return &gw{proc: p, instance: inst, url: fmt.Sprintf("http://127.0.0.1:%d", port), admin: fmt.Sprintf("http://127.0.0.1:%d", admin)}
}

func (g *gw) startReady(t *testing.T) *gw {
	t.Helper()
	g.start()
	eventually(t, 30*time.Second, "gateway ready "+g.instance, func() bool {
		resp, err := http.Get(g.admin + "/readyz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	})
	return g
}

func newBridge(t *testing.T, specDir string, s *svc, instance string) *proc {
	return newBridgeEnv(t, specDir, s, instance, nil)
}

func newBridgeEnv(t *testing.T, specDir string, s *svc, instance string, extra map[string]string) *proc {
	t.Helper()
	env := map[string]string{
		"KAFKA_BROKERS":           brokers,
		"KB_TRUSTED_GATEWAY_KEYS": devEnv["KB_TRUSTED_GATEWAY_KEYS"],
		"KB_BRIDGE_SIGNING_KEY":   devEnv["KB_BRIDGE_SIGNING_KEY"],
		"KB_SERVICE":              s.name,
		"KB_UPSTREAM":             s.url,
		"KB_SPEC_DIR":             specDir,
		"KB_BRIDGE_INSTANCE":      instance,
	}
	for k, v := range extra {
		env[k] = v
	}
	return newProc(t, "bridge-"+instance, "bridge", env)
}

const partitions = 6 // KB_PARTITIONS default, used by every binary

// startBridgeReady starts b and waits until it owns and restored all
// partitions (only valid when it is the only bridge of its service).
func startBridgeReady(t *testing.T, b *proc) {
	startBridgeReadyN(t, b, partitions)
}

func startBridgeReadyN(t *testing.T, b *proc, n int) {
	t.Helper()
	m := b.mark()
	b.start()
	eventually(t, 90*time.Second, b.name+" restored its partitions", func() bool {
		return b.countSince(m, `"msg":"partition ready"`) >= n
	})
}

// docker pauses/unpauses the broker container (SIGSTOP of the JVM: open
// connections stay up, nothing answers).
func docker(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		t.Fatalf("docker %v: %v %s", args, err, out)
	}
}

func pauseKafka(t *testing.T) {
	docker(t, "pause", kafkaContainer)
	t.Cleanup(func() { _ = exec.Command("docker", "unpause", kafkaContainer).Run() })
}

func unpauseKafka(t *testing.T) { docker(t, "unpause", kafkaContainer) }

func newDeriver(t *testing.T, specDir string, s *svc, instance string) *proc {
	return newProc(t, "deriver-"+instance, "deriver", map[string]string{
		"KAFKA_BROKERS":          brokers,
		"KB_SERVICE":             s.name,
		"KB_SPEC_DIR":            specDir,
		"KB_DERIVER_INSTANCE":    instance,
		"KB_TRUSTED_BRIDGE_KEYS": devEnv["KB_TRUSTED_BRIDGE_KEYS"],
	})
}

// ---------- client ----------

type caller struct {
	app, instance string
	token         string
	hc            *http.Client
}

func newCaller(t *testing.T, app string) *caller {
	t.Helper()
	kid, b64, _ := strings.Cut(devEnv["KB_DEV_IDP_KEY"], ":")
	seed, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := devidp.Token{
		KeyID: kid, Key: ed25519.NewKeyFromSeed(seed),
		Issuer: devEnv["KB_JWT_ISSUER"], Audience: devEnv["KB_JWT_AUDIENCE"],
		Application: app, Instance: app + "-1", IssuedAt: time.Now(), TTL: 2 * time.Hour,
	}.Sign()
	if err != nil {
		t.Fatal(err)
	}
	return &caller{app: app, instance: app + "-1", token: tok, hc: &http.Client{Timeout: 2 * time.Minute}}
}

type reply struct {
	status  int
	header  http.Header
	body    []byte
	err     error
	elapsed time.Duration
}

func (r reply) problemType() string {
	var p struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(r.body, &p)
	return p.Type
}

func (r reply) String() string {
	if r.err != nil {
		return "transport error: " + r.err.Error()
	}
	return fmt.Sprintf("%d %s", r.status, bytes.TrimSpace(r.body))
}

type req struct {
	method, path string
	body         string
	header       map[string]string
}

// via calls through gateway g as service host.
func (c *caller) via(g *gw, host string, r req) reply {
	return c.do(g.url, host, true, r)
}

// direct calls B itself, as A would without the gateway.
func (c *caller) direct(base string, r req) reply {
	return c.do(base, "", false, r)
}

func (c *caller) do(base, host string, auth bool, r req) reply {
	var body io.Reader
	if r.body != "" {
		body = strings.NewReader(r.body)
	}
	hr, err := http.NewRequestWithContext(context.Background(), r.method, base+r.path, body)
	if err != nil {
		return reply{err: err}
	}
	if r.body != "" {
		hr.Header.Set("Content-Type", "application/json")
	}
	if auth {
		hr.Header.Set("Authorization", "Bearer "+c.token)
	}
	if host != "" {
		hr.Host = host
	}
	for k, v := range r.header {
		hr.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := c.hc.Do(hr)
	if err != nil {
		return reply{err: err, elapsed: time.Since(start)}
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	return reply{status: resp.StatusCode, header: resp.Header, body: b, err: err, elapsed: time.Since(start)}
}

func charge(account string) string {
	return fmt.Sprintf(`{"accountId":%q,"amountCents":100,"currency":"EUR"}`, account)
}

// ---------- kafka (operator / attacker view) ----------

func kclient(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// readTopic returns every record of topic present now. It reads until every
// partition reached the end offset seen at call time, or nothing new arrived
// for 1.5s (trailing transaction markers and aborted batches yield no record,
// so the last record can sit below the end offset).
// readCommitted=false shows what a naive consumer sees, aborted data included.
func readTopic(t *testing.T, topic string, readCommitted bool) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	adm := kadm.NewClient(kclient(t))
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("end offsets %s: %v", topic, err)
	}
	want := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil && o.Offset > 0 {
			want[o.Partition] = o.Offset
		}
	})
	if len(want) == 0 {
		return nil
	}
	iso := kgo.ReadUncommitted()
	if readCommitted {
		iso = kgo.ReadCommitted()
	}
	cl := kclient(t, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(iso), kgo.FetchMaxWait(300*time.Millisecond))
	var out []*kgo.Record
	last := map[int32]int64{}
	idleSince := time.Now()
	for {
		done := true
		for p, end := range want {
			if l, ok := last[p]; !ok || l+1 < end {
				done = false
			}
		}
		if done || time.Since(idleSince) > 1500*time.Millisecond {
			return out
		}
		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fs := cl.PollFetches(pctx)
		pcancel()
		if ctx.Err() != nil {
			t.Fatalf("reading %s: %v", topic, ctx.Err())
		}
		n := 0
		fs.EachRecord(func(r *kgo.Record) {
			out = append(out, r)
			last[r.Partition] = r.Offset
			n++
		})
		if n > 0 {
			idleSince = time.Now()
		}
	}
}

func listTopics(t *testing.T) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	td, err := kadm.NewClient(kclient(t)).ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return td.Names()
}

// endOffsets of every non-internal topic, summed per topic.
func endOffsets(t *testing.T) map[string]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	adm := kadm.NewClient(kclient(t))
	ends, err := adm.ListEndOffsets(ctx, listTopics(t)...)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	ends.Each(func(o kadm.ListedOffset) { out[o.Topic] += o.Offset })
	return out
}

func header(r *kgo.Record, k string) string {
	for _, h := range r.Headers {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}
