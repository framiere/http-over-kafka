package bridge_test

import (
	"encoding/json"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/demo/orders"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// A bridge process is SIGKILLed at each point of the processing of a charge,
// then another bridge takes over. B (payments) has no idempotency of its own:
// debitCount is the ground truth of how many times it executed.
func TestCrashAtEveryStep(t *testing.T) {
	cases := []struct {
		at      string
		crashed int          // B executions when the process dies
		final   int          // B executions at the end
		outcome wire.Outcome // what the caller is eventually told
	}{
		// "started" never committed: provably not run, so the next owner executes.
		{"before-started-commit", 0, 1, wire.OutcomeSucceeded},
		// "started" committed, B not called: nobody can prove it, so unknown.
		{"after-started", 0, 0, wire.OutcomeUnknown},
		{"after-upstream", 1, 1, wire.OutcomeUnknown},
		{"before-commit", 1, 1, wire.OutcomeUnknown},
		{"after-commit", 1, 1, wire.OutcomeSucceeded},
	}
	for _, c := range cases {
		t.Run(c.at, func(t *testing.T) {
			pay, srv := paymentsB(t)
			e := newEnv(t, "crash", api.Payments, srv.URL)
			acc := "acc-" + c.at
			cmd := e.charge(acc, "key-"+c.at)

			p := e.proc("payments", c.at, 0)
			e.send(cmd)
			kafkatest.Eventually(t, 60*time.Second, p.exited, "bridge process killed at %s", c.at)
			if n := pay.Account(acc).DebitCount; n != c.crashed {
				t.Fatalf("debits after crash: %d, want %d", n, c.crashed)
			}
			committed := len(e.responsesFor(cmd.RequestID))
			if c.at == "before-commit" {
				// The outcome is physically on the reply topic, in a
				// transaction that will never commit: only a read_committed
				// gateway is safe from it.
				if n := e.countRaw(cmd.RequestID, 1, 10*time.Second); n != 1 {
					t.Fatalf("uncommitted response records: %d", n)
				}
			}

			e.start(e.config())
			got := e.await(cmd.RequestID, 60*time.Second)
			rs := e.results()
			if n := len(e.responsesFor(cmd.RequestID)); n != 1 {
				t.Fatalf("%d committed responses (before restart: %d)", n, committed)
			}
			res := resultsFor(rs, cmd.RequestID)
			if len(res) != 1 || res[0].Outcome != c.outcome || wire.OutcomeOf(got) != c.outcome {
				t.Fatalf("outcome: response %+v, results %+v", got, res)
			}
			if c.outcome == wire.OutcomeUnknown && (got.Status != 502 || got.Fault != wire.FaultOutcomeUnknown) {
				t.Fatalf("unknown must be explicit: %+v", got)
			}

			// The caller retries with the same key: same answer, no new debit.
			retry := e.charge(acc, "key-"+c.at)
			e.send(retry)
			r := e.await(retry.RequestID, 30*time.Second)
			if r.ReplayOf != cmd.RequestID || r.Status != got.Status {
				t.Fatalf("retry: %+v", r)
			}
			if n := pay.Account(acc).DebitCount; n != c.final {
				t.Fatalf("debits at the end: %d, want %d", n, c.final)
			}
			t.Logf("crash %-21s → debits at crash=%d, at end=%d; caller got %d %s; results=%d; retry got %d replayOf=%s",
				c.at, c.crashed, c.final, got.Status, c.outcome, len(res), r.Status, r.ReplayOf)
		})
	}
}

// SIGKILL from outside while B is executing the charge (no hook involved).
func TestKilledDuringUpstreamCall(t *testing.T) {
	pay, srv := paymentsB(t)
	pay.SetDelay(3 * time.Second)
	e := newEnv(t, "crash", api.Payments, srv.URL)
	p := e.proc("payments", "", 0)
	cmd := e.charge("acc-kill", "")
	e.send(cmd)
	kafkatest.Eventually(t, 60*time.Second, func() bool { return pay.Account("acc-kill").DebitCount == 1 }, "B executing")
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	kafkatest.Eventually(t, 10*time.Second, p.exited, "killed")

	pay.SetDelay(0)
	e.start(e.config())
	got := e.await(cmd.RequestID, 60*time.Second)
	if got.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("got %+v", got)
	}
	if res := resultsFor(e.results(), cmd.RequestID); len(res) != 1 || res[0].Outcome != wire.OutcomeUnknown {
		t.Fatalf("results %+v", res)
	}
	if n := pay.Account("acc-kill").DebitCount; n != 1 {
		t.Fatalf("debits %d", n)
	}
}

// PUT is idempotent by HTTP contract (D3): after a crash with an unknown
// outcome, the next owner calls B again and relays B's real answer.
func TestReplayableOperationIsReexecutedAfterCrash(t *testing.T) {
	svc := orders.New()
	srv := httptestServer(t, svc.Handler())
	e := newEnv(t, "ord", api.Orders, srv)
	created := createOrder(t, srv)

	put := e.command(http.MethodPut, "replaceOrder", "/orders/{orderId}", "/orders/"+created.ID,
		map[string]string{"orderId": created.ID}, `{"customerId":"c1","items":[{"sku":"B","quantity":3}]}`, "")
	p := e.proc("orders", "after-upstream", 0)
	e.send(put)
	kafkatest.Eventually(t, 60*time.Second, p.exited, "killed after B answered")
	if v := getOrder(t, srv, created.ID).Version; v != 2 {
		t.Fatalf("version after first execution %d", v)
	}

	e.start(e.config())
	got := e.await(put.RequestID, 60*time.Second)
	if got.Status != 200 || got.Fault != "" {
		t.Fatalf("got %+v", got)
	}
	o := decode[orders.Order](t, got.Body.Bytes())
	if o.Version != 3 || getOrder(t, srv, created.ID).Version != 3 {
		t.Fatalf("expected an explicit second execution (version 3), response %+v", o)
	}
	if res := resultsFor(e.results(), put.RequestID); len(res) != 1 || res[0].Outcome != wire.OutcomeSucceeded {
		t.Fatalf("results %+v", res)
	}
}

// A bridge freezes (SIGSTOP) while B executes, long enough to lose its
// partitions; another bridge takes over and answers "unknown". Then the
// frozen one wakes up holding B's real answer: it must not publish it.
func TestFrozenZombieCannotPublish(t *testing.T) {
	pay, srv := paymentsB(t)
	pay.SetDelay(time.Second)
	e := newEnv(t, "zombie", api.Payments, srv.URL)
	const session = 6 * time.Second // broker minimum
	z := e.proc("payments", "", session)
	cmd := e.charge("acc-zombie", "key-zombie")
	e.send(cmd)
	kafkatest.Eventually(t, 60*time.Second, func() bool { return pay.Account("acc-zombie").DebitCount == 1 }, "B executing")
	if err := z.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}

	cfg := e.config()
	cfg.SessionTimeout = session
	e.start(cfg)
	got := e.await(cmd.RequestID, 90*time.Second)
	if got.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("takeover answer: %+v", got)
	}

	if err := z.cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	// Let the zombie run until it has given up the partition, then stop it.
	kafkatest.Eventually(t, 60*time.Second, func() bool {
		return strings.Contains(z.log(), "partitions released") || strings.Contains(z.log(), "processing failed")
	}, "zombie noticed it lost the partition")
	_ = z.cmd.Process.Signal(syscall.SIGTERM)

	rs := e.results()
	if n := len(e.responsesFor(cmd.RequestID)); n != 1 {
		t.Fatalf("%d responses: the zombie published", n)
	}
	if res := resultsFor(rs, cmd.RequestID); len(res) != 1 || res[0].Outcome != wire.OutcomeUnknown {
		t.Fatalf("results %+v", res)
	}
	if n := pay.Account("acc-zombie").DebitCount; n != 1 {
		t.Fatalf("debits %d", n)
	}
	retry := e.charge("acc-zombie", "key-zombie")
	e.send(retry)
	if r := e.await(retry.RequestID, 30*time.Second); r.ReplayOf != cmd.RequestID || r.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("retry %+v", r)
	}
}

func httptestServer(t *testing.T, h http.Handler) string {
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

func createOrder(t *testing.T, base string) orders.Order {
	t.Helper()
	resp, err := http.Post(base+"/orders", "application/json", strings.NewReader(`{"customerId":"c1","items":[{"sku":"A","quantity":1}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var o orders.Order
	if err := json.NewDecoder(resp.Body).Decode(&o); err != nil || resp.StatusCode != 201 {
		t.Fatalf("create: %d %v", resp.StatusCode, err)
	}
	return o
}

func getOrder(t *testing.T, base, id string) orders.Order {
	t.Helper()
	resp, err := http.Get(base + "/orders/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return decode[orders.Order](t, b)
}

// The bridge's transaction is fenced from outside while it still owns the
// partition (what a zombie's successor does, or an ambiguous commit error):
// the bridge must recover from Kafka and publish what it really knows, not
// fall back to "unknown" and not call B again.
func TestCommitFailureWhileOwnerKeepsRealOutcome(t *testing.T) {
	for _, phase := range []string{"started", "outcome"} {
		t.Run(phase, func(t *testing.T) {
			pay, srv := paymentsB(t)
			e := newEnv(t, "fence", api.Payments, srv.URL)
			acc := "acc-fence-" + phase
			cmd := e.charge(acc, "")
			p := kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service)).
				Partition(&kgo.Record{Key: []byte(cmd.DedupKey())}, partitions)
			var once sync.Once
			intrude := func(string) {
				once.Do(func() {
					// Same transactional id as the bridge's producer for p.
					cl := kafkatest.Client(t, kgo.TransactionalID(fmt.Sprintf("hok-bridge.%s.%d", e.service, p)),
						kgo.RecordPartitioner(kgo.ManualPartitioner()))
					if err := cl.BeginTransaction(); err != nil {
						t.Error(err)
						return
					}
					// Another instance of the same bridge (it holds the key),
					// as a zombie's successor would be.
					rec := signedState(e, int32(p), "#fence", []byte(`{"instance":"intruder"}`))
					if err := cl.ProduceSync(t.Context(), rec).FirstErr(); err != nil {
						t.Error(err)
					}
					if err := cl.EndTransaction(t.Context(), kgo.TryCommit); err != nil {
						t.Error(err)
					}
				})
			}
			cfg := e.config()
			if phase == "started" {
				cfg.Hooks.BeforeStartedCommit = intrude
			} else {
				cfg.Hooks.BeforeCommit = intrude
			}
			e.start(cfg)
			e.send(cmd)
			got := e.await(cmd.RequestID, 60*time.Second)
			if got.Status != 201 || got.Fault != "" {
				t.Fatalf("got %+v", got)
			}
			if res := resultsFor(e.results(), cmd.RequestID); len(res) != 1 || res[0].Outcome != wire.OutcomeSucceeded {
				t.Fatalf("results %+v", res)
			}
			if n := len(e.responsesFor(cmd.RequestID)); n != 1 {
				t.Fatalf("%d responses", n)
			}
			if n := pay.Account(acc).DebitCount; n != 1 {
				t.Fatalf("debits %d", n)
			}
		})
	}
}

// SIGKILL in the middle of concurrent load: many lanes are between "started"
// and their outcome, settled offsets have gaps. After the takeover every
// command has exactly one Response and one Result, nothing ran twice, and
// every unknown outcome corresponds to a command that was in flight.
func TestKilledUnderConcurrentLoad(t *testing.T) {
	pay, srv := paymentsB(t)
	pay.SetDelay(50 * time.Millisecond)
	e := newEnv(t, "crash", api.Payments, srv.URL)
	t.Setenv("HOK_MAX_INFLIGHT", "4") // so that some are done, some in flight, some pending
	p := e.proc("payments", "", 0)

	const n = 120
	var cmds []wire.Command
	for i := range n {
		key := ""
		if i%3 == 0 {
			key = fmt.Sprintf("load-%d", i)
		}
		c := e.charge(fmt.Sprintf("acc-cl-%d", i), key)
		cmds = append(cmds, c)
		e.send(c)
	}
	debits := func() (total int) {
		for i := range n {
			total += pay.Account(fmt.Sprintf("acc-cl-%d", i)).DebitCount
		}
		return total
	}
	kafkatest.Eventually(t, 60*time.Second, func() bool { return debits() >= 20 }, "load under way")
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	kafkatest.Eventually(t, 10*time.Second, p.exited, "killed")
	atKill := debits()

	e.start(e.config())
	unknown := 0
	for _, c := range cmds {
		r := e.await(c.RequestID, 60*time.Second)
		if r.Fault == wire.FaultOutcomeUnknown {
			unknown++
		} else if r.Status != 201 {
			t.Errorf("%s: %+v", c.RequestID, r)
		}
	}
	rs := e.results()
	for i, c := range cmds {
		if got := len(e.responsesFor(c.RequestID)); got != 1 {
			t.Errorf("%s: %d responses", c.RequestID, got)
		}
		res := resultsFor(rs, c.RequestID)
		if len(res) != 1 {
			t.Errorf("%s: %d results", c.RequestID, len(res))
			continue
		}
		d := pay.Account(fmt.Sprintf("acc-cl-%d", i)).DebitCount
		switch res[0].Outcome {
		case wire.OutcomeSucceeded:
			if d != 1 {
				t.Errorf("acc-cl-%d: succeeded with %d debits", i, d)
			}
		case wire.OutcomeUnknown:
			if d > 1 {
				t.Errorf("acc-cl-%d: unknown with %d debits", i, d)
			}
		default:
			t.Errorf("acc-cl-%d: outcome %s", i, res[0].Outcome)
		}
	}
	t.Logf("killed with %d debits applied; %d commands answered unknown, %d succeeded, %d debits at the end", atKill, unknown, n-unknown, debits())
}
