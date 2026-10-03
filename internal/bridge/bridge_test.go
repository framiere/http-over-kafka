package bridge_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/api"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/bridge"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/demo/payments"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkatest"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestChargeRoundTrip(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	e.start(e.config())

	cmd := e.charge("acc-rt", "")
	e.send(cmd)
	resp := e.await(cmd.RequestID, 30*time.Second)
	if resp.Status != 201 || resp.Fault != "" || resp.Headers.Get("location") == "" {
		t.Fatalf("got %+v", resp)
	}
	ch := decode[payments.Charge](t, resp.Body.Bytes())
	if ch.AccountID != "acc-rt" || ch.AmountCents != 1250 {
		t.Fatalf("body %s", resp.Body.Bytes())
	}
	res := resultsFor(e.results(), cmd.RequestID)
	if len(res) != 1 || res[0].Type != "CreateChargeSucceeded" || res[0].Response.Status != 201 {
		t.Fatalf("results %+v", res)
	}
	if a := pay.Account("acc-rt"); a.DebitCount != 1 {
		t.Fatalf("debits %d", a.DebitCount)
	}
}

func TestIdempotentRetryGetsOriginalOutcome(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	e.start(e.config())

	first := e.charge("acc-idem", "key-1")
	e.send(first)
	orig := e.await(first.RequestID, 30*time.Second)

	retry := e.charge("acc-idem", "key-1") // new requestId, same key and body
	e.send(retry)
	got := e.await(retry.RequestID, 30*time.Second)
	if got.ReplayOf != first.RequestID || got.Status != orig.Status || !bytes.Equal(got.Body.Bytes(), orig.Body.Bytes()) ||
		got.Headers.Get("location") != orig.Headers.Get("location") {
		t.Fatalf("replay differs from original:\norig %+v\ngot  %+v", orig, got)
	}

	reused := e.charge("acc-idem-other", "key-1") // same key, different body
	e.send(reused)
	if r := e.await(reused.RequestID, 30*time.Second); r.Status != 422 || r.Fault != wire.FaultIdempotencyKeyReused {
		t.Fatalf("key reuse: %+v", r)
	}

	rs := e.results()
	if n := len(resultsFor(rs, first.RequestID)); n != 1 {
		t.Fatalf("original: %d results", n)
	}
	if n := len(resultsFor(rs, retry.RequestID)); n != 0 {
		t.Fatalf("a replay must not produce a result, got %d", n)
	}
	if r := resultsFor(rs, reused.RequestID); len(r) != 1 || r[0].Outcome != wire.OutcomeNotExecuted {
		t.Fatalf("key reuse result: %+v", r)
	}
	if a, b := pay.Account("acc-idem").DebitCount, pay.Account("acc-idem-other").DebitCount; a != 1 || b != 0 {
		t.Fatalf("debits %d / %d", a, b)
	}
}

// The client timed out (gateway 504) and retries while the first execution
// is still running on B: the retry waits for it and gets its outcome.
func TestRetryWhileOriginalStillExecuting(t *testing.T) {
	pay, srv := paymentsB(t)
	pay.SetDelay(2 * time.Second)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	e.start(e.config())

	first := e.charge("acc-inflight", "key-slow")
	e.send(first)
	kafkatest.Eventually(t, 30*time.Second, func() bool { return pay.Account("acc-inflight").DebitCount == 1 }, "first execution started")
	retry := e.charge("acc-inflight", "key-slow")
	e.send(retry)

	got := e.await(retry.RequestID, 30*time.Second)
	orig := e.await(first.RequestID, time.Second)
	if got.ReplayOf != first.RequestID || got.Status != 201 || !bytes.Equal(got.Body.Bytes(), orig.Body.Bytes()) {
		t.Fatalf("got %+v", got)
	}
	if n := pay.Account("acc-inflight").DebitCount; n != 1 {
		t.Fatalf("debits %d", n)
	}
}

// B was down: the first attempt certainly did not run, so a retry with the
// same key executes instead of replaying the 503.
func TestRetryAfterUpstreamUnavailableExecutes(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	e := newEnv(t, "pay", api.Payments, "http://"+addr)
	e.start(e.config())

	first := e.charge("acc-down", "key-down")
	e.send(first)
	if r := e.await(first.RequestID, 30*time.Second); r.Fault != wire.FaultUpstreamUnavailable || r.Status != 503 {
		t.Fatalf("got %+v", r)
	}

	pay := payments.New()
	l, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(pay.Handler())
	srv.Listener = l
	srv.Start()
	defer srv.Close()

	retry := e.charge("acc-down", "key-down")
	e.send(retry)
	if r := e.await(retry.RequestID, 30*time.Second); r.Status != 201 || r.ReplayOf != "" {
		t.Fatalf("retry must execute: %+v", r)
	}
	if n := pay.Account("acc-down").DebitCount; n != 1 {
		t.Fatalf("debits %d", n)
	}
}

func TestRejectedCommands(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	e.start(e.config())

	// Altered after signing: no execution, no reply, no result. The valid
	// command shares its dedup key, hence its partition, and comes after:
	// once it is answered, the forgery has been processed.
	valid := e.charge("acc-auth", "key-auth")
	forged, err := wire.EncodeCommand(valid, e.signer)
	if err != nil {
		t.Fatal(err)
	}
	forged.Value = bytes.Replace(forged.Value, []byte(`1250`), []byte(`9999`), 1)
	e.produce(forged)
	e.send(valid)
	e.await(valid.RequestID, 30*time.Second)
	if r := e.responsesFor(valid.RequestID); len(r) != 1 || r[0].Status != 201 {
		t.Fatalf("responses for the request id: %+v", r)
	}

	// A signed command copied onto another partition: it would otherwise be
	// run by a second owner concurrently with the original.
	copied := e.charge("acc-copy", "")
	rec := e.send(copied)
	e.await(copied.RequestID, 30*time.Second)
	dup := *rec
	dup.Partition = (rec.Partition + 1) % partitions
	cl := kafkatest.Client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerLinger(0))
	if err := cl.ProduceSync(t.Context(), &kgo.Record{Topic: dup.Topic, Partition: dup.Partition, Key: dup.Key, Value: dup.Value, Headers: dup.Headers}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	stale := e.charge("acc-stale", "")
	stale.IssuedAt = time.Now().Add(-20 * time.Minute)
	stale.Deadline = stale.IssuedAt.Add(time.Second)
	stale.ExpiresAt = stale.IssuedAt.Add(5 * time.Minute)
	e.send(stale)
	if r := e.await(stale.RequestID, 30*time.Second); r.Fault != wire.FaultCommandStale || r.Status != 503 {
		t.Fatalf("stale: %+v", r)
	}

	mismatch := e.charge("acc-mismatch", "")
	mismatch.OperationID = "getAccount"
	e.send(mismatch)
	if r := e.await(mismatch.RequestID, 30*time.Second); r.Fault != wire.FaultOperationMismatch {
		t.Fatalf("mismatch: %+v", r)
	}

	rs := e.results()
	if r := resultsFor(rs, valid.RequestID); len(r) != 1 || r[0].Outcome != wire.OutcomeSucceeded {
		t.Fatalf("the forgery must leave no trace: %+v", r)
	}
	if r := resultsFor(rs, copied.RequestID); len(r) != 1 || len(e.responsesFor(copied.RequestID)) != 1 {
		t.Fatalf("the copy must leave no trace: %d results, %d responses", len(r), len(e.responsesFor(copied.RequestID)))
	}
	for id, want := range map[string]wire.Outcome{stale.RequestID: wire.OutcomeNotExecuted, mismatch.RequestID: wire.OutcomeNotExecuted} {
		if r := resultsFor(rs, id); len(r) != 1 || r[0].Outcome != want {
			t.Fatalf("%s: %+v", id, r)
		}
	}
	for acc, want := range map[string]int{"acc-auth": 1, "acc-copy": 1, "acc-stale": 0, "acc-mismatch": 0} {
		if n := pay.Account(acc).DebitCount; n != want {
			t.Errorf("%s: %d debits, want %d", acc, n, want)
		}
	}
}

// Many commands, two bridges, one of them shut down and another started
// mid-flight: every command executed exactly once, answered once, one
// Result each; graceful handovers produce no unknown outcome.
func TestScaleOutAndInUnderLoad(t *testing.T) {
	pay, srv := paymentsB(t)
	pay.SetDelay(20 * time.Millisecond)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	stopA := e.start(e.config())
	e.start(e.config())

	const n = 150
	var cmds []wire.Command
	send := func(from, to int) {
		for i := from; i < to; i++ {
			key := ""
			if i%2 == 0 {
				key = fmt.Sprintf("load-%d", i)
			}
			c := e.charge(fmt.Sprintf("acc-load-%d", i), key)
			cmds = append(cmds, c)
			e.send(c)
		}
	}
	send(0, n/3)
	for _, c := range cmds {
		e.await(c.RequestID, 60*time.Second) // both bridges are up and busy
	}
	send(n/3, 2*n/3)
	stopA() // graceful while its partitions have calls in flight
	send(2*n/3, n)
	e.start(e.config())
	for _, c := range cmds {
		r := e.await(c.RequestID, 60*time.Second)
		if r.Status != 201 {
			t.Errorf("%s: %+v", c.RequestID, r)
		}
	}
	rs := e.results()
	for i, c := range cmds {
		if got := len(e.responsesFor(c.RequestID)); got != 1 {
			t.Errorf("%s: %d responses", c.RequestID, got)
		}
		if got := len(resultsFor(rs, c.RequestID)); got != 1 {
			t.Errorf("%s: %d results", c.RequestID, got)
		}
		if d := pay.Account(fmt.Sprintf("acc-load-%d", i)).DebitCount; d != 1 {
			t.Errorf("acc-load-%d: %d debits", i, d)
		}
	}
}

func TestRequestReachesBAsPlainHTTP(t *testing.T) {
	var seen *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Clone(r.Context())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"ch_1"}`))
	}))
	defer srv.Close()
	e := newEnv(t, "pay", api.Payments, srv.URL)
	e.start(e.config())
	cmd := e.charge("acc-shape", "key-shape")
	cmd.Headers["x-caller-application"] = []string{"admin"}
	e.send(cmd)
	e.await(cmd.RequestID, 30*time.Second)
	for h, want := range map[string]string{
		bridge.HeaderCallerApplication: "checkout",
		bridge.HeaderCallerInstance:    "checkout-1",
		wire.RequestIDHeader:           cmd.RequestID,
		wire.IdempotencyKeyHeader:      "key-shape",
		"Content-Type":                 "application/json",
		"Authorization":                "",
	} {
		if got := seen.Header.Get(h); got != want {
			t.Errorf("%s: %q, want %q", h, got, want)
		}
	}
}

// A slow call to B holds back its own dedup key only: unrelated commands on
// the same partition are answered meanwhile, a retry of the slow one waits.
func TestSlowCallDoesNotBlockItsPartition(t *testing.T) {
	release := make(chan struct{})
	pay := payments.New()
	inner := pay.Handler()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(wire.IdempotencyKeyHeader) == "key-slow-lane" {
			<-release
		}
		inner.ServeHTTP(w, r)
	}))
	defer srv.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	e := newEnv(t, "pay", api.Payments, srv.URL)
	e.start(e.config())
	hasher := kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service))
	part := func(c wire.Command) int { return hasher.Partition(&kgo.Record{Key: []byte(c.DedupKey())}, partitions) }

	slow := e.charge("acc-slow", "key-slow-lane")
	e.send(slow)
	retry := e.charge("acc-slow", "key-slow-lane")
	e.send(retry)
	var fast []wire.Command
	for i := 0; len(fast) < 20; i++ {
		c := e.charge(fmt.Sprintf("acc-fast-%d", i), "")
		if part(c) != part(slow) {
			continue
		}
		fast = append(fast, c)
		e.send(c)
	}
	for _, c := range fast {
		if r := e.await(c.RequestID, 30*time.Second); r.Status != 201 {
			t.Fatalf("%+v", r)
		}
	}
	if n := len(e.responsesFor(slow.RequestID)) + len(e.responsesFor(retry.RequestID)); n != 0 {
		t.Fatalf("slow command answered before B returned: %d", n)
	}
	close(release)
	orig := e.await(slow.RequestID, 30*time.Second)
	got := e.await(retry.RequestID, 30*time.Second)
	if orig.Status != 201 || got.ReplayOf != slow.RequestID || !bytes.Equal(got.Body.Bytes(), orig.Body.Bytes()) {
		t.Fatalf("orig %+v retry %+v", orig, got)
	}
	if n := pay.Account("acc-slow").DebitCount; n != 1 {
		t.Fatalf("debits %d", n)
	}
}

// Operator actions that silently change which dedup state a retry would
// meet. Growing all three topics together keeps their counts equal, yet every
// key now hashes elsewhere than its entry; recreating a topic empties state or
// resets offsets; time retention lets Kafka delete entries. Each must make
// the bridge refuse to serve (fail closed), never execute B again.
func TestStateIdentityChangesFailClosed(t *testing.T) {
	cases := map[string]struct {
		mutate func(t *testing.T, e *env)
		want   string
	}{
		"partitions grown on all topics": {func(t *testing.T, e *env) {
			resp, err := kafkatest.Admin(t).UpdatePartitions(t.Context(), 2*partitions,
				wire.CommandTopic(e.service), wire.ResultTopic(e.service), bridge.StateTopic(e.service))
			if err != nil || resp.Error() != nil {
				t.Fatal(err, resp.Error())
			}
		}, fmt.Sprintf("changed from %d to %d", partitions, 2*partitions)},
		"state topic recreated": {func(t *testing.T, e *env) {
			deleteTopics(t, bridge.StateTopic(e.service))
		}, "state topic"},
		"command topic recreated": {func(t *testing.T, e *env) {
			deleteTopics(t, wire.CommandTopic(e.service))
			kafkatest.CreateTopics(t, kafkaenv.CommandTopic(e.service, partitions)) // by the gateway
		}, "command topic"},
		"unclean leader election on state": {func(t *testing.T, e *env) {
			v := "true"
			resp, err := kafkatest.Admin(t).AlterTopicConfigs(t.Context(),
				[]kadm.AlterConfig{{Op: kadm.SetConfig, Name: "unclean.leader.election.enable", Value: &v}}, bridge.StateTopic(e.service))
			if err != nil || resp[0].Err != nil {
				t.Fatal(err, resp)
			}
		}, "unclean leader election"},
		"time retention on state": {func(t *testing.T, e *env) {
			v := "delete"
			resp, err := kafkatest.Admin(t).AlterTopicConfigs(t.Context(),
				[]kadm.AlterConfig{{Op: kadm.SetConfig, Name: "cleanup.policy", Value: &v}}, bridge.StateTopic(e.service))
			if err != nil || resp[0].Err != nil {
				t.Fatal(err, resp)
			}
		}, "cleanup.policy=delete"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			pay, srv := paymentsB(t)
			e := newEnv(t, "pay", api.Payments, srv.URL)
			stop := e.start(e.config())
			first := e.charge("acc-id", "key-id")
			e.send(first)
			if r := e.await(first.RequestID, 30*time.Second); r.Status != 201 {
				t.Fatalf("%+v", r)
			}
			stop()
			c.mutate(t, e)

			b, err := bridge.New(e.config())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			err = b.Run(ctx)
			if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "refusing to serve") {
				t.Fatalf("bridge must refuse to serve (%q), got %v", c.want, err)
			}
			if n := pay.Account("acc-id").DebitCount; n != 1 {
				t.Fatalf("debits %d", n)
			}
			t.Logf("%s → %v", name, err)
		})
	}
}

// deleteTopics deletes and waits until the broker no longer lists them.
func deleteTopics(t *testing.T, topics ...string) {
	t.Helper()
	adm := kafkatest.Admin(t)
	if _, err := adm.DeleteTopics(t.Context(), topics...); err != nil {
		t.Fatal(err)
	}
	kafkatest.Eventually(t, 30*time.Second, func() bool {
		td, err := adm.ListTopics(t.Context(), topics...)
		if err != nil {
			return false
		}
		for _, d := range td {
			if d.Err == nil {
				return false
			}
		}
		return true
	}, "topics %v deleted", topics)
}

// Everything that remembered past executions is wiped (state, layout,
// group, results) while commands still valid sit in the command topic. The
// new bridge cannot tell them from new ones by their content, only by their
// position: anything older than its memory is answered unknown, never run,
// and a retry with the same key gets that answer too.
func TestTotalWipeNeverReexecutes(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	stop := e.start(e.config())
	keyed := e.charge("acc-wipe-k", "key-wipe")
	plain := e.charge("acc-wipe-p", "")
	for _, c := range []wire.Command{keyed, plain} {
		e.send(c)
		if r := e.await(c.RequestID, 30*time.Second); r.Status != 201 {
			t.Fatalf("%+v", r)
		}
	}
	stop()

	deleteTopics(t, bridge.StateTopic(e.service), bridge.LayoutTopic(e.service), wire.ResultTopic(e.service))
	if _, err := kafkatest.Admin(t).DeleteGroup(t.Context(), "kb-bridge."+e.service); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.responses = map[string][]wire.Response{} // the gateway restarted too
	e.mu.Unlock()

	e.start(e.config())
	for _, c := range []wire.Command{keyed, plain} {
		if r := e.await(c.RequestID, 30*time.Second); r.Fault != wire.FaultOutcomeUnknown {
			t.Fatalf("pre-memory command %s: %+v", c.RequestID, r)
		}
	}
	retry := e.charge("acc-wipe-k", "key-wipe")
	e.send(retry)
	if r := e.await(retry.RequestID, 30*time.Second); r.ReplayOf != keyed.RequestID || r.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("retry must not execute: %+v", r)
	}
	fresh := e.charge("acc-wipe-new", "")
	e.send(fresh)
	if r := e.await(fresh.RequestID, 30*time.Second); r.Status != 201 {
		t.Fatalf("a command born after the memory runs normally: %+v", r)
	}
	rs := e.results()
	for _, c := range []wire.Command{keyed, plain} {
		if res := resultsFor(rs, c.RequestID); len(res) != 1 || res[0].Outcome != wire.OutcomeUnknown {
			t.Fatalf("%s results %+v", c.RequestID, res)
		}
	}
	for acc, want := range map[string]int{"acc-wipe-k": 1, "acc-wipe-p": 1, "acc-wipe-new": 1} {
		if n := pay.Account(acc).DebitCount; n != want {
			t.Errorf("%s: %d debits, want %d", acc, n, want)
		}
	}
}

// Genesis: the gateway accepted commands before any bridge of the service
// ever ran. Same rule, same direction of error: unknown, never executed.
func TestBacklogBeforeFirstRunIsNotExecuted(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	early := e.charge("acc-early", "key-early")
	e.send(early)
	e.start(e.config())
	if r := e.await(early.RequestID, 30*time.Second); r.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("%+v", r)
	}
	if n := pay.Account("acc-early").DebitCount; n != 0 {
		t.Fatalf("debits %d", n)
	}
}

// A dedup memory reset (layout, and possibly state, group, results deleted),
// optionally after the partition count changed: the bridge starts a new
// memory and serves again. Retries of keys used before must not run B; a
// fresh key runs normally.
func TestMemoryResetNeverReexecutes(t *testing.T) {
	cases := map[string]struct {
		grow   bool
		delete func(e *env) []string
		group  bool
		want   wire.Fault // what a retry of an old key gets ("" = the original 201)
	}{
		"layout only":                  {false, func(e *env) []string { return []string{bridge.LayoutTopic(e.service)} }, false, ""},
		"layout and state":             {false, func(e *env) []string { return []string{bridge.LayoutTopic(e.service), bridge.StateTopic(e.service)} }, false, wire.FaultOutcomeUnknown},
		"layout, state, group":         {false, func(e *env) []string { return []string{bridge.LayoutTopic(e.service), bridge.StateTopic(e.service)} }, true, wire.FaultOutcomeUnknown},
		"partitions grown, then reset": {true, func(e *env) []string { return []string{bridge.LayoutTopic(e.service), bridge.StateTopic(e.service)} }, false, wire.FaultOutcomeUnknown},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			pay, srv := paymentsB(t)
			e := newEnv(t, "pay", api.Payments, srv.URL)
			stop := e.start(e.config())
			var orig []wire.Command
			for i := range 12 {
				cmd := e.charge(fmt.Sprintf("acc-reset-%d", i), fmt.Sprintf("key-reset-%d", i))
				e.send(cmd)
				orig = append(orig, cmd)
			}
			for _, cmd := range orig {
				if r := e.await(cmd.RequestID, 30*time.Second); r.Status != 201 {
					t.Fatalf("%+v", r)
				}
			}
			stop()
			if c.grow {
				resp, err := kafkatest.Admin(t).UpdatePartitions(t.Context(), 2*partitions,
					wire.CommandTopic(e.service), wire.ResultTopic(e.service), bridge.StateTopic(e.service))
				if err != nil || resp.Error() != nil {
					t.Fatal(err, resp.Error())
				}
				e.producer = kafkatest.Client(t, kgo.ProducerLinger(0)) // a restarted gateway sees the new count
			}
			deleteTopics(t, c.delete(e)...)
			if c.group {
				if _, err := kafkatest.Admin(t).DeleteGroup(t.Context(), "kb-bridge."+e.service); err != nil {
					t.Fatal(err)
				}
			}
			e.start(e.config())

			// The restarted gateway now hashes with the current count.
			for i, o := range orig {
				retry := e.charge(fmt.Sprintf("acc-reset-%d", i), fmt.Sprintf("key-reset-%d", i))
				e.send(retry)
				r := e.await(retry.RequestID, 30*time.Second)
				if r.ReplayOf != o.RequestID || r.Fault != c.want {
					t.Errorf("retry %d: %+v", i, r)
				}
			}
			fresh := e.charge("acc-reset-new", "key-reset-new")
			e.send(fresh)
			if r := e.await(fresh.RequestID, 30*time.Second); r.Status != 201 || r.ReplayOf != "" {
				t.Errorf("a new key runs normally: %+v", r)
			}
			for i := range 12 {
				if n := pay.Account(fmt.Sprintf("acc-reset-%d", i)).DebitCount; n != 1 {
					t.Errorf("acc-reset-%d: %d debits", i, n)
				}
			}
		})
	}
}

// Total wipe after the original commands left the command topic (retention,
// DeleteRecords): nothing can tell which keys were used. Until the
// idempotency window has passed, keyed commands with unknown keys are not
// executed; commands without a key run.
func TestWipeAfterCommandsDeletedIsBlind(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	stop := e.start(e.config())
	orig := e.charge("acc-blind", "key-blind")
	e.send(orig)
	if r := e.await(orig.RequestID, 30*time.Second); r.Status != 201 {
		t.Fatalf("%+v", r)
	}
	stop()
	adm := kafkatest.Admin(t)
	ends, err := adm.ListEndOffsets(t.Context(), wire.CommandTopic(e.service))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adm.DeleteRecords(t.Context(), ends.Offsets()); err != nil {
		t.Fatal(err)
	}
	deleteTopics(t, bridge.LayoutTopic(e.service), bridge.StateTopic(e.service), wire.ResultTopic(e.service))
	if _, err := adm.DeleteGroup(t.Context(), "kb-bridge."+e.service); err != nil {
		t.Fatal(err)
	}
	e.start(e.config())

	retry := e.charge("acc-blind", "key-blind")
	e.send(retry)
	if r := e.await(retry.RequestID, 30*time.Second); r.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("retry of a deleted original: %+v", r)
	}
	plain := e.charge("acc-blind-plain", "")
	e.send(plain)
	if r := e.await(plain.RequestID, 30*time.Second); r.Status != 201 {
		t.Fatalf("no key, no ambiguity: %+v", r)
	}
	if n := pay.Account("acc-blind").DebitCount; n != 1 {
		t.Fatalf("debits %d", n)
	}
}

// The bridge starts before the gateway: it waits for the command topic
// instead of creating it, then takes the gateway's partition count for its
// results, its dedup state and its layout.
func TestBridgeStartsBeforeGateway(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnvWith(t, "pay", api.Payments, srv.URL, false)
	var logs syncBuf
	cfg := e.config()
	cfg.Log = slog.New(slog.NewTextHandler(&logs, nil))
	b, err := bridge.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	kafkatest.Eventually(t, 30*time.Second, func() bool { return strings.Contains(logs.String(), "waiting for "+wire.CommandTopic(e.service)) },
		"bridge waits for the command topic")
	adm := kafkatest.Admin(t)
	td, err := adm.ListTopics(t.Context(), wire.CommandTopic(e.service), wire.ResultTopic(e.service), bridge.StateTopic(e.service))
	if err != nil {
		t.Fatal(err)
	}
	for name, d := range td {
		if d.Err == nil {
			t.Fatalf("bridge created %s before the gateway created the command topic", name)
		}
	}

	const gwPartitions = 4 // the gateway's choice, not the bridge's
	kafkatest.CreateTopics(t, kafkaenv.CommandTopic(e.service, gwPartitions))
	// The gateway accepts traffic at once, maybe before the bridge's memory
	// begins: the bridge saw the topic absent, so nothing in it can predate
	// a previous bridge, and these must run.
	early := e.charge("acc-early-gw", "key-early-gw")
	e.send(early)
	e.awaitMemory()
	if r := e.await(early.RequestID, 30*time.Second); r.Status != 201 {
		t.Fatalf("command sent right after the gateway started: %+v", r)
	}
	td, err = adm.ListTopics(t.Context(), wire.CommandTopic(e.service), wire.ResultTopic(e.service), bridge.StateTopic(e.service))
	if err != nil {
		t.Fatal(err)
	}
	for name, d := range td {
		if d.Err != nil || len(d.Partitions) != gwPartitions {
			t.Fatalf("%s: %v, %d partitions, want %d", name, d.Err, len(d.Partitions), gwPartitions)
		}
	}
	cmd := e.charge("acc-order", "key-order")
	e.send(cmd)
	if r := e.await(cmd.RequestID, 30*time.Second); r.Status != 201 {
		t.Fatalf("%+v", r)
	}
	if n := pay.Account("acc-order").DebitCount; n != 1 {
		t.Fatalf("debits %d", n)
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// D10: what the bridge publishes verifies with its key only.
func TestOutputsAreSignedByTheBridge(t *testing.T) {
	_, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	e.start(e.config())
	cmd := e.charge("acc-sig", "key-sig")
	e.send(cmd)
	e.await(cmd.RequestID, 30*time.Second) // decoded with e.bridgeKeys by the harness
	retry := e.charge("acc-sig", "key-sig")
	e.send(retry)
	e.await(retry.RequestID, 30*time.Second) // a replay is signed too

	_, otherPub, err := identity.Generate("test-bridge") // same kid, other key: an impostor
	if err != nil {
		t.Fatal(err)
	}
	impostor, err := identity.ParseTrustedKeys(identity.RoleBridge, otherPub)
	if err != nil {
		t.Fatal(err)
	}
	gwAsBridge, err := identity.ParseTrustedKeys(identity.RoleBridge, e.trusted)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{wire.ReplyTopic(e.gw), wire.ResultTopic(e.service)} {
		recs := kafkatest.Consume(t, topic, 20*time.Second, func(rs []*kgo.Record) bool { return len(rs) > 0 })
		for _, r := range recs {
			if err := wire.VerifySignature(r, e.bridgeKeys); err != nil {
				t.Fatalf("%s: not verifiable with the bridge key: %v", topic, err)
			}
			for name, k := range map[string]identity.TrustedKeys{"impostor key": impostor, "gateway key": gwAsBridge} {
				if wire.VerifySignature(r, k) == nil {
					t.Fatalf("%s: verified with the %s", topic, name)
				}
			}
		}
	}
}

// Anyone able to write to the state topic could plant a "done" entry whose
// Response the bridge would replay, signed, to a retry (laundering a forgery
// through D10), or erase a "started" marker. Every state record is signed by
// the bridge; any record that does not verify stops the bridge.
func TestForgedStateIsNeverServed(t *testing.T) {
	cases := map[string]func(e *env, orig *kgo.Record) *kgo.Record{
		"unsigned done entry for a new key": func(e *env, orig *kgo.Record) *kgo.Record {
			return forgedDone(e, "key-victim", nil)
		},
		"done entry signed by another key": func(e *env, orig *kgo.Record) *kgo.Record {
			s, _ := identity.NewSigner(identity.RoleBridge, e.bridgeSigner.KeyID(), make([]byte, 32))
			return forgedDone(e, "key-victim", s)
		},
		"genuine entry copied under another key": func(e *env, orig *kgo.Record) *kgo.Record {
			c := *orig
			c.Key = []byte(e.charge("acc-victim", "key-victim").DedupKey())
			c.Partition = int32(kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service)).Partition(&kgo.Record{Key: c.Key}, partitions))
			return &c
		},
		"unsigned tombstone erasing an entry": func(e *env, orig *kgo.Record) *kgo.Record {
			return &kgo.Record{Topic: orig.Topic, Partition: orig.Partition, Key: orig.Key}
		},
	}
	for name, forge := range cases {
		t.Run(name, func(t *testing.T) {
			pay, srv := paymentsB(t)
			e := newEnv(t, "pay", api.Payments, srv.URL)
			stop := e.start(e.config())
			first := e.charge("acc-genuine", "key-genuine")
			e.send(first)
			if r := e.await(first.RequestID, 30*time.Second); r.Status != 201 {
				t.Fatalf("%+v", r)
			}
			stop()
			var orig *kgo.Record
			kafkatest.Consume(t, bridge.StateTopic(e.service), 20*time.Second, func(rs []*kgo.Record) bool {
				for _, r := range rs {
					if string(r.Key) == first.DedupKey() {
						orig = r
					}
				}
				return orig != nil
			})
			cl := kafkatest.Client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerLinger(0))
			if err := cl.ProduceSync(t.Context(), forge(e, orig)).FirstErr(); err != nil {
				t.Fatal(err)
			}

			b, err := bridge.New(e.config())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			err = b.Run(ctx)
			if err == nil || !strings.Contains(err.Error(), "dedup state tampered") {
				t.Fatalf("bridge must stop on a forged state record, got %v", err)
			}
			for id, rs := range e.responsesFor0() {
				for _, r := range rs {
					if strings.Contains(string(r.Body.Bytes()), "forged") {
						t.Fatalf("forged response served to %s", id)
					}
				}
			}
			if n := pay.Account("acc-genuine").DebitCount; n != 1 {
				t.Fatalf("debits %d", n)
			}
		})
	}
}

// forgedDone is a "done" state entry for key whose stored Response the
// attacker chose. signer nil: unsigned.
func forgedDone(e *env, key string, signer *identity.Signer) *kgo.Record {
	victim := e.charge("acc-victim", key)
	resp := wire.Response{V: wire.Version, RequestID: victim.RequestID, Status: 201,
		Headers: wire.Headers{"content-type": {"application/json"}}, Body: wire.NewBody([]byte(`{"id":"forged"}`))}
	v, _ := json.Marshal(map[string]any{"requestId": victim.RequestID, "fingerprint": victim.Fingerprint(),
		"phase": "done", "outcome": "Succeeded", "response": resp, "purgeAfter": time.Now().Add(time.Hour)})
	dk := victim.DedupKey()
	p := int32(kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service)).Partition(&kgo.Record{Key: []byte(dk)}, partitions))
	if signer != nil {
		return signStateWith(signer, bridge.StateTopic(e.service), p, dk, v)
	}
	return &kgo.Record{Topic: bridge.StateTopic(e.service), Partition: p, Key: []byte(dk), Value: v}
}

// signedState is a state record as the bridge itself writes it.
func signedState(e *env, p int32, key string, value []byte) *kgo.Record {
	return signStateWith(e.bridgeSigner, bridge.StateTopic(e.service), p, key, value)
}

// signStateWith mirrors the bridge's state record signature (state.go).
func signStateWith(s *identity.Signer, topic string, p int32, key string, value []byte) *kgo.Record {
	sig := s.Sign("kafka-backbone/bridge-state/v1", []byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s", topic, p, key, value)))
	return &kgo.Record{Topic: topic, Partition: p, Key: []byte(key), Value: value, Headers: []kgo.RecordHeader{
		{Key: "kb-kid", Value: []byte(s.KeyID())}, {Key: "kb-sig", Value: []byte(base64.StdEncoding.EncodeToString(sig))}}}
}

// The gateway rotates its signing key, then the dedup memory is reset. The
// originals of old keys no longer verify; their keys must still be claimed.
func TestResetAfterKeyRotationNeverReexecutes(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	stop := e.start(e.config())
	var orig []wire.Command
	for i := range 6 {
		c := e.charge(fmt.Sprintf("acc-rot-%d", i), fmt.Sprintf("key-rot-%d", i))
		e.send(c)
		orig = append(orig, c)
	}
	for _, c := range orig {
		if r := e.await(c.RequestID, 30*time.Second); r.Status != 201 {
			t.Fatalf("%+v", r)
		}
	}
	stop()

	signing, trusted, err := identity.Generate("test-gw-2")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(identity.RoleGateway), signing)
	if e.signer, err = identity.SignerFromEnv(identity.RoleGateway); err != nil {
		t.Fatal(err)
	}
	if e.keys, err = identity.ParseTrustedKeys(identity.RoleGateway, trusted); err != nil { // old key retired
		t.Fatal(err)
	}
	deleteTopics(t, bridge.LayoutTopic(e.service), bridge.StateTopic(e.service))
	e.start(e.config())

	for i, o := range orig {
		retry := e.charge(fmt.Sprintf("acc-rot-%d", i), fmt.Sprintf("key-rot-%d", i))
		e.send(retry)
		if r := e.await(retry.RequestID, 30*time.Second); r.ReplayOf != o.RequestID || r.Fault != wire.FaultOutcomeUnknown {
			t.Errorf("retry %d: %+v", i, r)
		}
		if n := pay.Account(fmt.Sprintf("acc-rot-%d", i)).DebitCount; n != 1 {
			t.Errorf("acc-rot-%d: %d debits", i, n)
		}
	}
}

const securedPayments = `openapi: 3.0.3
info: {title: secured, version: "1"}
components:
  securitySchemes:
    key: {type: apiKey, in: header, name: X-Service-Key} # contract-declared, not a standard secret name
    qtok: {type: apiKey, in: query, name: access_token}
    tok: {type: http, scheme: bearer}
paths:
  /charges:
    post:
      operationId: createCharge
      security: [{key: []}, {tok: []}]
      responses: {'201': {description: ok}}
`

// D12 on mutations: B always gets the provider's credential when its contract
// requires one, never A's; a bridge that cannot satisfy a mutation does not
// start; a command still carrying a declared secret is refused without
// calling B, and the secret is not copied into the Result.
func TestProviderCredentialAndSecretsInCommands(t *testing.T) {
	var calls atomic.Int32
	var seen atomic.Pointer[http.Header]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		h := r.Header.Clone()
		seen.Store(&h)
		w.WriteHeader(201)
	}))
	defer srv.Close()
	e := newEnv(t, "sec", []byte(securedPayments), srv.URL)

	if _, err := bridge.New(e.config()); err == nil || !strings.Contains(err.Error(), "configure an upstream credential") {
		t.Fatalf("a mutation requiring a credential must not start without one: %v", err)
	}
	cfg := e.config()
	creds, err := apispec.ParseCredentials(e.service, e.service+".tok=provider-token")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Credentials = creds
	e.start(cfg)

	clean := e.charge("acc-sec", "")
	e.send(clean)
	if r := e.await(clean.RequestID, 30*time.Second); r.Status != 201 {
		t.Fatalf("%+v", r)
	}
	if h := *seen.Load(); h.Get("Authorization") != "Bearer provider-token" || h.Get("X-Service-Key") != "" {
		t.Fatalf("B got Authorization=%q X-Service-Key=%q", h.Get("Authorization"), h.Get("X-Service-Key"))
	}

	leakH := e.charge("acc-sec", "")
	leakH.Headers["x-service-key"] = []string{"callers-secret"}
	leakQ := e.charge("acc-sec", "")
	leakQ.RawQuery = "a=1&access_token=callers-secret"
	for _, c := range []wire.Command{leakH, leakQ} {
		e.send(c)
		if r := e.await(c.RequestID, 30*time.Second); r.Fault != wire.FaultSecretInCommand || wire.OutcomeOf(r) != wire.OutcomeNotExecuted {
			t.Fatalf("%+v", r)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("B called %d times, want 1", n)
	}
	rs := e.results()
	for _, c := range []wire.Command{leakH, leakQ} {
		res := resultsFor(rs, c.RequestID)
		if len(res) != 1 || res[0].Outcome != wire.OutcomeNotExecuted {
			t.Fatalf("results %+v", res)
		}
		b, _ := json.Marshal(res[0])
		if strings.Contains(string(b), "callers-secret") {
			t.Fatalf("secret copied into the Result: %s", b)
		}
	}
}

// The state topic deleted while the bridge runs: the broker's answer proves
// the dedup state gone. The bridge must stop (fatal), not retry forever, and
// must not call B for commands it can no longer deduplicate.
func TestStateTopicDeletedWhileRunningIsFatal(t *testing.T) {
	pay, srv := paymentsB(t)
	e := newEnv(t, "pay", api.Payments, srv.URL)
	b, err := bridge.New(e.config())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	e.awaitMemory()
	first := e.charge("acc-del", "key-del")
	e.send(first)
	if r := e.await(first.RequestID, 30*time.Second); r.Status != 201 {
		t.Fatalf("%+v", r)
	}

	deleteTopics(t, bridge.StateTopic(e.service))
	next := e.charge("acc-del-2", "key-del-2")
	e.send(next)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "deleted while running") {
			t.Fatalf("want a fatal identity error, got %v", err)
		}
		t.Logf("bridge stopped: %v", err)
	case <-ctx.Done():
		t.Fatal("bridge kept running on a deleted state topic")
	}
	if n := pay.Account("acc-del-2").DebitCount; n != 0 {
		t.Fatalf("B called without dedup state: %d debits", n)
	}
}
