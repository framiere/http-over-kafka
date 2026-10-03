//go:build e2e

package e2e

import (
	"encoding/json"
	"syscall"
	"testing"
	"time"
)

const (
	probUnknown   = "urn:kafka-backbone:outcome_unknown"
	probTimeout   = "urn:kafka-backbone:gateway-timeout"
	probTransport = "urn:kafka-backbone:transport_unavailable"
	probUpstream  = "urn:kafka-backbone:upstream_unavailable"
	probStale     = "urn:kafka-backbone:command_stale"
	probReused    = "urn:kafka-backbone:idempotency_key_reused"
)

type resultView struct {
	Outcome string `json:"outcome"`
	Type    string `json:"type"`
	Command struct {
		RequestID      string `json:"requestId"`
		IdempotencyKey string `json:"idempotencyKey"`
		OperationID    string `json:"operationId"`
	} `json:"command"`
	Response struct {
		Status int    `json:"status"`
		Fault  string `json:"fault"`
	} `json:"response"`
}

// results reads a service's Result stream as a read_committed consumer.
func results(t *testing.T, s *svc) []resultView {
	t.Helper()
	var out []resultView
	for _, r := range readTopic(t, "http.results."+s.name, true) {
		var v resultView
		if err := json.Unmarshal(r.Value, &v); err != nil {
			t.Fatalf("result undecodable: %v", err)
		}
		out = append(out, v)
	}
	return out
}

func resultsFor(rs []resultView, requestID string) []resultView {
	var out []resultView
	for _, r := range rs {
		if r.Command.RequestID == requestID {
			out = append(out, r)
		}
	}
	return out
}

// async sends r in the background.
func async(c *caller, g *gw, host string, r req) <-chan reply {
	ch := make(chan reply, 1)
	go func() { ch <- c.via(g, host, r) }()
	return ch
}

// The spec's own scenario: B charged the card, the bridge dies before the
// response is published, Kafka redelivers to the restarted bridge.
func TestC2_KillBridgeWhileBExecutes(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 3*time.Second) // debit, then 3s before answering
	g := newGateway(t, spec, 60*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	pending := async(a, g, pay.name, r)
	eventually(t, 10*time.Second, "B debited", func() bool { return debits(t, pay, acct) == 1 })
	b.kill() // B executed; its answer will never reach this process
	startBridgeReady(t, b)

	got := <-pending
	t.Logf("caller got: %s (after %s)", got, got.elapsed.Round(time.Millisecond))
	if got.status != 201 && got.problemType() != probUnknown {
		t.Errorf("want the true outcome (201) or an explicit unknown, got %s", got)
	}
	stable(t, 3*time.Second, "no second debit after recovery", func() bool { return debits(t, pay, acct) == 1 })

	again := a.via(g, pay.name, r)
	t.Logf("retry with same key: %s replayed=%q", again, again.header.Get("Idempotent-Replayed"))
	if again.status != got.status || again.header.Get("Idempotent-Replayed") != "true" {
		t.Errorf("retry should replay the original outcome %d, got %s", got.status, again)
	}
	if n := debits(t, pay, acct); n != 1 {
		t.Fatalf("GROUND TRUTH: %d debits for one charge", n)
	}
	rs := resultsFor(results(t, pay), got.header.Get("X-Request-Id"))
	if len(rs) != 1 {
		t.Errorf("want exactly 1 Result for the request, got %d: %+v", len(rs), rs)
	} else {
		t.Logf("Result: %s fault=%q", rs[0].Outcome, rs[0].Response.Fault)
	}
}

// B answered, the bridge holds the answer but Kafka does not accept the
// commit; then the bridge dies. The narrowest form of "crash between the call
// and the publication".
func TestC2_BAnsweredKafkaFrozenBridgeKilled(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 2*time.Second)
	g := newGateway(t, spec, 90*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	pending := async(a, g, pay.name, r)
	eventually(t, 10*time.Second, "B debited", func() bool { return debits(t, pay, acct) == 1 })
	debitedAt := time.Now()
	pauseKafka(t)
	// B answers 2s after the debit; give the answer time to reach the bridge,
	// whose commit now hangs on the frozen broker.
	time.Sleep(time.Until(debitedAt.Add(3 * time.Second)))
	b.kill()
	unpauseKafka(t)
	startBridgeReady(t, b)

	got := <-pending
	t.Logf("caller got: %s (after %s)", got, got.elapsed.Round(time.Millisecond))
	if got.status != 201 && got.problemType() != probUnknown {
		t.Errorf("want 201 or explicit unknown, got %s", got)
	}
	stable(t, 3*time.Second, "single debit", func() bool { return debits(t, pay, acct) == 1 })
	again := a.via(g, pay.name, r)
	if again.status != got.status {
		t.Errorf("retry: want %d replayed, got %s", got.status, again)
	}
	if n := debits(t, pay, acct); n != 1 {
		t.Fatalf("GROUND TRUTH: %d debits", n)
	}
}

// Same, but the bridge survives the broker freeze (longer than its 10s
// transaction timeout): it must publish the real outcome, not run B again.
func TestC2_BAnsweredKafkaFrozenBridgeSurvives(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 2*time.Second)
	g := newGateway(t, spec, 90*time.Second, pay).startReady(t)
	startBridgeReady(t, newBridge(t, spec, pay, "b1"))
	a := newCaller(t, "checkout")

	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	pending := async(a, g, pay.name, r)
	eventually(t, 10*time.Second, "B debited", func() bool { return debits(t, pay, acct) == 1 })
	pauseKafka(t)
	frozen := time.Now()
	time.Sleep(time.Until(frozen.Add(15 * time.Second))) // the freeze itself is the attack
	unpauseKafka(t)

	got := <-pending
	t.Logf("caller got: %s (after %s)", got, got.elapsed.Round(time.Millisecond))
	if got.status != 201 {
		t.Errorf("bridge held B's answer across the freeze; want 201, got %s", got)
	}
	stable(t, 3*time.Second, "single debit", func() bool { return debits(t, pay, acct) == 1 })
}

// Zombie: the bridge owning the partition freezes (GC pause, VM stall) while
// B executes; the group hands the partition to a standby; the zombie wakes up
// holding B's answer and tries to publish it.
func TestC2_ZombieBridge(t *testing.T) {
	spec := t.TempDir()
	one := map[string]string{"KB_PARTITIONS": "1"} // one partition: one owner, one standby
	pay := startPayments(t, spec, 3*time.Second)
	g := newGatewayEnv(t, spec, 120*time.Second, one, pay).startReady(t)
	b1 := newBridgeEnv(t, spec, pay, "b1", one)
	startBridgeReadyN(t, b1, 1)
	b2 := newBridgeEnv(t, spec, pay, "b2", one)
	m2 := b2.mark()
	b2.start()
	eventually(t, 30*time.Second, "b2 joined the group", func() bool { return b2.countSince(m2, "partitions assigned") >= 1 })
	a := newCaller(t, "checkout")

	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	pending := async(a, g, pay.name, r)
	eventually(t, 10*time.Second, "B debited", func() bool { return debits(t, pay, acct) == 1 })
	b1.signal(syscall.SIGSTOP)
	eventually(t, 90*time.Second, "b2 took over the partition", func() bool {
		return b2.countSince(m2, `"msg":"partition ready"`) >= 1
	})
	got := <-pending
	t.Logf("caller got (zombie frozen): %s after %s", got, got.elapsed.Round(time.Millisecond))
	m1 := b1.mark()
	b1.signal(syscall.SIGCONT)
	// Let the zombie act on what it holds: it must notice it lost the
	// partition, or be fenced, without publishing a second outcome.
	eventually(t, 60*time.Second, "zombie noticed (lost partition or fenced)", func() bool {
		return b1.countSince(m1, "partitions released") >= 1 || b1.countSince(m1, "commit failed") >= 1
	})
	stable(t, 5*time.Second, "single debit after zombie woke", func() bool { return debits(t, pay, acct) == 1 })

	rs := resultsFor(results(t, pay), got.header.Get("X-Request-Id"))
	if len(rs) != 1 {
		t.Errorf("want 1 Result, got %d: %+v", len(rs), rs)
	} else {
		t.Logf("Result: %s fault=%q", rs[0].Outcome, rs[0].Response.Fault)
	}
	// The system must still serve after the fight over the partition.
	fresh := randName("acct-")
	after := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(fresh), header: map[string]string{"Idempotency-Key": "k-" + fresh}})
	t.Logf("fresh charge after zombie episode: %s after %s", after, after.elapsed.Round(time.Millisecond))
	if after.status != 201 || debits(t, pay, fresh) != 1 {
		t.Errorf("system did not recover: %s", after)
	}
	if again := a.via(g, pay.name, r); again.status != got.status {
		t.Errorf("replay after zombie episode: want %d got %s", got.status, again)
	}
	if n := debits(t, pay, acct); n != 1 {
		t.Fatalf("GROUND TRUTH: %d debits", n)
	}
}
