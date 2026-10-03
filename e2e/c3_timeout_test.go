//go:build e2e

package e2e

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// retryUntilAnswer resends r with the same key while the gateway answers 504,
// as a well-behaved client following the 504's own advice would.
func retryUntilAnswer(t *testing.T, c *caller, g *gw, host string, r req, max int) (reply, int) {
	t.Helper()
	for i := 1; ; i++ {
		got := c.via(g, host, r)
		if got.status != 504 || i >= max {
			return got, i
		}
	}
}

func TestC3_TimeoutThenRetrySameKey(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 3*time.Second)
	g1 := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	g2 := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	startBridgeReady(t, newBridge(t, spec, pay, "b1"))
	a := newCaller(t, "checkout")

	acct := randName("acct-")
	key := "k-" + acct
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": key}}
	first := a.via(g1, pay.name, r)
	var p struct {
		Type, Detail, RequestID string
	}
	_ = json.Unmarshal(first.body, &p)
	t.Logf("first: %s", first)
	if first.status != 504 || p.Type != probTimeout || p.RequestID == "" || p.RequestID != first.header.Get("X-Request-Id") {
		t.Fatalf("want 504 gateway-timeout with requestId in body and header, got %s", first)
	}
	if !strings.Contains(p.Detail, "Idempotency-Key") {
		t.Errorf("504 detail does not tell the caller how to recover: %q", p.Detail)
	}
	got, tries := retryUntilAnswer(t, a, g2, pay.name, r, 20) // retry lands on another gateway instance
	t.Logf("after %d retries via another gateway: %s replayed=%q", tries, got, got.header.Get("Idempotent-Replayed"))
	if got.status != 201 || got.header.Get("Idempotent-Replayed") != "true" {
		t.Errorf("want the original 201, replayed, got %s", got)
	}
	stable(t, 2*time.Second, "B executed once", func() bool { return debits(t, pay, acct) == 1 })

	// Same key, different body: an error, not a new execution.
	other := r
	other.body = strings.Replace(charge(acct), "100", "999", 1)
	reused := a.via(g1, pay.name, other)
	t.Logf("same key, other body: %s", reused)
	if reused.status != 422 || reused.problemType() != probReused {
		t.Errorf("want 422 idempotency_key_reused, got %s", reused)
	}
	// Same key, same body, irrelevant header change: still a replay.
	r2 := r
	r2.header = map[string]string{"Idempotency-Key": key, "User-Agent": "other/2", "Accept-Language": "de"}
	if rp := a.via(g1, pay.name, r2); rp.status != 201 {
		t.Errorf("same request with other incidental headers: want replayed 201, got %s", rp)
	}
	if n := debits(t, pay, acct); n != 1 {
		t.Fatalf("GROUND TRUTH: %d debits", n)
	}

	// Scope is the calling application (D4: "même appelant"): another
	// application reusing the key is a different request. Observed, not judged.
	b := newCaller(t, "billing")
	x, _ := retryUntilAnswer(t, b, g1, pay.name, r, 20)
	t.Logf("other application, same key: %s; debits now %d", x, debits(t, pay, acct))
}

// Retries queue up while the (already deployed) bridge is down: when it comes
// back, the first executes and every queued retry is answered from it.
func TestC3_RetriesQueuedWhileBridgeDown(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b) // first run: the bridge's dedup memory starts here
	b.term(30 * time.Second)
	a := newCaller(t, "checkout")

	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	for i := 0; i < 5; i++ {
		if got := a.via(g, pay.name, r); got.status != 504 {
			t.Fatalf("no bridge: want 504, got %s", got)
		}
	}
	startBridgeReady(t, b)
	got, tries := retryUntilAnswer(t, a, g, pay.name, r, 20)
	t.Logf("after bridge start, %d tries: %s replayed=%q", tries, got, got.header.Get("Idempotent-Replayed"))
	if got.status != 201 {
		t.Errorf("want 201, got %s", got)
	}
	stable(t, 3*time.Second, "one debit for 6+ commands sharing a key", func() bool { return debits(t, pay, acct) == 1 })
	rs := results(t, pay)
	n := 0
	for _, x := range rs {
		if x.Command.IdempotencyKey == "k-"+acct {
			n++
		}
	}
	if n != 1 {
		t.Errorf("want 1 Result for the key (replays produce none), got %d", n)
	}
}

// Without an Idempotency-Key the 504 says "may still be applied" and a blind
// retry is a new command. This documents the default, it is not a defect:
// the guarantee the VP asked about requires the key.
func TestC3_NoKeyBlindRetryIsANewCharge(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 2*time.Second)
	g := newGateway(t, spec, 500*time.Millisecond, pay).startReady(t)
	startBridgeReady(t, newBridge(t, spec, pay, "b1"))
	a := newCaller(t, "checkout")
	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct)}
	first := a.via(g, pay.name, r)
	second := a.via(g, pay.name, r)
	eventually(t, 10*time.Second, "both executed", func() bool { return debits(t, pay, acct) == 2 })
	t.Logf("no key: %d then %d -> %d debits (expected by D4: the key is what makes a retry safe)", first.status, second.status, debits(t, pay, acct))
}

// A client that fires the same keyed request many times at once, through
// several gateways (retry storm, double-click, two pods of the caller).
func TestC3_ConcurrentSameKey(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 500*time.Millisecond)
	gws := []*gw{newGateway(t, spec, 30*time.Second, pay).startReady(t), newGateway(t, spec, 30*time.Second, pay).startReady(t)}
	startBridgeReady(t, newBridge(t, spec, pay, "b1"))
	a := newCaller(t, "checkout")
	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	out := make(chan reply, 20)
	for i := range 20 {
		go func() { out <- a.via(gws[i%2], pay.name, r) }()
	}
	ids := map[string]int{}
	replayed := 0
	for range 20 {
		got := <-out
		if got.status != 201 {
			t.Errorf("want 201, got %s", got)
			continue
		}
		var c struct{ ID string }
		_ = json.Unmarshal(got.body, &c)
		ids[c.ID]++
		if got.header.Get("Idempotent-Replayed") == "true" {
			replayed++
		}
	}
	stable(t, 2*time.Second, "one debit", func() bool { return debits(t, pay, acct) == 1 })
	t.Logf("20 concurrent identical keyed requests over 2 gateways: charge ids %v, %d replayed, %d debit", ids, replayed, debits(t, pay, acct))
	if len(ids) != 1 {
		t.Errorf("callers saw %d different charges", len(ids))
	}
}
