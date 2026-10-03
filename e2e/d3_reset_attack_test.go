//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/twmb/franz-go/pkg/kadm"
)

func admin(t *testing.T) (*kadm.Client, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return kadm.NewClient(kclient(t)), ctx
}

func deleteTopics(t *testing.T, topics ...string) {
	t.Helper()
	adm, ctx := admin(t)
	if _, err := adm.DeleteTopics(ctx, topics...); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, fmt.Sprintf("deleted %v", topics), func() bool {
		td, _ := adm.ListTopics(ctx, topics...)
		for _, d := range td {
			if d.Err == nil {
				return false
			}
		}
		return true
	})
}

// resetMemory is the reset the bridge's refusal message prescribes: with
// the bridges stopped, delete layout and state.
func resetMemory(t *testing.T, s *svc) {
	deleteTopics(t, "http.bridge-layout."+s.name, "http.bridge-state."+s.name)
}

func keyed(acct string) req {
	return req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
}

// startBridgeOutcome starts b and waits until it serves all its partitions or
// refuses; it reports whether it refused.
func startBridgeOutcome(t *testing.T, b *proc, n int) bool {
	t.Helper()
	m := b.mark()
	b.start()
	eventually(t, 120*time.Second, b.name+" serving or refusing", func() bool {
		return b.countSince(m, `"msg":"partition ready"`) >= n || b.countSince(m, "refusing to serve") >= 1
	})
	return b.countSince(m, "refusing to serve") >= 1
}

// Deployment order reversed: the bridge comes up before any gateway (and so
// before the service's command topic exists), then traffic starts the moment
// the gateway is ready. No first command may be lost to the genesis.
func TestOrder_BridgeBeforeGateway(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	b := newBridge(t, spec, pay, "b1")
	m := b.mark()
	b.start()
	g := newGateway(t, spec, 5*time.Second, pay)
	g.startReady(t)
	a := newCaller(t, "checkout")

	// Fire immediately, before the bridge reports ready.
	var wg sync.WaitGroup
	got := make([]reply, 24)
	accts := make([]string, 24)
	for i := range got {
		accts[i] = randName("acct-")
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Unkeyed: one attempt, a blind retry would be the caller's own
			// double charge (D4). Keyed: retried on 504 like a good client.
			if i%2 == 0 {
				got[i], _ = retryUntilAnswer(t, a, g, pay.name, keyed(accts[i]), 20)
			} else {
				got[i] = a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(accts[i])})
			}
		}()
	}
	wg.Wait()
	eventually(t, 60*time.Second, "bridge ready", func() bool { return b.countSince(m, `"msg":"partition ready"`) >= partitions })
	counts := map[string]int{}
	for i, r := range got {
		d := debits(t, pay, accts[i])
		counts[fmt.Sprintf("%d %s debits=%d", r.status, r.problemType(), d)]++
		if d > 1 || (r.status == 201 && d != 1) {
			t.Errorf("%s: %s with %d debits", accts[i], r, d)
		}
	}
	t.Logf("first 24 commands, bridge started before gateway: %v", counts)
	for _, line := range []string{"dedup memory started", "predates the dedup memory"} {
		t.Logf("bridge log %q: %d", line, b.countSince(m, line))
	}
	// What must not happen: a command lost to the genesis (unknown, never
	// executed). A 503 transport_unavailable is fine if B did not run.
	for i, r := range got {
		if r.problemType() == probUnknown {
			t.Errorf("%s: first command answered unknown (lost to the genesis)", accts[i])
		}
		if classify(r) == vNotApplied && debits(t, pay, accts[i]) != 0 {
			t.Errorf("%s: told not applied but debited", accts[i])
		}
	}
}

// The prescribed reset applied while callers keep sending keyed charges and
// retrying them. Oracle: payments ground truth.
func TestReset_UnderKeyedTraffic(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 20*time.Millisecond)
	g := newGateway(t, spec, 2*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	var stop atomic.Bool
	var mu sync.Mutex
	type res struct {
		acct  string
		final reply
		seen  []verdict
	}
	var all []res
	var seq atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				acct := fmt.Sprintf("acct-%d", seq.Add(1))
				var seen []verdict
				var last reply
				giveUp := time.Now().Add(3 * time.Minute)
				for {
					last = a.via(g, pay.name, keyed(acct))
					v := classify(last)
					seen = append(seen, v)
					if !(v == vTimeout || v == vTransport || v == vNotApplied) || time.Now().After(giveUp) {
						break
					}
					time.Sleep(200 * time.Millisecond)
				}
				mu.Lock()
				all = append(all, res{acct, last, seen})
				mu.Unlock()
			}
		}()
	}
	time.Sleep(4 * time.Second) // traffic before the reset
	b.term(30 * time.Second)
	time.Sleep(3 * time.Second) // retries pile up while the bridge is down
	resetMemory(t, pay)
	refused := startBridgeOutcome(t, b, partitions)
	if refused {
		t.Fatalf("bridge refuses after the prescribed reset: %s", lastLine(b.logs()))
	}
	time.Sleep(6 * time.Second) // traffic after the reset
	stop.Store(true)
	wg.Wait()
	stable(t, 5*time.Second, "settled", func() bool { return true })

	counts := map[string]int{}
	for _, r := range all {
		d := debits(t, pay, r.acct)
		v := classify(r.final)
		counts[fmt.Sprintf("%s debits=%d", v, d)]++
		switch {
		case d > 1:
			t.Errorf("DOUBLE DEBIT %s (answers %v)", r.acct, r.seen)
		case v == vOK && d != 1:
			t.Errorf("201 without debit %s", r.acct)
		case v == vNotApplied && d != 0:
			t.Errorf("not-applied but debited %s", r.acct)
		}
	}
	t.Logf("%d keyed jobs across a live reset: %v", len(all), counts)
}

// Partial wipes: one of the two topics of the dedup memory is lost.
func TestReset_PartialWipes(t *testing.T) {
	for _, c := range []struct {
		name   string
		delete func(s *svc) []string
	}{
		{"state only", func(s *svc) []string { return []string{"http.bridge-state." + s.name} }},
		{"layout only", func(s *svc) []string { return []string{"http.bridge-layout." + s.name} }},
	} {
		t.Run(c.name, func(t *testing.T) {
			spec := t.TempDir()
			pay := startPayments(t, spec, 0)
			g := newGateway(t, spec, 2*time.Second, pay).startReady(t)
			b := newBridge(t, spec, pay, "b1")
			startBridgeReady(t, b)
			a := newCaller(t, "checkout")
			acct := randName("acct-")
			if r := a.via(g, pay.name, keyed(acct)); r.status != 201 {
				t.Fatal(r)
			}
			b.term(30 * time.Second)
			deleteTopics(t, c.delete(pay)...)
			refused := startBridgeOutcome(t, b, partitions)
			r, _ := retryUntilAnswer(t, a, g, pay.name, keyed(acct), 3)
			t.Logf("refused=%v; retry: %d %s; debits=%d", refused, r.status, r.problemType(), debits(t, pay, acct))
			if refused {
				t.Logf("refusal: %s", lastLine(b.logs()))
			}
			if debits(t, pay, acct) > 1 {
				t.Errorf("DOUBLE DEBIT after partial wipe (%s)", c.name)
			}
		})
	}
	// The state topic deleted under a running bridge.
	t.Run("state deleted while running", func(t *testing.T) {
		spec := t.TempDir()
		pay := startPayments(t, spec, 0)
		g := newGateway(t, spec, 2*time.Second, pay).startReady(t)
		b := newBridge(t, spec, pay, "b1")
		startBridgeReady(t, b)
		a := newCaller(t, "checkout")
		var accts []string
		for range 6 {
			acct := randName("acct-")
			if r := a.via(g, pay.name, keyed(acct)); r.status != 201 {
				t.Fatal(r)
			}
			accts = append(accts, acct)
		}
		m := b.mark()
		deleteTopics(t, "http.bridge-state."+pay.name)
		for _, acct := range accts {
			r, _ := retryUntilAnswer(t, a, g, pay.name, keyed(acct), 3)
			t.Logf("retry %s: %d %s debits=%d", acct, r.status, r.problemType(), debits(t, pay, acct))
		}
		fresh := randName("acct-")
		r := a.via(g, pay.name, keyed(fresh))
		t.Logf("new charge after deletion: %d %s debits=%d", r.status, r.problemType(), debits(t, pay, fresh))
		for _, acct := range accts {
			if debits(t, pay, acct) > 1 {
				t.Errorf("DOUBLE DEBIT %s after state deletion under a running bridge", acct)
			}
		}
		// The deletion must stop the bridge explicitly, not leave it looping.
		eventually(t, 60*time.Second, "bridge exits with an explicit message", func() bool {
			return !b.alive() && b.countSince(m, "deleted while running") >= 1
		})
		t.Logf("bridge exited: %s", lastLine(b.logs()))
	})
}

// Blind mode, partially purged history: only one command partition lost its
// records (DeleteRecords: same effect as retention on that partition).
func TestBlind_OnePartitionPurged(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 2*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")
	var accts []string
	for range 24 {
		acct := randName("acct-")
		if r := a.via(g, pay.name, keyed(acct)); r.status != 201 {
			t.Fatal(r)
		}
		accts = append(accts, acct)
	}
	b.term(30 * time.Second)
	adm, ctx := admin(t)
	cmdTopic := "http.requests." + pay.name
	ends, err := adm.ListEndOffsets(ctx, cmdTopic)
	if err != nil {
		t.Fatal(err)
	}
	var del kadm.Offsets
	ends.Each(func(o kadm.ListedOffset) {
		if o.Partition == 0 { // half of partition 0 only
			del.AddOffset(cmdTopic, 0, o.Offset/2+1, -1)
			t.Logf("purging partition 0 below offset %d of %d", o.Offset/2+1, o.Offset)
		}
	})
	if _, err := adm.DeleteRecords(ctx, del); err != nil {
		t.Fatal(err)
	}
	resetMemory(t, pay)
	if startBridgeOutcome(t, b, partitions) {
		t.Fatalf("refused: %s", lastLine(b.logs()))
	}
	counts := map[string]int{}
	for _, acct := range accts {
		r, _ := retryUntilAnswer(t, a, g, pay.name, keyed(acct), 5)
		counts[fmt.Sprintf("%d %s", r.status, r.problemType())]++
		if debits(t, pay, acct) > 1 {
			t.Errorf("DOUBLE DEBIT %s", acct)
		}
	}
	fresh := randName("acct-")
	nk := a.via(g, pay.name, keyed(fresh))
	nn := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(fresh + "-x")})
	t.Logf("retries of 24 old keys: %v; new keyed: %d %s (debits %d); new unkeyed: %d", counts, nk.status, nk.problemType(), debits(t, pay, fresh), nn.status)
	if nk.status == 201 {
		t.Errorf("blind mode not engaged although window history was purged")
	}
}

// Signing key rotation (old key dropped from KB_TRUSTED_KEYS), later followed
// by the prescribed reset, within the idempotency window.
func TestReset_AfterKeyRotation(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	sign1, trust1, _ := identity.Generate("rot-1")
	sign2, trust2, _ := identity.Generate("rot-2")
	g := newGatewayEnv(t, spec, 2*time.Second, map[string]string{"KB_GATEWAY_SIGNING_KEY": sign1}, pay).startReady(t)
	b := newBridgeEnv(t, spec, pay, "b1", map[string]string{"KB_TRUSTED_GATEWAY_KEYS": trust1})
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")
	var accts []string
	for range 12 {
		acct := randName("acct-")
		if r := a.via(g, pay.name, keyed(acct)); r.status != 201 {
			t.Fatal(r)
		}
		accts = append(accts, acct)
	}
	// Rotation: gateway signs with key 2, bridges trust key 2 only.
	g.kill()
	b.term(30 * time.Second)
	g2 := newGatewayEnv(t, spec, 2*time.Second, map[string]string{"KB_GATEWAY_SIGNING_KEY": sign2}, pay)
	g2.instance = g.instance
	g2.startReady(t)
	b2 := newBridgeEnv(t, spec, pay, "b1r", map[string]string{"KB_TRUSTED_GATEWAY_KEYS": trust2})
	startBridgeReady(t, b2)
	before := map[string]int{}
	for _, acct := range accts[:6] {
		r, _ := retryUntilAnswer(t, a, g2, pay.name, keyed(acct), 5)
		before[fmt.Sprintf("%d replayed=%s", r.status, r.header.Get("Idempotent-Replayed"))]++
	}
	t.Logf("after rotation, no reset, retries: %v", before)
	b2.term(30 * time.Second)
	resetMemory(t, pay)
	if startBridgeOutcome(t, b2, partitions) {
		t.Fatalf("refused: %s", lastLine(b2.logs()))
	}
	after := map[string]int{}
	doubled := 0
	for _, acct := range accts {
		r, _ := retryUntilAnswer(t, a, g2, pay.name, keyed(acct), 5)
		after[fmt.Sprintf("%d %s replayed=%s", r.status, r.problemType(), r.header.Get("Idempotent-Replayed"))]++
		if debits(t, pay, acct) > 1 {
			doubled++
		}
	}
	t.Logf("after rotation then reset, retries: %v", after)
	if doubled > 0 {
		t.Errorf("DOUBLE DEBIT: %d of %d keys ran again; their original commands are signed by a key the bridge no longer trusts, so the genesis scan skipped them", doubled, len(accts))
	}
}
