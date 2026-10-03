//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Commands published before a service's bridge ever ran predate its dedup
// memory: never executed, answered unknown (bridge contract since 18:01).
func TestD3_GenesisCommandsNotExecuted(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	a := newCaller(t, "checkout")
	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	if got := a.via(g, pay.name, r); got.status != 504 {
		t.Fatalf("no bridge yet: want 504, got %s", got)
	}
	startBridgeReady(t, newBridge(t, spec, pay, "b1"))
	got, _ := retryUntilAnswer(t, a, g, pay.name, r, 20)
	t.Logf("retry after first bridge start: %s", got)
	stable(t, 3*time.Second, "never executed", func() bool { return debits(t, pay, acct) == 0 })
	if got.status == 201 {
		t.Errorf("a pre-memory command was executed")
	}
	fresh := randName("acct-")
	if f := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(fresh)}); f.status != 201 {
		t.Errorf("new command after genesis: %s", f)
	}
}

// The recovery the bridge's refusal message prescribes ("delete
// http.bridge-layout.<svc>, http.bridge-state.<svc> and the consumer group
// once no command is in flight or retryable"), applied by an operator while
// callers still hold Idempotency-Keys they may retry.
func TestD3_WipeWhileKeysAreRetryable(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	done := randName("acct-")
	rDone := req{method: "POST", path: "/charges", body: charge(done), header: map[string]string{"Idempotency-Key": "k-" + done}}
	if got := a.via(g, pay.name, rDone); got.status != 201 {
		t.Fatal(got)
	}
	b.term(30 * time.Second)
	// In flight across the wipe: published while the bridge is down.
	pending := randName("acct-")
	rPending := req{method: "POST", path: "/charges", body: charge(pending), header: map[string]string{"Idempotency-Key": "k-" + pending}}
	if got := a.via(g, pay.name, rPending); got.status != 504 {
		t.Fatalf("bridge down: want 504, got %s", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adm := kadm.NewClient(kclient(t))
	wipe := func(topics ...string) {
		if _, err := adm.DeleteTopics(ctx, topics...); err != nil {
			t.Fatal(err)
		}
		eventually(t, 30*time.Second, "topics deleted", func() bool {
			td, _ := adm.ListTopics(ctx, topics...)
			for _, tp := range topics {
				if td.Has(tp) {
					return false
				}
			}
			return true
		})
	}
	wipe("http.bridge-layout."+pay.name, "http.bridge-state."+pay.name)
	if _, err := adm.DeleteGroups(ctx, "kb-bridge."+pay.name); err != nil {
		t.Fatal(err)
	}
	t.Logf("operator: deleted layout, state and consumer group, as the refusal message says")

	m := b.mark()
	b.start()
	eventually(t, 60*time.Second, "bridge serving or refusing", func() bool {
		return b.countSince(m, `"msg":"partition ready"`) >= partitions || b.countSince(m, "refusing to serve") >= 1
	})
	if b.countSince(m, "refusing to serve") >= 1 {
		t.Logf("CONFIRMED: the prescribed procedure is not enough, the bridge still refuses: %s", lastLine(b.logs()))
		// The operator goes further: results too (audit history is lost).
		wipe("http.results." + pay.name)
		m = b.mark()
		b.start()
		eventually(t, 60*time.Second, "bridge serving after total wipe", func() bool {
			return b.countSince(m, `"msg":"partition ready"`) >= partitions || b.countSince(m, "refusing to serve") >= 1
		})
		if b.countSince(m, "refusing to serve") >= 1 {
			t.Fatalf("bridge refuses even after a total wipe: %s", lastLine(b.logs()))
		}
		t.Logf("operator: deleted http.results too; bridge starts a new dedup memory")
	}

	gotDone, _ := retryUntilAnswer(t, a, g, pay.name, rDone, 20)
	gotPending, _ := retryUntilAnswer(t, a, g, pay.name, rPending, 20)
	stable(t, 3*time.Second, "settled", func() bool { return true })
	t.Logf("retry of a key answered 201 before the wipe: %s replayed=%q -> debits=%d", gotDone, gotDone.header.Get("Idempotent-Replayed"), debits(t, pay, done))
	t.Logf("retry of a key in flight across the wipe: %d %s -> debits=%d", gotPending.status, gotPending.problemType(), debits(t, pay, pending))
	if debits(t, pay, done) > 1 {
		t.Errorf("DOUBLE DEBIT: a retry with the original Idempotency-Key ran B again after the wipe")
	}
	if debits(t, pay, pending) > 1 {
		t.Errorf("DOUBLE DEBIT on the in-flight key")
	}
}

func lastLine(s string) string {
	for i := len(s) - 2; i >= 0; i-- {
		if s[i] == '\n' {
			if len(s)-i > 400 {
				return s[i+1 : i+400]
			}
			return s[i+1:]
		}
	}
	return s
}

// Same total wipe, but the command topic no longer holds the original
// commands (short retention.ms, or an operator purging the backlog): the
// genesis bridge cannot claim their keys.
func TestD3_WipeAfterCommandsExpired(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")
	acct := randName("acct-")
	r := req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}
	if got := a.via(g, pay.name, r); got.status != 201 {
		t.Fatal(got)
	}
	b.term(30 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adm := kadm.NewClient(kclient(t))
	cmdTopic := "http.requests." + pay.name
	ends, err := adm.ListEndOffsets(ctx, cmdTopic)
	if err != nil {
		t.Fatal(err)
	}
	var del kadm.Offsets
	ends.Each(func(o kadm.ListedOffset) { del.AddOffset(cmdTopic, o.Partition, o.Offset, -1) })
	if _, err := adm.DeleteRecords(ctx, del); err != nil {
		t.Fatal(err)
	}
	if _, err := adm.DeleteTopics(ctx, "http.bridge-layout."+pay.name, "http.bridge-state."+pay.name, "http.results."+pay.name); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "topics deleted", func() bool {
		td, _ := adm.ListTopics(ctx, "http.bridge-state."+pay.name, "http.results."+pay.name, "http.bridge-layout."+pay.name)
		for _, d := range td {
			if d.Err == nil {
				return false
			}
		}
		return true
	})
	if _, err := adm.DeleteGroups(ctx, "kb-bridge."+pay.name); err != nil {
		t.Fatal(err)
	}
	t.Logf("operator: command records expired/purged, then total wipe")
	m := b.mark()
	b.start()
	eventually(t, 60*time.Second, "bridge serving", func() bool { return b.countSince(m, `"msg":"partition ready"`) >= partitions })
	got, _ := retryUntilAnswer(t, a, g, pay.name, r, 20)
	stable(t, 3*time.Second, "settled", func() bool { return true })
	t.Logf("retry with the original key: %s replayed=%q -> debits=%d", got, got.header.Get("Idempotent-Replayed"), debits(t, pay, acct))
	if debits(t, pay, acct) > 1 {
		t.Errorf("DOUBLE DEBIT after wipe once the original command left the topic")
	}
	// What the protection costs: a brand-new key after such a wipe.
	fresh := randName("acct-")
	nk, _ := retryUntilAnswer(t, a, g, pay.name, req{method: "POST", path: "/charges", body: charge(fresh), header: map[string]string{"Idempotency-Key": "k-" + fresh}}, 20)
	nn, _ := retryUntilAnswer(t, a, g, pay.name, req{method: "POST", path: "/charges", body: charge(fresh + "-nokey")}, 20)
	t.Logf("after wipe with purged history: new keyed charge %d %s (debits %d); new unkeyed charge %d", nk.status, nk.problemType(), debits(t, pay, fresh), nn.status)
}

// The full operator story behind the refusal: partitions are increased, the
// bridge refuses, the operator applies the reset it prescribes (plus results,
// without which it still refuses) and restarts. Callers retry their keys.
func TestD3_RepartitionThenPrescribedReset(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")
	var accts []string
	for range 12 {
		acct := randName("acct-")
		if got := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}); got.status != 201 {
			t.Fatal(got)
		}
		accts = append(accts, acct)
	}
	b.term(30 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	adm := kadm.NewClient(kclient(t))
	if resp, err := adm.UpdatePartitions(ctx, 12, "http.requests."+pay.name, "http.results."+pay.name, "http.bridge-state."+pay.name); err != nil || resp.Error() != nil {
		t.Fatalf("add partitions: %v %v", err, resp.Error())
	}
	m := b.mark()
	b.start()
	eventually(t, 60*time.Second, "bridge refuses", func() bool { return b.countSince(m, "refusing to serve") >= 1 })
	t.Logf("bridge refuses after the partition increase, as designed")

	topics := []string{"http.bridge-layout." + pay.name, "http.bridge-state." + pay.name, "http.results." + pay.name}
	if _, err := adm.DeleteTopics(ctx, topics...); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "topics deleted", func() bool {
		td, _ := adm.ListTopics(ctx, topics...)
		for _, d := range td {
			if d.Err == nil {
				return false
			}
		}
		return true
	})
	if _, err := adm.DeleteGroups(ctx, "kb-bridge."+pay.name); err != nil {
		t.Fatal(err)
	}
	// Every binary would recreate http.results with KB_PARTITIONS (6) and the
	// bridge would then refuse on mismatched counts: the operator creates it
	// with 12 by hand.
	if _, err := adm.CreateTopic(ctx, 12, -1, map[string]*string{"max.message.bytes": ptr("8388608")}, "http.results."+pay.name); err != nil {
		t.Fatal(err)
	}
	g.kill()
	g.startReady(t) // picks up 12 partitions
	m = b.mark()
	b.start()
	eventually(t, 90*time.Second, "bridge serving 12 partitions", func() bool { return b.countSince(m, `"msg":"partition ready"`) >= 12 })
	t.Logf("operator reset applied; bridge serving on 12 partitions")

	doubled := 0
	answers := map[string]int{}
	for _, acct := range accts {
		got, _ := retryUntilAnswer(t, a, g, pay.name, req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}, 20)
		answers[got.problemType()+" "+got.header.Get("Idempotent-Replayed")+" "+itoa(got.status)]++
		if debits(t, pay, acct) > 1 {
			doubled++
		}
	}
	t.Logf("retries: %v", answers)
	if doubled > 0 {
		t.Errorf("DOUBLE DEBIT: %d of %d keys executed again after the prescribed reset", doubled, len(accts))
	}
}

func itoa(i int) string { return fmt.Sprint(i) }

func ptr(s string) *string { return &s }

// The reset as the bridge's refusal message now prescribes it (stop, delete
// layout and state, restart), after a partition increase, with keys still
// retryable; then a brand-new keyed charge, to see what "blind" mode costs.
func TestD3_RepartitionThenCurrentPrescribedReset(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 1*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")
	var accts []string
	for range 12 {
		acct := randName("acct-")
		if got := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}); got.status != 201 {
			t.Fatal(got)
		}
		accts = append(accts, acct)
	}
	b.term(30 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	adm := kadm.NewClient(kclient(t))
	if resp, err := adm.UpdatePartitions(ctx, 12, "http.requests."+pay.name, "http.results."+pay.name, "http.bridge-state."+pay.name); err != nil || resp.Error() != nil {
		t.Fatalf("add partitions: %v %v", err, resp.Error())
	}
	m := b.mark()
	b.start()
	eventually(t, 60*time.Second, "bridge refuses", func() bool { return b.countSince(m, "refusing to serve") >= 1 })
	t.Logf("refusal: %s", lastLine(b.logs()))
	topics := []string{"http.bridge-layout." + pay.name, "http.bridge-state." + pay.name}
	if _, err := adm.DeleteTopics(ctx, topics...); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "topics deleted", func() bool {
		td, _ := adm.ListTopics(ctx, topics...)
		for _, d := range td {
			if d.Err == nil {
				return false
			}
		}
		return true
	})
	g.kill()
	g.startReady(t)
	m = b.mark()
	b.start()
	eventually(t, 90*time.Second, "bridge serving 12 partitions", func() bool { return b.countSince(m, `"msg":"partition ready"`) >= 12 })

	doubled := 0
	answers := map[string]int{}
	for _, acct := range accts {
		got, _ := retryUntilAnswer(t, a, g, pay.name, req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}, 20)
		answers[fmt.Sprintf("%d %s replayed=%s", got.status, got.problemType(), got.header.Get("Idempotent-Replayed"))]++
		if debits(t, pay, acct) > 1 {
			doubled++
		}
	}
	t.Logf("retries of old keys: %v", answers)
	if doubled > 0 {
		t.Errorf("DOUBLE DEBIT: %d of %d", doubled, len(accts))
	}
	fresh := randName("acct-")
	nk, _ := retryUntilAnswer(t, a, g, pay.name, req{method: "POST", path: "/charges", body: charge(fresh), header: map[string]string{"Idempotency-Key": "k-" + fresh}}, 20)
	nn, _ := retryUntilAnswer(t, a, g, pay.name, req{method: "POST", path: "/charges", body: charge(fresh + "-nokey")}, 20)
	t.Logf("after reset: new keyed charge %d %s (debits %d); new unkeyed charge %d", nk.status, nk.problemType(), debits(t, pay, fresh), nn.status)
}
