//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// An operator scales a hot service the usual Kafka way: more partitions on
// its topics (all three, keeping the counts equal as the bridge requires),
// then rolls gateway and bridge. Does an Idempotency-Key retry still find
// its original outcome?
func TestD3_PartitionIncreaseNoDoubleDebit(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 3*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	// Several keys, so that some of them hash differently once the
	// partition count changes.
	var accts []string
	for i := range 12 {
		acct := fmt.Sprintf("%s-%d", randName("acct-"), i)
		r := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}})
		if r.status != 201 {
			t.Fatal(r)
		}
		accts = append(accts, acct)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	adm := kadm.NewClient(kclient(t))
	topics := []string{"http.requests." + pay.name, "http.results." + pay.name, "http.bridge-state." + pay.name}
	resp, err := adm.UpdatePartitions(ctx, 12, topics...)
	if err != nil || resp.Error() != nil {
		t.Fatalf("add partitions: %v %v", err, resp.Error())
	}
	t.Logf("operator: %v now have 12 partitions", topics)

	retryAll := func(phase string) {
		statuses := map[string]int{}
		doubled := 0
		for _, acct := range accts {
			r, _ := retryUntilAnswer(t, a, g, pay.name, req{method: "POST", path: "/charges", body: charge(acct), header: map[string]string{"Idempotency-Key": "k-" + acct}}, 2)
			statuses[fmt.Sprintf("%d replayed=%q", r.status, r.header.Get("Idempotent-Replayed"))]++
			if debits(t, pay, acct) > 1 {
				doubled++
			}
		}
		t.Logf("%s: retries answered %v", phase, statuses)
		if doubled > 0 {
			t.Errorf("DOUBLE DEBIT (%s): %d of %d idempotent retries executed B again", phase, doubled, len(accts))
		}
	}

	// Phase A: the gateway picks up the new layout (restart), the running
	// bridge has not been told.
	g.kill()
	g.startReady(t)
	retryAll("bridge still running, gateway restarted")

	// Phase B: the bridge restarts on the new layout.
	b.kill()
	m := b.mark()
	b.start()
	eventually(t, 120*time.Second, "bridge serving or refusing", func() bool {
		return b.countSince(m, `"msg":"partition ready"`) >= 12 || b.countSince(m, "dedup state identity broken") >= 1
	})
	if b.countSince(m, "dedup state identity broken") >= 1 {
		t.Logf("bridge refuses to start on the new layout (fail-closed): the service is down until an operator acts")
	}
	retryAll("bridge restarted")
	stable(t, 3*time.Second, "no late execution", func() bool {
		for _, acct := range accts {
			if debits(t, pay, acct) > 1 {
				return false
			}
		}
		return true
	})
}
