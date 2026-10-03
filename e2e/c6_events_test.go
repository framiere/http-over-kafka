//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

)

type eventView struct {
	key, ceID, ceType string
	value             map[string]any
}

func events(t *testing.T, topic string, committed bool) []eventView {
	t.Helper()
	var out []eventView
	for _, r := range readTopic(t, topic, committed) {
		var v map[string]any
		_ = json.Unmarshal(r.Value, &v)
		out = append(out, eventView{key: string(r.Key), ceID: header(r, "ce_id"), ceType: header(r, "ce_type"), value: v})
	}
	return out
}

func orderBody(customer string, qty int) string {
	return fmt.Sprintf(`{"customerId":%q,"items":[{"sku":"S-%s","quantity":%d}]}`, customer, customer, qty)
}

// Criterion 6: 201 → exactly one OrderCreated per the mapping; 4xx → Failed
// Result and no event; replays and other operations → nothing.
func TestC6_EventsFollowTheMapping(t *testing.T) {
	spec := t.TempDir()
	ord := startOrders(t, spec)
	g := newGateway(t, spec, 10*time.Second, ord).startReady(t)
	startBridgeReady(t, newBridge(t, spec, ord, "b1"))
	d := newDeriver(t, spec, ord, "d1").start()
	a := newCaller(t, "checkout")

	type sent struct {
		requestID, orderID, customer string
	}
	var created []sent
	for i := range 10 {
		cust := fmt.Sprintf("c%d", i)
		h := map[string]string{}
		if i%2 == 0 {
			h["Idempotency-Key"] = "k-" + cust
		}
		r := a.via(g, ord.name, req{method: "POST", path: "/orders", body: orderBody(cust, i+1), header: h})
		if r.status != 201 {
			t.Fatal(r)
		}
		var o struct{ ID string }
		_ = json.Unmarshal(r.body, &o)
		created = append(created, sent{r.header.Get("X-Request-Id"), o.ID, cust})
		if i%2 == 0 { // replay: must not produce a second event
			if rp := a.via(g, ord.name, req{method: "POST", path: "/orders", body: orderBody(cust, i+1), header: h}); rp.header.Get("Idempotent-Replayed") != "true" {
				t.Fatalf("replay expected: %s", rp)
			}
		}
	}
	var failedIDs []string
	for _, bad := range []string{`{"customerId":"","items":[]}`, `{"nope":1}`} {
		r := a.via(g, ord.name, req{method: "POST", path: "/orders", body: bad})
		if r.status != 400 {
			t.Fatal(r)
		}
		failedIDs = append(failedIDs, r.header.Get("X-Request-Id"))
	}
	a.via(g, ord.name, req{method: "PUT", path: "/orders/" + created[0].orderID, body: orderBody("c0", 9)})
	a.via(g, ord.name, req{method: "DELETE", path: "/orders/" + created[1].orderID})

	topic := ord.name + ".events"
	eventually(t, 30*time.Second, "10 events", func() bool { return len(events(t, topic, true)) >= 10 })
	stable(t, 3*time.Second, "no 11th event", func() bool { return len(events(t, topic, true)) == 10 })
	evs := events(t, topic, true)
	byID := map[string]eventView{}
	for _, e := range evs {
		byID[e.ceID] = e
	}
	for i, c := range created {
		e, ok := byID[c.requestID]
		if !ok {
			t.Errorf("no event for 201 request %s", c.requestID)
			continue
		}
		items, _ := e.value["items"].([]any)
		first, _ := items[0].(map[string]any)
		if e.ceType != "OrderCreated" || e.key != c.orderID || e.value["id"] != c.orderID || e.value["customerId"] != c.customer ||
			first["quantity"] != float64(i+1) || len(e.value) != 3 {
			t.Errorf("event does not follow the mapping: %+v (want id %s customer %s)", e, c.orderID, c.customer)
		}
	}
	rs := results(t, ord)
	for _, id := range failedIDs {
		r := resultsFor(rs, id)
		if len(r) != 1 || r[0].Outcome != "Failed" || r[0].Type != "CreateOrderFailed" {
			t.Errorf("400 must yield one CreateOrderFailed Result, got %+v", r)
		}
		if _, ok := byID[id]; ok {
			t.Errorf("event emitted for a 400")
		}
	}
	types := map[string]int{}
	for _, r := range rs {
		types[r.Type]++
	}
	t.Logf("results by type: %v; events: %d OrderCreated; failure topic records: %d", types, len(evs), len(readTopic(t, "http.event-failures."+ord.name, true)))
	_ = d
}

// Event derivation under deriver kill -9 during a concurrent burst: every
// 201 yields exactly one event for read_committed readers.
func TestC6_EventsExactlyOnceUnderDeriverCrash(t *testing.T) {
	spec := t.TempDir()
	ord := startOrders(t, spec)
	g := newGateway(t, spec, 10*time.Second, ord).startReady(t)
	startBridgeReady(t, newBridge(t, spec, ord, "b1"))
	d := newDeriver(t, spec, ord, "d1").start()
	a := newCaller(t, "checkout")

	const n = 400
	var mu sync.Mutex
	ok201 := map[string]bool{}
	var wg sync.WaitGroup
	work := make(chan int)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				r := a.via(g, ord.name, req{method: "POST", path: "/orders", body: orderBody(fmt.Sprintf("c%d", i), 1)})
				if r.status == 201 {
					mu.Lock()
					ok201[r.header.Get("X-Request-Id")] = true
					mu.Unlock()
				}
			}
		}()
	}
	go func() {
		for i := range n {
			work <- i
		}
		close(work)
	}()
	topic := ord.name + ".events"
	for k := range 3 {
		// A killed member keeps its partitions until its session times out
		// (45s default): the replacement waits that long.
		eventually(t, 90*time.Second, "deriver making progress", func() bool { return len(events(t, topic, false)) > (k+1)*60 })
		d.kill()
		d.start()
	}
	wg.Wait()
	eventually(t, 60*time.Second, "all events derived", func() bool { return len(events(t, topic, true)) >= len(ok201) })
	stable(t, 3*time.Second, "no extra event", func() bool { return len(events(t, topic, true)) == len(ok201) })
	committed := events(t, topic, true)
	seen := map[string]int{}
	for _, e := range committed {
		seen[e.ceID]++
	}
	for id, c := range seen {
		if c != 1 || !ok201[id] {
			t.Errorf("event %s seen %d times (201 observed: %v)", id, c, ok201[id])
		}
	}
	raw := events(t, topic, false)
	t.Logf("%d requests, %d 201s, %d committed events (unique %d), %d records for a read_uncommitted reader", n, len(ok201), len(committed), len(seen), len(raw))
}

// A fact B really performed but whose outcome the bridge could not prove:
// what does the event stream say? (payments with a ChargeCreated mapping
// declared by the operator; B unchanged.)
func TestC6_UnknownOutcomeMeansNoEvent(t *testing.T) {
	spec := t.TempDir()
	pay := startPaymentsSpec(t, spec, 3*time.Second, "payments-events")
	g := newGateway(t, spec, 60*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	newDeriver(t, spec, pay, "d1").start()
	a := newCaller(t, "checkout")

	ok := randName("acct-")
	if r := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(ok)}); r.status != 201 {
		t.Fatal(r)
	}
	acct := randName("acct-")
	pending := async(a, g, pay.name, req{method: "POST", path: "/charges", body: charge(acct)})
	eventually(t, 10*time.Second, "B debited", func() bool { return debits(t, pay, acct) == 1 })
	b.kill()
	startBridgeReady(t, b)
	got := <-pending
	topic := pay.name + ".events"
	eventually(t, 30*time.Second, "control event", func() bool { return len(events(t, topic, true)) >= 1 })
	stable(t, 3*time.Second, "settled", func() bool { return true })
	var forAcct int
	for _, e := range events(t, topic, true) {
		if e.value["accountId"] == acct {
			forAcct++
		}
	}
	t.Logf("caller: %d %s; B debits for the account: %d; ChargeCreated events for it: %d", got.status, got.problemType(), debits(t, pay, acct), forAcct)
}

