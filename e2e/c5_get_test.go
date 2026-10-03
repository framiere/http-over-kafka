//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

// Criterion 5: reads never write to Kafka. Every topic's end offset, internal
// ones included, is compared before and after a burst of reads of every shape.
func TestC5_GetWritesNothing(t *testing.T) {
	spec := t.TempDir()
	ord := startOrders(t, spec)
	e := startEcho(t, spec)
	g := newGateway(t, spec, 10*time.Second, ord, e).startReady(t)
	startBridgeReady(t, newBridge(t, spec, ord, "b1"))
	a := newCaller(t, "checkout")
	created := a.via(g, ord.name, req{method: "POST", path: "/orders", body: `{"customerId":"c","items":[{"sku":"s","quantity":1}]}`})
	if created.status != 201 {
		t.Fatal(created)
	}
	loc := created.header.Get("Location")

	// Let the mutation's own writes (and group offset commits) settle.
	var before map[string]int64
	eventually(t, 30*time.Second, "offsets quiet", func() bool {
		x := endOffsets(t)
		time.Sleep(2 * time.Second)
		y := endOffsets(t)
		before = y
		quiet := true
		for k, v := range x {
			if y[k] != v {
				t.Logf("not quiet yet: %s %d -> %d", k, v, y[k])
				quiet = false
			}
		}
		return quiet
	})

	reads := []struct {
		host string
		r    req
		want int
	}{
		{ord.name, req{method: "GET", path: "/orders"}, 200},
		{ord.name, req{method: "GET", path: loc}, 200},
		{ord.name, req{method: "GET", path: "/orders/missing"}, 404},
		{ord.name, req{method: "HEAD", path: loc}, 200},
		{ord.name, req{method: "GET", path: loc, header: map[string]string{"Idempotency-Key": "k1"}}, 200},
		{ord.name, req{method: "GET", path: loc, header: map[string]string{"X-HTTP-Method-Override": "DELETE"}}, 200},
		{ord.name, req{method: "OPTIONS", path: "/orders"}, 405},
		{ord.name, req{method: "GET", path: "/nowhere"}, 404},
		{e.name, req{method: "GET", path: "/echo/1?x=y", body: `{"get":"with body"}`}, 200},
	}
	for range 20 {
		for _, rd := range reads {
			got := a.via(g, rd.host, rd.r)
			if got.status != rd.want {
				t.Errorf("%s %s: got %s", rd.r.method, rd.r.path, got)
			}
		}
	}
	after := endOffsets(t)
	moved := 0
	for k, v := range after {
		if before[k] != v {
			moved++
			t.Errorf("topic %s moved from %d to %d during reads", k, before[k], v)
		}
	}
	t.Logf("%d reads of 9 shapes; %d topics checked (internal included), %d moved", 20*len(reads), len(after), moved)
	if d := a.direct(ord.url, req{method: "GET", path: "/orders"}); d.status == 200 && strings.Count(string(d.body), `"id"`) != 1 {
		t.Errorf("X-HTTP-Method-Override must not have deleted anything: %s", d)
	}
}
