//go:build e2e

package e2e

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Criterion 8, measured independently of cmd/latency. Direct and gateway
// calls alternate within each worker, so both see the same machine load.
//
// Default target: a stack this test starts (orders, gateway, bridge as local
// processes, broker from e2e/compose.yaml). To measure another stack, e.g.
// the dev compose one:
//
//	E2E_LAT_DIRECT=http://localhost:8081 E2E_LAT_GATEWAY=http://localhost:8080 E2E_LAT_HOST=orders
//
// E2E_LAT_N (default 5000) samples per target and per run; warmup 500.
func TestLatency(t *testing.T) {
	n := 5000
	if v, err := strconv.Atoi(os.Getenv("E2E_LAT_N")); err == nil {
		n = v
	}
	direct, gateway, host := os.Getenv("E2E_LAT_DIRECT"), os.Getenv("E2E_LAT_GATEWAY"), os.Getenv("E2E_LAT_HOST")
	where := "external stack " + gateway
	if gateway == "" {
		spec := t.TempDir()
		ord := startOrders(t, spec)
		g := newGateway(t, spec, 10*time.Second, ord).startReady(t)
		startBridgeReady(t, newBridge(t, spec, ord, "b1"))
		direct, gateway, host = ord.url, g.url, ord.name
		where = "local processes (gateway, bridge, orders on the host; broker in Docker)"
	}
	a := newCaller(t, "latency")
	t.Logf("target: %s; %s/%s, GOMAXPROCS=%d", where, runtime.GOOS, runtime.GOARCH, runtime.GOMAXPROCS(0))

	body := `{"customerId":"c1","items":[{"sku":"A1","quantity":2}]}`
	run := func(c int, keyed bool) {
		dc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: c * 2}}
		gc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: c * 2}}
		var seq int64
		var mu sync.Mutex
		one := func(gw bool) time.Duration {
			r := req{method: "POST", path: "/orders", body: body}
			if keyed {
				mu.Lock()
				seq++
				r.header = map[string]string{"Idempotency-Key": fmt.Sprintf("lat-%d-%d", time.Now().UnixNano(), seq)}
				mu.Unlock()
			}
			cl := &caller{token: a.token, hc: dc}
			var got reply
			if gw {
				cl.hc = gc
				got = cl.do(gateway, host, true, r)
			} else {
				got = cl.do(direct, "", false, r)
			}
			if got.err != nil || got.status != 201 {
				t.Fatalf("sample failed (gateway=%v): %s", gw, got)
			}
			return got.elapsed
		}
		for range 500 {
			one(false)
			one(true)
		}
		var d, g []time.Duration
		var wg sync.WaitGroup
		start := time.Now()
		for range c {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range n / c {
					order := []bool{false, true}
					if i%2 == 1 {
						order = []bool{true, false}
					}
					for _, gw := range order {
						x := one(gw)
						mu.Lock()
						if gw {
							g = append(g, x)
						} else {
							d = append(d, x)
						}
						mu.Unlock()
					}
				}
			}()
		}
		wg.Wait()
		wall := time.Since(start)
		pd, pg := pct(d), pct(g)
		t.Logf("c=%d keyed=%v n=%d/target wall=%s gateway throughput=%.0f req/s", c, keyed, len(g), wall.Round(time.Millisecond), float64(len(g))/wall.Seconds())
		t.Logf("   direct  p50=%-8s p95=%-8s p99=%-8s max=%s", pd[0], pd[1], pd[2], pd[3])
		t.Logf("   gateway p50=%-8s p95=%-8s p99=%-8s max=%s", pg[0], pg[1], pg[2], pg[3])
		t.Logf("   added   p50=%-8s p95=%-8s p99=%-8s", pg[0]-pd[0], pg[1]-pd[1], pg[2]-pd[2])
	}
	for _, c := range []int{1, 8} {
		run(c, false)
		run(c, true)
	}
}

// pct returns nearest-rank p50, p95, p99 and max, rounded to 10µs.
func pct(s []time.Duration) [4]time.Duration {
	slices.Sort(s)
	q := func(p float64) time.Duration {
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return s[max(0, i)].Round(10 * time.Microsecond)
	}
	return [4]time.Duration{q(0.50), q(0.95), q(0.99), s[len(s)-1].Round(10 * time.Microsecond)}
}
