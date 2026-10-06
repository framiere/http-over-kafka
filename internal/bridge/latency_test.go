package bridge_test

import (
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/demo/orders"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

type percentiles []time.Duration

func (p percentiles) at(q float64) time.Duration {
	s := slices.Clone(p)
	slices.Sort(s)
	return s[min(len(s)-1, int(q*float64(len(s))))]
}

func (p percentiles) String() string {
	return fmt.Sprintf("p50=%-8s p95=%-8s p99=%-8s", p.at(.5).Round(10*time.Microsecond), p.at(.95).Round(10*time.Microsecond), p.at(.99).Round(10*time.Microsecond))
}

// TestLatency measures what the provider side adds: produce a signed command
// → bridge → B → Response read back (read_committed), sequentially, against
// the same B called directly. It is a measurement, not an assertion. N via
// HOK_LATENCY_N (default 300).
func TestLatency(t *testing.T) {
	n := 300
	if v, err := strconv.Atoi(os.Getenv("HOK_LATENCY_N")); err == nil && v > 0 {
		n = v
	}
	var mu sync.Mutex
	phases := map[string]map[string]percentiles{}
	observe := func(op string) func(bridge.Timing) {
		return func(tm bridge.Timing) {
			mu.Lock()
			defer mu.Unlock()
			if phases[op] == nil {
				phases[op] = map[string]percentiles{}
			}
			for k, v := range map[string]time.Duration{"started-commit": tm.Started, "upstream": tm.Upstream, "outcome-commit": tm.Complete, "bridge-total": tm.Total} {
				phases[op][k] = append(phases[op][k], v)
			}
		}
	}

	measure := func(e *env, mk func(i int) wire.Command) percentiles {
		var out percentiles
		for i := range n + 20 {
			cmd := mk(i)
			ch := e.waiter(cmd.RequestID)
			t0 := time.Now()
			e.send(cmd)
			select {
			case r := <-ch:
				if r.Status >= 300 {
					t.Fatalf("%+v", r)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("timeout")
			}
			if i >= 20 { // warm-up excluded
				out = append(out, time.Since(t0))
			}
		}
		return out
	}
	direct := func(method, url, body string) percentiles {
		var out percentiles
		for i := range n + 20 {
			req, _ := http.NewRequest(method, url, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			t0 := time.Now()
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if i >= 20 {
				out = append(out, time.Since(t0))
			}
		}
		return out
	}

	// POST /charges: not replayable, two transactions (started, outcome).
	_, pay := paymentsB(t)
	pe := newEnv(t, "lat", api.Payments, pay.URL)
	cfg := pe.config()
	cfg.Hooks.OnTiming = observe("POST /charges")
	pe.start(cfg)
	postDirect := direct(http.MethodPost, pay.URL+"/charges", `{"accountId":"lat","amountCents":1,"currency":"EUR"}`)
	postBridge := measure(pe, func(i int) wire.Command { return pe.charge("lat", fmt.Sprintf("lat-%d", i)) })

	// PUT /orders/{id}: replayable, one transaction.
	svc := orders.New()
	ordURL := httptestServer(t, svc.Handler())
	oe := newEnv(t, "lat", api.Orders, ordURL)
	o := createOrder(t, ordURL)
	cfg = oe.config()
	cfg.Hooks.OnTiming = observe("PUT /orders/{id}")
	oe.start(cfg)
	body := `{"customerId":"c1","items":[{"sku":"A","quantity":1}]}`
	putDirect := direct(http.MethodPut, ordURL+"/orders/"+o.ID, body)
	putBridge := measure(oe, func(int) wire.Command {
		return oe.command(http.MethodPut, "replaceOrder", "/orders/{orderId}", "/orders/"+o.ID, map[string]string{"orderId": o.ID}, body, "")
	})

	// Concurrency: HOK_LATENCY_C callers (default 8), distinct keys spread
	// over the partitions.
	callers := 8
	if v, err := strconv.Atoi(os.Getenv("HOK_LATENCY_C")); err == nil && v > 0 {
		callers = v
	}
	// Commands of one partition are processed one at a time.
	var cmu sync.Mutex
	var conc percentiles
	var wg sync.WaitGroup
	start := time.Now()
	for g := range callers {
		wg.Go(func() {
			for i := range n / callers {
				cmd := pe.charge("lat-c", fmt.Sprintf("lat-c-%d-%d", g, i))
				ch := pe.waiter(cmd.RequestID)
				t0 := time.Now()
				pe.send(cmd)
				select {
				case <-ch:
				case <-time.After(30 * time.Second):
					t.Error("timeout")
					return
				}
				cmu.Lock()
				conc = append(conc, time.Since(t0))
				cmu.Unlock()
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("n=%d sequential requests, 1 in flight, broker %s, B in-process (httptest)", n, os.Getenv("KAFKA_BROKERS"))
	t.Logf("POST /charges     direct      %s", postDirect)
	t.Logf("POST /charges     via bridge  %s  (produce → response consumed)", postBridge)
	t.Logf("POST /charges     via bridge, %d concurrent callers, %d partitions: %s  (%.0f req/s)", callers, partitions, conc, float64(len(conc))/elapsed.Seconds())
	t.Logf("PUT /orders/{id}  direct      %s", putDirect)
	t.Logf("PUT /orders/{id}  via bridge  %s", putBridge)
	mu.Lock()
	defer mu.Unlock()
	for _, op := range []string{"POST /charges", "PUT /orders/{id}"} {
		for _, k := range []string{"started-commit", "upstream", "outcome-commit", "bridge-total"} {
			t.Logf("%-17s bridge %-15s %s", op, k, phases[op][k])
		}
	}
}
