package gateway_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/demo/orders"
	"github.com/sderosiaux/http-over-kafka/internal/gateway"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
)

// TestLatencyBreakdown splits the gateway's added latency with the protocol
// peer standing in for the bridge (no dedup store, so it is a lower bound of
// the full system; cmd/latency measures the real stack). Opt-in:
//
//	HOK_LATENCY=2000 [HOK_LATENCY_C=8] go test ./internal/gateway -run LatencyBreakdown -v
func TestLatencyBreakdown(t *testing.T) {
	n := 0
	if _, err := fmt.Sscan(os.Getenv("HOK_LATENCY"), &n); err != nil || n <= 0 {
		t.Skip("set HOK_LATENCY=<samples> to run")
	}
	name := kafkatest.Service(t, "orders")
	b := httptest.NewServer(orders.New().Handler())
	defer b.Close()
	var mu sync.Mutex
	var produce, total []time.Duration
	g := startGateway(t, gwOpts{onTiming: func(tm gateway.Timing) {
		mu.Lock()
		produce, total = append(produce, tm.Produce), append(total, tm.Total)
		mu.Unlock()
	}}, loadService(t, name, api.Orders, b.URL))
	g.waitReady(t)
	startPeer(t, name, b.URL, nil)

	body := `{"customerId":"c1","items":[{"sku":"A","quantity":2}]}`
	tok := token(t)
	measure := func(url, host string) time.Duration {
		req, _ := http.NewRequest("POST", url+"/orders", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if host != "" {
			req.Host = host
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		start := time.Now()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err) // Errorf: called from worker goroutines
			return 0
		}
		resp.Body.Close()
		if resp.StatusCode != 201 {
			t.Errorf("status %d", resp.StatusCode)
		}
		return time.Since(start)
	}
	for range 100 {
		measure(b.URL, "")
		measure(g.srv.URL, name)
	}
	mu.Lock()
	produce, total = nil, nil
	mu.Unlock()
	workers := 1
	fmt.Sscan(os.Getenv("HOK_LATENCY_C"), &workers)
	var direct, via []time.Duration
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range n / workers {
				d, v := measure(b.URL, ""), measure(g.srv.URL, name)
				mu.Lock()
				direct, via = append(direct, d), append(via, v)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	t.Logf("%d samples, %d workers", len(via), workers)
	report := func(label string, s []time.Duration) {
		slices.Sort(s)
		q := func(p float64) time.Duration {
			return s[min(len(s)-1, int(float64(len(s))*p))].Round(10 * time.Microsecond)
		}
		t.Logf("%-28s p50 %8s  p95 %8s  p99 %8s", label, q(.5), q(.95), q(.99))
	}
	report("direct A->B", direct)
	report("via gateway A->B", via)
	report("gateway: until command acked", produce)
	report("gateway: until response", total)
}
