// Command latency measures what the gateway costs A: the same request sent
// directly to B and through the gateway, interleaved so both see the same
// machine load, reported as P50/P95/P99 (criterion 8).
//
//	go run ./cmd/latency -n 2000 -c 1
//
// Defaults target the compose stack (orders on :8081, gateway on :8080) and
// mint a dev token. Every sample must succeed with the expected status, or the
// run fails: a fast error is not a latency.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"
)

func main() {
	direct := flag.String("direct", "http://localhost:8081", "B's base URL")
	gw := flag.String("gateway", "http://localhost:8080", "gateway base URL")
	host := flag.String("host", "orders", "Host header for the gateway")
	method := flag.String("method", "POST", "method")
	path := flag.String("path", "/orders", "path")
	body := flag.String("body", `{"customerId":"c1","items":[{"sku":"A1","quantity":2}]}`, "request body")
	status := flag.Int("status", 201, "expected status")
	n := flag.Int("n", 1000, "samples per target")
	c := flag.Int("c", 1, "concurrent workers")
	warmup := flag.Int("warmup", 100, "unmeasured requests per target first")
	token := flag.String("token", os.Getenv("KB_TOKEN"), "bearer token (default: go run ./cmd/devtoken)")
	flag.Parse()

	if *token == "" {
		out, err := exec.Command("go", "run", "./cmd/devtoken", "-app", "latency").Output()
		if err != nil {
			log.Fatalf("minting token: %v", err)
		}
		*token = strings.TrimSpace(string(out))
	}
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: *c * 2}}
	send := func(base, hostHeader string) (time.Duration, error) {
		req, err := http.NewRequest(*method, base+*path, strings.NewReader(*body))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		if hostHeader != "" {
			req.Host = hostHeader
			req.Header.Set("Authorization", "Bearer "+*token)
		}
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		elapsed := time.Since(start)
		if err != nil {
			return 0, err
		}
		if resp.StatusCode != *status {
			return 0, fmt.Errorf("%s: status %d: %s", base, resp.StatusCode, bytes.TrimSpace(b))
		}
		return elapsed, nil
	}

	for range *warmup {
		must(send(*direct, ""))
		must(send(*gw, *host))
	}

	var mu sync.Mutex
	var directS, gwS []time.Duration
	var wg sync.WaitGroup
	per := *n / *c
	start := time.Now()
	for range *c {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				// Alternate which goes first so neither benefits from order.
				first, second := *direct, *gw
				if i%2 == 1 {
					first, second = second, first
				}
				for _, base := range []string{first, second} {
					hh := ""
					if base == *gw {
						hh = *host
					}
					d := must(send(base, hh))
					mu.Lock()
					if base == *gw {
						gwS = append(gwS, d)
					} else {
						directS = append(directS, d)
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	wall := time.Since(start)

	fmt.Printf("%s %s, %d samples per target, %d workers, %s wall\n", *method, *path, len(directS), *c, wall.Round(time.Millisecond))
	fmt.Printf("%-8s %9s %9s %9s %9s %9s\n", "", "p50", "p95", "p99", "max", "mean")
	d, g := stats(directS), stats(gwS)
	row("direct", d)
	row("gateway", g)
	row("added", [5]time.Duration{g[0] - d[0], g[1] - d[1], g[2] - d[2], g[3] - d[3], g[4] - d[4]})
}

func stats(s []time.Duration) [5]time.Duration {
	slices.Sort(s)
	q := func(p float64) time.Duration { return s[min(len(s)-1, int(float64(len(s))*p))] }
	var sum time.Duration
	for _, v := range s {
		sum += v
	}
	return [5]time.Duration{q(0.50), q(0.95), q(0.99), s[len(s)-1], sum / time.Duration(len(s))}
}

func row(name string, v [5]time.Duration) {
	fmt.Printf("%-8s", name)
	for _, d := range v {
		fmt.Printf(" %9s", d.Round(10*time.Microsecond))
	}
	fmt.Println()
}

func must(d time.Duration, err error) time.Duration {
	if err != nil {
		log.Fatal(err)
	}
	return d
}
