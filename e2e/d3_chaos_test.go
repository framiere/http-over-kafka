//go:build e2e

package e2e

import (
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type verdict string

const (
	vOK         verdict = "201"
	vNotApplied verdict = "not-applied" // the system claims B did not run
	vUnknown    verdict = "unknown"     // explicit outcome_unknown
	vTimeout    verdict = "504"
	vTransport  verdict = "transport-error"
	vOther      verdict = "other"
)

func classify(r reply) verdict {
	switch {
	case r.err != nil:
		return vTransport
	case r.status == 201:
		return vOK
	case r.status == 504:
		return vTimeout
	}
	switch r.problemType() {
	case probUnknown:
		return vUnknown
	case probTransport, probUpstream, probStale:
		return vNotApplied
	}
	return vOther
}

type job struct {
	account  string
	keyed    bool
	final    reply
	verdict  verdict
	attempts int
	claims   []verdict // every answer received, in order
}

// The VP's question, answered by an oracle instead of a scenario: while
// bridges and gateways are killed -9, stopped gracefully and Kafka freezes,
// clients charge distinct accounts. Afterwards every answer a client got is
// checked against the payments ground truth.
//
//   - with an Idempotency-Key: at most one debit per key, whatever happened
//   - any answer "201" must match exactly one debit
//   - any answer "not applied" must match zero debits
//
// E2E_CHAOS_SECONDS (default 60) sets the chaos window.
func TestD3_ChaosOracle(t *testing.T) {
	window := 60 * time.Second
	if v, err := strconv.Atoi(os.Getenv("E2E_CHAOS_SECONDS")); err == nil {
		window = time.Duration(v) * time.Second
	}
	spec := t.TempDir()
	pay := startPayments(t, spec, 30*time.Millisecond)
	gws := []*gw{newGateway(t, spec, 3*time.Second, pay).startReady(t), newGateway(t, spec, 3*time.Second, pay).startReady(t)}
	b1, b2 := newBridge(t, spec, pay, "b1"), newBridge(t, spec, pay, "b2")
	startBridgeReady(t, b1)
	b2.start()
	bridges := []*proc{b1, b2}
	a := newCaller(t, "checkout")

	var (
		mu       sync.Mutex
		jobs     []*job
		stopping atomic.Bool
		chaosLog []string
		wg       sync.WaitGroup
		seq      atomic.Int64
	)
	worker := func(w int) {
		defer wg.Done()
		for !stopping.Load() {
			n := seq.Add(1)
			j := &job{account: fmt.Sprintf("acct-%d", n), keyed: n%3 != 0}
			r := req{method: "POST", path: "/charges", body: charge(j.account)}
			if j.keyed {
				r.header = map[string]string{"Idempotency-Key": "key-" + j.account}
			}
			giveUp := time.Now().Add(window + 3*time.Minute)
			for {
				j.attempts++
				got := a.via(gws[(w+j.attempts)%2], pay.name, r)
				v := classify(got)
				j.claims = append(j.claims, v)
				j.final, j.verdict = got, v
				retry := j.keyed && (v == vTimeout || v == vTransport || v == vNotApplied || got.status == 409)
				if !retry || time.Now().After(giveUp) {
					break
				}
				time.Sleep(200 * time.Millisecond) // client backoff, part of the client model
			}
			mu.Lock()
			jobs = append(jobs, j)
			mu.Unlock()
		}
	}
	for w := range 12 {
		wg.Add(1)
		go worker(w)
	}

	chaosEnd := time.Now().Add(window)
	logf := func(f string, args ...any) {
		mu.Lock()
		chaosLog = append(chaosLog, time.Now().Format("15:04:05.000")+" "+fmt.Sprintf(f, args...))
		mu.Unlock()
	}
	for time.Now().Before(chaosEnd) {
		time.Sleep(time.Duration(1500+rand.IntN(2500)) * time.Millisecond) // chaos pacing
		switch rand.IntN(6) {
		case 0, 1:
			b := bridges[rand.IntN(2)]
			b.kill()
			b.start()
			logf("kill -9 + restart %s", b.name)
		case 2:
			g := gws[rand.IntN(2)]
			g.kill()
			g.start()
			logf("kill -9 + restart %s", g.instance)
		case 3:
			pauseKafka(t)
			time.Sleep(time.Duration(1000+rand.IntN(4000)) * time.Millisecond)
			unpauseKafka(t)
			logf("kafka frozen then thawed")
		case 4:
			b := bridges[rand.IntN(2)]
			b.term(30 * time.Second)
			b.start()
			logf("SIGTERM + restart %s", b.name)
		case 5:
			b := bridges[rand.IntN(2)]
			b.signal(syscall.SIGSTOP)
			time.Sleep(time.Duration(2000+rand.IntN(3000)) * time.Millisecond)
			b.signal(syscall.SIGCONT)
			logf("SIGSTOP/SIGCONT %s", b.name)
		}
	}
	stopping.Store(true)
	logf("chaos over, waiting for clients")
	wg.Wait()

	// Settle: no new debit and no new Result for 15s.
	total := func() int {
		n := 0
		for _, j := range jobs {
			n += debits(t, pay, j.account)
		}
		return n
	}
	var last int
	quietSince := time.Now()
	eventually(t, 3*time.Minute, "ground truth settled", func() bool {
		if n := total(); n != last {
			last, quietSince = n, time.Now()
		}
		return time.Since(quietSince) > 15*time.Second
	})

	for _, l := range chaosLog {
		t.Log(l)
	}
	var violations []string
	stats := map[string]int{}
	for _, j := range jobs {
		d := debits(t, pay, j.account)
		kind := "unkeyed"
		if j.keyed {
			kind = "keyed"
		}
		stats[fmt.Sprintf("%s final=%s debits=%d", kind, j.verdict, d)]++
		switch {
		case d > 1:
			violations = append(violations, fmt.Sprintf("DOUBLE DEBIT %s (%s, claims %v): %d debits", j.account, kind, j.claims, d))
		case j.verdict == vOK && d != 1:
			violations = append(violations, fmt.Sprintf("201 without debit %s (%s, claims %v)", j.account, kind, j.claims))
		case j.verdict == vNotApplied && d != 0:
			violations = append(violations, fmt.Sprintf("told not-applied but debited %s (%s, claims %v): %s", j.account, kind, j.claims, j.final))
		}
		// Any intermediate "not applied" followed by a debit is fine only
		// for keyed jobs that retried; for unkeyed there is one claim.
	}
	for k, v := range stats {
		t.Logf("%5d  %s", v, k)
	}
	rs := results(t, pay)
	perReq := map[string]int{}
	for _, r := range rs {
		perReq[r.Command.RequestID]++
	}
	for id, n := range perReq {
		if n > 1 {
			violations = append(violations, fmt.Sprintf("requestId %s has %d Results", id, n))
		}
	}
	t.Logf("%d jobs, %d debits, %d Results", len(jobs), total(), len(rs))
	for _, v := range violations {
		t.Error(v)
	}
}
