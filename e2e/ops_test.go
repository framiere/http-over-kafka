//go:build e2e

package e2e

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// The gateway's 503 transport_unavailable says "the operation was not
// applied; it is safe to retry". A caller that believes it retries without a
// key. Freeze or crash the broker around the produce and check the claim
// against the ground truth.
func TestOps_TransportUnavailableMeansNotApplied(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 2*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	type shot struct {
		account string
		got     reply
	}
	var (
		mu    sync.Mutex
		shots []shot
	)
	fire := func(n int, spacing time.Duration) *sync.WaitGroup {
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(i) * spacing)
				acct := randName("acct-")
				got := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(acct)})
				mu.Lock()
				shots = append(shots, shot{acct, got})
				mu.Unlock()
			}()
		}
		return &wg
	}
	for round := range 4 {
		// Freeze: connections stay open, nothing answers.
		pauseKafka(t)
		wg := fire(6, 400*time.Millisecond)
		wg.Wait()
		unpauseKafka(t)
		// Crash: the broker process dies and comes back; connections reset.
		wg = fire(6, 150*time.Millisecond)
		docker(t, "kill", "-s", "KILL", kafkaContainer)
		wg.Wait()
		docker(t, "start", kafkaContainer)
		eventually(t, 90*time.Second, "service recovered after the broker restart (warm-up charge 201)", func() bool {
			if !b.alive() || !g.alive() {
				t.Fatalf("a process exited after the broker restart (bridge alive=%v, gateway alive=%v); bridge: %s", b.alive(), g.alive(), lastLine(b.logs()))
			}
			r := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(fmt.Sprintf("warm-%d", round))})
			return r.status == 201
		})
	}
	// Settle and judge.
	stable(t, 10*time.Second, "settling", func() bool { return true })
	counts := map[string]int{}
	for _, s := range shots {
		d := debits(t, pay, s.account)
		v := classify(s.got)
		counts[fmt.Sprintf("%s debits=%d", v, d)]++
		if v == vNotApplied && d != 0 {
			t.Errorf("told %q (not applied, safe to retry) but B debited %d: %s", s.got.problemType(), d, s.account)
		}
		if v == vOK && d != 1 {
			t.Errorf("201 but %d debits", d)
		}
		if d > 1 {
			t.Errorf("double debit %s", s.account)
		}
	}
	for k, v := range counts {
		t.Logf("%3d  %s", v, k)
	}
}

// What a crashed bridge costs in availability: with two bridges, kill -9 one
// and measure, per second, how many requests succeed. Requests are unkeyed and
// spread over all partitions.
func TestOps_BridgeCrashAvailability(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 2*time.Second, pay).startReady(t)
	b1, b2 := newBridge(t, spec, pay, "b1"), newBridge(t, spec, pay, "b2")
	startBridgeReady(t, b1)
	m := b2.mark()
	b2.start()
	eventually(t, 60*time.Second, "b2 owns partitions", func() bool { return b2.countSince(m, `"msg":"partition ready"`) >= 1 })
	eventually(t, 30*time.Second, "b1 settled after handover", func() bool { return b1.countSince(0, "partitions released") >= 1 })
	a := newCaller(t, "checkout")

	type sample struct {
		at time.Duration
		ok bool
	}
	var mu sync.Mutex
	var samples []sample
	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				r := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(randName("acct-"))})
				mu.Lock()
				samples = append(samples, sample{time.Since(start), r.status == 201})
				mu.Unlock()
			}
		}()
	}
	time.Sleep(5 * time.Second) // baseline window
	b2.kill()
	killedAt := time.Since(start)
	b2.start()                   // restarted immediately, as an orchestrator would
	time.Sleep(70 * time.Second) // observation window
	close(stop)
	wg.Wait()

	buckets := map[int][2]int{}
	for _, s := range samples {
		k := int(s.at / (5 * time.Second))
		v := buckets[k]
		if s.ok {
			v[0]++
		} else {
			v[1]++
		}
		buckets[k] = v
	}
	var firstFull time.Duration = -1
	for k := 0; k <= int(samples[len(samples)-1].at/(5*time.Second)); k++ {
		v := buckets[k]
		t.Logf("t=%3ds..%3ds  ok=%4d failed=%4d", k*5, k*5+5, v[0], v[1])
		if time.Duration(k)*5*time.Second > killedAt && v[1] == 0 && firstFull < 0 {
			firstFull = time.Duration(k) * 5 * time.Second
		}
	}
	t.Logf("bridge b2 killed at %s; first 5s window with zero failures starts at %s", killedAt.Round(time.Millisecond), firstFull)
}

// SIGTERM of a bridge is a graceful handover. How long does it take, alone
// and when a peer has just died?
func TestOps_BridgeGracefulStopTime(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	newGateway(t, spec, 2*time.Second, pay).startReady(t)
	b1, b2 := newBridge(t, spec, pay, "b1"), newBridge(t, spec, pay, "b2")
	startBridgeReady(t, b1)
	m := b2.mark()
	b2.start()
	eventually(t, 60*time.Second, "b2 owns partitions", func() bool { return b2.countSince(m, `"msg":"partition ready"`) >= 1 })

	t0 := time.Now()
	b2.term(120 * time.Second)
	t.Logf("SIGTERM with a healthy peer: stopped in %s", time.Since(t0).Round(time.Millisecond))
	m = b2.mark()
	b2.start()
	eventually(t, 90*time.Second, "b2 owns partitions again", func() bool { return b2.countSince(m, `"msg":"partition ready"`) >= 1 })

	b2.kill()
	t0 = time.Now()
	b1.term(120 * time.Second)
	t.Logf("SIGTERM right after the peer was killed -9: stopped in %s", time.Since(t0).Round(time.Millisecond))
}

// Same, with traffic flowing, and after a broker freeze: the chaos run saw a
// SIGTERM'd bridge not stop within 30s.
func TestOps_BridgeGracefulStopUnderLoad(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 2*time.Second, pay).startReady(t)
	b1, b2 := newBridge(t, spec, pay, "b1"), newBridge(t, spec, pay, "b2")
	startBridgeReady(t, b1)
	m := b2.mark()
	b2.start()
	eventually(t, 60*time.Second, "b2 owns partitions", func() bool { return b2.countSince(m, `"msg":"partition ready"`) >= 1 })
	a := newCaller(t, "checkout")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(randName("acct-"))})
			}
		}()
	}
	defer func() { close(stop); wg.Wait() }()
	ready := func(p *proc) {
		m := p.mark()
		p.start()
		eventually(t, 120*time.Second, p.name+" owns partitions", func() bool { return p.countSince(m, `"msg":"partition ready"`) >= 1 })
	}
	time.Sleep(3 * time.Second) // traffic warms up

	t0 := time.Now()
	b2.term(120 * time.Second)
	t.Logf("under load, healthy peer: SIGTERM stopped in %s", time.Since(t0).Round(time.Millisecond))
	ready(b2)

	b2.kill()
	t0 = time.Now()
	b1.term(120 * time.Second)
	t.Logf("under load, peer just killed -9: SIGTERM stopped in %s", time.Since(t0).Round(time.Millisecond))
	// Back to two healthy members.
	m1, m2 := b1.mark(), b2.mark()
	b1.start()
	b2.start()
	eventually(t, 120*time.Second, "both own partitions", func() bool {
		return b1.countSince(m1, `"msg":"partition ready"`) >= 1 && b2.countSince(m2, `"msg":"partition ready"`) >= 1
	})

	pauseKafka(t)
	time.Sleep(3 * time.Second)
	unpauseKafka(t)
	t0 = time.Now()
	b1.term(120 * time.Second)
	t.Logf("under load, right after a 3s broker freeze: SIGTERM stopped in %s", time.Since(t0).Round(time.Millisecond))
	ready(b1)

	// A peer crashes and its replacement joins at once (what an orchestrator
	// does): the group now waits for the dead member's session to expire.
	b2.kill()
	b2.start()
	time.Sleep(time.Second) // the replacement's JoinGroup reaches the coordinator
	t0 = time.Now()
	b1.term(120 * time.Second)
	t.Logf("under load, while the group waits on a dead member: SIGTERM stopped in %s", time.Since(t0).Round(time.Millisecond))
}
