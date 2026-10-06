package events_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/events"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The broker's group.min.session.timeout.ms: a member that dies without
// leaving is evicted after this, not after franz-go's 45s default.
const shortSession = 6 * time.Second

// A batch's transaction is held open while a second instance joins and takes
// partitions away. The held End must abort (revocation), the first instance
// must rewind to the committed offsets for the partitions it keeps, and the
// committed stream must still hold each event exactly once.
func TestDeriverRebalanceAbortsHeldTransaction(t *testing.T) {
	f := newFixture(t)
	want := f.created(30)

	a := f.deriver("a")
	inBatch, release := make(chan struct{}), make(chan struct{})
	var hold sync.Once
	events.SetBeforeCommit(a, func() error {
		held := false
		hold.Do(func() { held = true; close(inBatch) })
		if held {
			<-release
		}
		return nil
	})
	var mu sync.Mutex
	var ends []bool
	events.SetOnEnd(a, func(committed bool) {
		mu.Lock()
		defer mu.Unlock()
		ends = append(ends, committed)
	})
	ra := start(a)
	select {
	case <-inBatch:
	case <-time.After(time.Minute):
		t.Fatal("instance a never reached a commit")
	}
	held := drain(t, f.evTopic, kgo.ReadUncommitted())
	if len(held) == 0 {
		t.Fatal("the held transaction wrote nothing; the test proves nothing")
	}

	rb := start(f.deriver("b"))
	// Both members hold partitions: a has already been through revocation.
	adm := kafkatest.Admin(t)
	kafkatest.Eventually(t, time.Minute, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		gs, err := adm.DescribeGroups(ctx, events.GroupID(f.name))
		if err != nil {
			return false
		}
		g := gs[events.GroupID(f.name)]
		if g.State != "Stable" || len(g.Members) != 2 {
			return false
		}
		for _, m := range g.Members {
			c, ok := m.Assigned.AsConsumer()
			if !ok || len(c.Topics) == 0 || len(c.Topics[0].Partitions) == 0 {
				return false
			}
		}
		return true
	}, "instance b never got partitions")
	close(release)

	kafkatest.Eventually(t, time.Minute, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(ends) > 0
	}, "held transaction never ended")
	mu.Lock()
	first := ends[0]
	mu.Unlock()
	if first {
		t.Fatal("the transaction held across the rebalance committed; it must abort")
	}

	f.caughtUp(time.Minute)
	ra.stop(t)
	rb.stop(t)
	f.checkEvents(want)
	t.Logf("held transaction wrote %d events, aborted; transaction ends seen by a: %v", len(held), ends)
}

// The deriver runs in a child process that is SIGKILLed while its
// transaction is open: no abort, no LeaveGroup, nothing graceful. A new
// process with the same instance must fence it and re-derive the batch.
//
// Recovery bounds, from the kill to every event readable in read_committed:
//   - same instance: it fences the dead transaction at once, then waits for
//     the dead member's eviction (one session timeout);
//   - another instance: eviction, then the dead transaction must time out
//     (transaction timeout = session timeout) and be aborted by the broker's
//     sweep, which runs every 10s (transaction.abort.timed.out.transaction.
//     cleanup.interval.ms) before read_committed readers move on.
func TestDeriverKilledMidBatch(t *testing.T) {
	const brokerAbortSweep = 10 * time.Second
	cases := []struct {
		successor string
		limit     time.Duration
	}{
		// Expected ~1 session (d1) and ~2 sessions + sweep (d2); the limits
		// leave room for a loaded machine yet stay under franz-go's 45s
		// default session, the regression this guards against.
		{"d1", 4 * shortSession},
		{"d2", 4*shortSession + brokerAbortSweep},
	}
	for _, c := range cases {
		t.Run("successor "+c.successor, func(t *testing.T) { killedMidBatch(t, c.successor, c.limit) })
	}
}

func killedMidBatch(t *testing.T, successor string, limit time.Duration) {
	f := newFixture(t)
	want := f.created(12)
	f.failed(4)

	child := exec.Command(os.Args[0], "-test.run=^$")
	child.Env = append(os.Environ(),
		childEnv+"=1",
		"HOK_TEST_CHILD_SERVICE="+f.name,
		"KAFKA_BROKERS="+strings.Join(kafkatest.Brokers(t), ","),
		identity.TrustedKeysEnv(identity.RoleBridge)+"="+f.trusted,
	)
	var logs bytes.Buffer
	child.Stderr = &logs
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan bool, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if sc.Text() == childReady {
				ready <- true
				return
			}
		}
		ready <- false
	}()
	select {
	case ok := <-ready:
		if !ok {
			_ = child.Wait()
			t.Fatalf("child exited before its transaction was open:\n%s", logs.String())
		}
	case <-time.After(time.Minute):
		_ = child.Process.Kill()
		t.Fatalf("child never opened a transaction:\n%s", logs.String())
	}
	if err := child.Process.Kill(); err != nil { // SIGKILL
		t.Fatal(err)
	}
	_ = child.Wait()
	killed := time.Now()

	dirty := drain(t, f.evTopic, kgo.ReadUncommitted())
	if len(dirty) == 0 {
		t.Fatal("the child died before writing any event; the test proves nothing")
	}

	r := start(f.deriver(successor))
	f.caughtUp(time.Minute)
	f.checkEvents(want)
	recovery := time.Since(killed)
	r.stop(t)
	t.Logf("killed child had written %d uncommitted events; all %d events committed %s after the kill",
		len(dirty), len(want), recovery.Round(100*time.Millisecond))
	if recovery > limit {
		t.Errorf("recovery took %s, want under %s", recovery, limit)
	}
}

const (
	childEnv   = "HOK_TEST_DERIVER_CHILD"
	childReady = "transaction-open"
)

// childMain runs a deriver (instance d1) that announces its first open,
// flushed transaction on stdout and then blocks until killed.
func childMain() {
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, "child:", err)
		os.Exit(2)
	}
	name := os.Getenv("HOK_TEST_CHILD_SERVICE")
	svc, err := apispec.Load(name, ordersSpec(name))
	if err != nil {
		fail(err)
	}
	keys, err := identity.TrustedKeysFromEnv(identity.RoleBridge)
	if err != nil {
		fail(err)
	}
	d, err := events.New(events.Config{
		BridgeKeys:     keys,
		Brokers:        strings.Split(os.Getenv("KAFKA_BROKERS"), ","),
		Service:        svc,
		Instance:       "d1",
		SessionTimeout: shortSession,
		DedupWindow:    testWindow,
	})
	if err != nil {
		fail(err)
	}
	events.SetBeforeCommit(d, func() error {
		fmt.Println(childReady)
		select {}
	})
	fail(fmt.Errorf("deriver returned: %v", d.Run(context.Background())))
}
