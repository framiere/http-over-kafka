package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/events"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// genuine returns a signed Result record of a successful createOrder.
func (f *fixture) genuine(orderID string) (wire.Command, *kgo.Record) {
	f.t.Helper()
	cmd := createOrder(f.t, f.name, orderReq)
	return cmd, f.resultRecord(result(f.t, cmd, answered(cmd, 201, orderResp(orderID))))
}

// copyOf is a byte-identical copy of rec (value, key, headers), as anyone
// with write access to the result topic can produce it.
func copyOf(rec *kgo.Record) *kgo.Record {
	return &kgo.Record{Topic: rec.Topic, Key: rec.Key, Value: rec.Value, Headers: rec.Headers}
}

func (f *fixture) failureReasons() map[string]int {
	f.t.Helper()
	out := map[string]int{}
	for _, r := range drain(f.t, events.FailureTopic(f.name), kgo.ReadCommitted()) {
		var fl events.Failure
		if err := json.Unmarshal(r.Value, &fl); err != nil {
			f.t.Fatal(err)
		}
		out[fl.Reason]++
	}
	return out
}

func (f *fixture) run(instance string) {
	f.t.Helper()
	r := start(f.deriver(instance))
	f.caughtUp(time.Minute)
	r.stop(f.t)
}

// Copies of a genuine Result within the window: in the same batch, in a
// later batch, on another partition, under another key. One event in total.
func TestDedupReplayWithinWindow(t *testing.T) {
	f := newFixture(t)
	cmd, orig := f.genuine("ord_1")
	f.produce(orig, copyOf(orig)) // same batch: caught by the pending set
	f.run("d1")
	f.produce(copyOf(orig)) // later batch: caught by the committed set

	// Elsewhere than where the bridge puts it: would escape the owner.
	elsewhere := copyOf(orig)
	elsewhere.Partition = (orig.Partition + 1) % partitions
	manual := kafkatest.Client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err := manual.ProduceSync(context.Background(), elsewhere).FirstErr(); err != nil {
		t.Fatal(err)
	}
	rekeyed := copyOf(orig)
	rekeyed.Key = []byte("r:another")
	f.produce(rekeyed)
	f.run("d1")

	f.checkEvents(map[string]string{cmd.RequestID: "ord_1"})
	got := f.failureReasons()
	if got[events.ReasonDuplicateResult] != 2 || got[events.ReasonMisrouted] != 2 || len(got) != 2 {
		t.Fatalf("failures %v, want 2 duplicate_result and 2 misrouted", got)
	}
}

// A genuine Result older than the window: the deriver cannot prove it never
// emitted its event, so it emits none and says so.
func TestDedupBeyondWindow(t *testing.T) {
	f := newFixture(t)
	old := createOrder(t, f.name, orderReq)
	res, err := wire.NewResult(old, answered(old, 201, orderResp("ord_old")), time.Now().Add(-2*testWindow))
	if err != nil {
		t.Fatal(err)
	}
	f.produce(f.resultRecord(res))
	want := f.created(1)
	f.run("d1")

	f.checkEvents(want)
	if got := f.failureReasons(); got[events.ReasonBeyondDedupWindow] != 1 || len(got) != 1 {
		t.Fatalf("failures %v, want 1 beyond_dedup_window", got)
	}
}

// The dedup entry lives in the event's transaction: a crash between the
// original and its copies, then a rebalance and a change of owner, still
// leave exactly one event per requestId.
func TestDedupSurvivesCrashAndRebalance(t *testing.T) {
	f := newFixture(t)
	cmd1, o1 := f.genuine("ord_1")
	f.produce(o1, copyOf(o1))

	crashing := f.deriver("d1")
	events.SetBeforeCommit(crashing, func() error { return errCrash })
	r := start(crashing)
	select {
	case err := <-r.done:
		if !errors.Is(err, errCrash) {
			t.Fatalf("want simulated crash, got %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("deriver never reached a commit")
	}
	r.cancel()
	if len(drain(t, f.evTopic, kgo.ReadUncommitted())) == 0 {
		t.Fatal("the crash happened before any event was written; the test proves nothing")
	}
	f.run("d1") // same instance: fences the dead transaction, derives o1 once

	a, b := start(f.deriver("a")), start(f.deriver("b"))
	cmd2, o2 := f.genuine("ord_2")
	f.produce(copyOf(o1), o2, copyOf(o2))
	f.caughtUp(time.Minute)
	a.stop(t) // b now owns every partition and must reload the dedup set
	f.produce(copyOf(o1), copyOf(o2))
	f.caughtUp(time.Minute)
	b.stop(t)

	f.checkEvents(map[string]string{cmd1.RequestID: "ord_1", cmd2.RequestID: "ord_2"})
	if got := f.failureReasons(); got[events.ReasonDuplicateResult] != 5 || len(got) != 1 {
		t.Fatalf("failures %v, want 5 duplicate_result, one per copy", got)
	}
}

var errCrash = errors.New("simulated crash")
