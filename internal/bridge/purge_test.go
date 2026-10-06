package bridge

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPurgeDrainsBacklogWithoutAdvancingClock(t *testing.T) {
	now := time.Now()
	w := &partition{b: &Bridge{cfg: Config{Now: func() time.Time { return now }}},
		st: store{entries: map[string]*entry{}}, lanes: map[string]*lane{}}
	// More than the old per-minute quota enters in every cycle. The clock
	// does not advance: catching up must not require a fresh interval.
	for cycle := range 5 {
		for i := range 3*purgeBatchSize + 1 {
			w.st.entries[fmt.Sprintf("r:%d/%d", cycle, i)] = &entry{Phase: done, PurgeAfter: now.Add(-time.Second)}
		}
		w.startPurge()
		for chunks := 0; w.purgeNext != nil; chunks++ {
			if chunks > 4 {
				t.Fatal("sweep did not finish in bounded chunks")
			}
			before := len(w.st.entries)
			keys := w.expired(nil)
			if len(keys) > purgeBatchSize || len(w.st.entries) != before {
				t.Fatalf("selection changed state or exceeded batch bound: %d keys", len(keys))
			}
			for _, key := range keys { // simulate a successful commit
				delete(w.st.entries, key)
			}
		}
		if len(w.st.entries) != 0 {
			t.Fatalf("cycle %d left %d expired entries", cycle, len(w.st.entries))
		}
	}
}

func TestPurgeBoundsScansAndReconsidersProtectedEntries(t *testing.T) {
	now := time.Now()
	w := &partition{b: &Bridge{cfg: Config{Now: func() time.Time { return now }}},
		st: store{entries: map[string]*entry{}}, lanes: map[string]*lane{}}
	for i := range 2 * purgeBatchSize {
		w.st.entries[fmt.Sprint(i)] = &entry{Phase: done, PurgeAfter: now.Add(time.Hour)}
	}
	w.startPurge()
	if keys := w.expired(nil); len(keys) != 0 || w.purgeNext == nil {
		t.Fatalf("one scan must yield with unexamined live entries, keys=%v", keys)
	}
	w.stopPurge()
	w.st.entries = map[string]*entry{
		"r:busy":      {Phase: done, PurgeAfter: now.Add(-time.Second)},
		"r:rewritten": {Phase: done, PurgeAfter: now.Add(-time.Second)},
		"r:started":   {Phase: started, PurgeAfter: now.Add(-time.Second)},
		"r:boundary":  {Phase: done, PurgeAfter: now},
		keyOffset:     {Phase: done, PurgeAfter: now.Add(-time.Second)},
	}
	w.lanes["r:busy"] = &lane{}
	w.startPurge()
	if keys := w.expired(map[string]*entry{"r:rewritten": {Phase: started}}); len(keys) != 0 {
		t.Fatalf("purged protected entries: %v", keys)
	}
	delete(w.lanes, "r:busy")
	w.startPurge()
	keys := w.expired(nil)
	if len(keys) != 2 {
		t.Fatalf("next sweep did not reconsider skipped keys: %v", keys)
	}
	for _, key := range keys {
		if key != "r:busy" && key != "r:rewritten" {
			t.Fatalf("purged protected key %q", key)
		}
	}
}

func TestPurgeScanOnlyChunksNeedNoKafkaTransaction(t *testing.T) {
	now := time.Now()
	w := &partition{b: &Bridge{cfg: Config{Now: func() time.Time { return now }, TxnTimeout: time.Second}},
		st: store{entries: map[string]*entry{}}, maxSeen: -1}
	for i := range 2 * purgeBatchSize {
		w.st.entries[fmt.Sprint(i)] = &entry{Phase: done, PurgeAfter: now.Add(time.Hour)}
	}
	// No producer is installed. A scan that cannot expire an entry must
	// finish without touching Kafka, however many chunks the store needs.
	w.startPurge()
	for w.purgeNext != nil {
		if err := w.commitBatch(nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.st.entries) != 2*purgeBatchSize {
		t.Fatal("scan changed live entries")
	}
}

// The real committer must drain several batches from restored state even
// with no incoming commands, and make their signed tombstones durable.
func TestPurgeIdlePartitionDrainsDurably(t *testing.T) {
	w := purgePartition(t, 3*purgeBatchSize+1)
	ctx, cancel := context.WithCancel(t.Context())
	go w.commitLoop(ctx)
	t.Cleanup(func() { cancel(); <-w.cgone })
	kafkatest.Eventually(t, 30*time.Second, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.gen > 0 && len(w.st.entries) == 0
	}, "idle partition drains every expired entry")
	cancel()
	<-w.cgone
	if !w.reopen(t.Context()) {
		t.Fatal("could not restore purged state")
	}
	if len(w.st.entries) != 0 {
		t.Fatalf("tombstones were not durable: restored %d entries", len(w.st.entries))
	}
}

func TestPurgeAllowsCommandCommitBeforeSweepFinishes(t *testing.T) {
	w := purgePartition(t, 3*purgeBatchSize+1)
	// The request is waiting when the committer starts its restored sweep.
	// Whether the timer or the request wins the select, it must be included
	// in the first transaction, while expired entries still remain.
	remaining := make(chan int, 1)
	w.b.cfg.Hooks.BeforeStartedCommit = func(string) { remaining <- len(w.st.entries) }
	r := &commitReq{gen: 1, offset: -1, writes: map[string]*entry{
		"r:new": {Phase: started, PurgeAfter: w.b.now().Add(time.Hour)},
	}, err: make(chan error, 1)}
	w.reqs <- r
	ctx, cancel := context.WithCancel(t.Context())
	go w.commitLoop(ctx)
	t.Cleanup(func() { cancel(); <-w.cgone })
	select {
	case err := <-r.err:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("command commit stalled behind purge")
	}
	if n := <-remaining; n <= purgeBatchSize {
		t.Fatalf("command only reached commit after sweep drained: %d remain", n)
	}
	kafkatest.Eventually(t, 30*time.Second, func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.st.entries) == 1 && w.st.entries["r:new"] != nil
	}, "purge catches up without deleting new started marker")
}

// Fence the producer after its tombstones are produced but before commit.
// Neither the old process nor the new owner may forget the aborted entries.
func TestPurgeFailedTransactionPreservesState(t *testing.T) {
	w := purgePartition(t, purgeBatchSize+1)
	if !w.reopen(t.Context()) {
		t.Fatal("could not open partition")
	}
	successor := newPartition(w.b, w.cons, w.id)
	t.Cleanup(func() { successor.end(); successor.closeProducer() })
	w.b.cfg.Hooks.BeforeStartedCommit = func(string) {
		st, err := successor.open() // aborts and fences w's in-flight transaction
		if err != nil {
			t.Fatal(err)
		}
		if len(st.entries) != purgeBatchSize+1 {
			t.Fatalf("aborted tombstones affected restored state: %d entries", len(st.entries))
		}
	}
	w.startPurge()
	err := w.commitBatch([]*commitReq{{offset: -1, writes: map[string]*entry{
		"r:new": {Phase: started, PurgeAfter: w.b.now().Add(time.Hour)},
	}}})
	if err == nil {
		t.Fatal("fenced purge transaction unexpectedly committed")
	}
	if len(w.st.entries) != purgeBatchSize+1 || w.st.entries["r:new"] != nil {
		t.Fatalf("failed transaction changed memory: %d entries", len(w.st.entries))
	}
	w.b.cfg.Hooks = Hooks{}
	if !w.reopen(t.Context()) {
		t.Fatal("could not reopen after aborted purge")
	}
	if w.purgeNext != nil {
		t.Fatal("restoration retained a cursor into the old generation")
	}
	w.startPurge()
	for w.purgeNext != nil {
		if err := w.commitBatch(nil); err != nil {
			t.Fatal(err)
		}
	}
	if !w.reopen(t.Context()) || len(w.st.entries) != 0 {
		t.Fatal("purge did not retry every aborted tombstone durably")
	}
}

// Seed signed, expired state on a real Kafka partition. A one-hour interval
// ensures these tests cannot pass by waiting for another scheduled sweep.
func purgePartition(t *testing.T, count int) *partition {
	t.Helper()
	brokers := kafkatest.Brokers(t)
	service := kafkatest.Service(t, "purge")
	spec, err := apispec.Load(service, api.Payments)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := identity.NewSigner(identity.RoleGateway, "gw", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	signer, err := identity.NewSigner(identity.RoleBridge, "bridge", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{Service: service, Brokers: brokers, Spec: spec, Keys: gw.Self(), Signer: signer,
		Upstream: "http://127.0.0.1:1", PurgeInterval: time.Hour,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	b.base = kafkatest.Client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	b.admin = kadm.NewClient(b.base)
	_, b.abandon = context.WithCancelCause(t.Context())
	kafkatest.CreateTopics(t, kafkaenv.CommandTopic(service, 1))
	kafkatest.Eventually(t, 30*time.Second, func() bool { return b.prepareTopics(t.Context()) == nil }, "prepare purge topics")
	w := newPartition(b, b.base, 0)
	t.Cleanup(func() { w.stopPurge(); w.end(); w.closeProducer() })
	// Bootstrap a real writer and inventory without advancing the worker
	// generation: commitLoop's first reopen must still be generation one.
	w.st, err = w.open()
	if err != nil {
		t.Fatal(err)
	}
	recs := make([]*kgo.Record, 0, count)
	for i := range count {
		r, err := stateRecord(b.stateTopic, 0, fmt.Sprintf("r:expired-%d", i),
			&entry{Phase: done, PurgeAfter: b.now().Add(-time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	commitPurgeFixture(t, w, recs, count)
	return w
}

// Fixtures use the same physical-offset signatures and transactional inventory
// as a legitimate state owner. The caller restores before inspecting memory.
func commitPurgeFixture(t *testing.T, w *partition, recs []*kgo.Record, entries int) {
	t.Helper()
	seal, err := stateRecord(w.b.stateTopic, w.id, keySeal, sealValue{entries, w.st.next, w.st.genesis})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.prod.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := w.produceState(t.Context(), append(recs, seal)); err != nil {
		w.abort()
		t.Fatal(err)
	}
	if err := w.prod.EndTransaction(t.Context(), kgo.TryCommit); err != nil {
		w.abort()
		t.Fatal(err)
	}
}
