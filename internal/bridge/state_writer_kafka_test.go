package bridge

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Exercise the physical-offset writer in isolation. This does not authorize
// restoring a state polluted by another writer: open also verifies its signed
// records and inventory. Here the separate producer deliberately creates a gap.
func TestStateWriterChecksOffsetsAcrossTransactionsAndReopen(t *testing.T) {
	broker := kafkatest.Dedicated(t)
	topic := StateTopic(kafkatest.Service(t, "writer"))
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	other, err := kgo.NewClient(kafkaenv.ClientOpts(broker.Brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := kadm.NewClient(other).CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	// A successful baseline append also establishes that the new topic is
	// visible, independently of the provisioning readiness implementation.
	seed := &kgo.Record{Topic: topic, Key: []byte("seed"), Value: []byte("baseline")}
	if err := other.ProduceSync(ctx, seed).FirstErr(); err != nil {
		t.Fatal(err)
	}
	signer := stateTestSigner(t, "writer", 31)
	w := &partition{b: &Bridge{cfg: Config{Signer: signer}, stateTopic: topic}}
	defer w.closeProducer()
	reopen := func() int64 {
		t.Helper()
		w.closeProducer()
		prod, err := kgo.NewClient(kafkaenv.ClientOpts(broker.Brokers,
			kgo.TransactionalID("test-"+topic), kgo.TransactionTimeout(10*time.Second),
			kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ConcurrentTransactionsBackoff(2*time.Millisecond))...)
		if err != nil {
			t.Fatal(err)
		}
		w.prod = prod
		if _, _, err := prod.ProducerID(ctx); err != nil {
			t.Fatal(err)
		}
		end, err := w.stableStateEnd(ctx)
		if err != nil {
			t.Fatal(err)
		}
		w.nextStateOffset = end
		return end
	}
	newRecord := func(key string) *kgo.Record {
		t.Helper()
		r, err := stateRecord(topic, 0, key, &entry{RequestID: key, Phase: done})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	appendTransaction := func(key string, end kgo.TransactionEndTry) *kgo.Record {
		t.Helper()
		if err := w.prod.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		r := newRecord(key)
		if err := w.produceState(ctx, []*kgo.Record{r}); err != nil {
			t.Fatal(err)
		}
		if err := verifyStateRecord(signer.Self(), r); err != nil {
			t.Fatalf("record not signed for its actual offset: %v", err)
		}
		if err := w.prod.EndTransaction(ctx, end); err != nil {
			t.Fatal(err)
		}
		return r
	}
	reopen()
	want := map[string]bool{}
	for i := range 10 {
		key := fmt.Sprintf("i:committed-%d", i)
		appendTransaction(key, kgo.TryCommit)
		want[key] = true
	}
	// No metadata refresh occurs between these ten commits. The next
	// signatures therefore exercise the predicted transaction-marker offset.
	aborted := appendTransaction("i:aborted", kgo.TryAbort)
	predicted := w.nextStateOffset
	if end := reopen(); end != predicted || end <= aborted.Offset {
		t.Fatalf("abort marker not reflected at reopen: stable=%d predicted=%d aborted=%d", end, predicted, aborted.Offset)
	}

	// Race another producer against the offset already observed by the
	// writer. The checked append must refuse before the caller commits.
	predicted = w.nextStateOffset
	intruder := &kgo.Record{Topic: topic, Key: []byte("intruder"), Value: []byte("concurrent append")}
	if err := other.ProduceSync(ctx, intruder).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if intruder.Offset != predicted {
		t.Fatalf("interference not at the predicted offset: got=%d want=%d", intruder.Offset, predicted)
	}
	if err := w.prod.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	rejected := newRecord("i:rejected")
	err = w.produceState(ctx, []*kgo.Record{rejected})
	if err == nil || !strings.Contains(err.Error(), "state append position changed") {
		t.Fatalf("concurrent append was not refused: %v", err)
	}
	if rejected.Offset != predicted+1 || w.nextStateOffset != predicted {
		t.Fatalf("writer accepted or hid the offset gap: actual=%d prior=%d next=%d", rejected.Offset, predicted, w.nextStateOffset)
	}
	if err := w.prod.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if end := reopen(); end <= rejected.Offset {
		t.Fatalf("aborted refused write not passed on reopen: stable=%d rejected=%d", end, rejected.Offset)
	}
	appendTransaction("i:after-reopen", kgo.TryCommit)
	want["i:after-reopen"] = true
	end, err := w.stableStateEnd(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Read the whole committed stream through the final control marker.
	// Both physically appended, aborted records must remain invisible.
	reader, err := kgo.NewClient(kafkaenv.ClientOpts(broker.Brokers,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())...)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := map[string]bool{}
	for position := int64(0); position < end; {
		fs := reader.PollFetches(ctx)
		if err := fs.Err(); err != nil {
			t.Fatal(err)
		}
		fs.EachRecord(func(r *kgo.Record) {
			position = r.Offset + 1
			if r.Attrs.IsControl() || string(r.Key) == "seed" || string(r.Key) == "intruder" {
				return
			}
			key := string(r.Key)
			if !want[key] || seen[key] {
				t.Fatalf("unexpected or duplicate committed state record %q", key)
			}
			if err := verifyStateRecord(signer.Self(), r); err != nil {
				t.Fatal(err)
			}
			seen[key] = true
		})
	}
	if len(seen) != len(want) {
		t.Fatalf("committed records=%d want=%d", len(seen), len(want))
	}
	t.Log("10 consecutive commits, abort/reopen, refused offset race and successful next transaction; 11 authentic committed records")
}
