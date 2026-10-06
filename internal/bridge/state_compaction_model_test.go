package bridge

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Generate legal state histories independently of the writer implementation,
// then emulate Kafka's offset-preserving compaction at arbitrary frontiers.
func TestStateV2CompactionModel(t *testing.T) {
	signer := stateTestSigner(t, "model", 27)
	rng := rand.New(rand.NewPCG(19, 87))
	checked := 0
	for history := range 200 {
		var log []*kgo.Record
		var offset, next int64
		live := map[string]*entry{}
		appendRecord := func(key string, value any) {
			r, err := stateRecord("http.bridge-state.model", 0, key, value)
			if err != nil {
				t.Fatal(err)
			}
			bindStateRecord(signer, r, offset)
			r.Offset = offset
			offset++
			log = append(log, r)
		}
		for step := range 30 {
			key := fmt.Sprintf("i:key-%d", rng.IntN(12))
			if rng.IntN(4) == 0 {
				delete(live, key)
				appendRecord(key, nil)
			} else {
				e := &entry{RequestID: fmt.Sprintf("%d-%d", history, step), Phase: done}
				live[key] = e
				appendRecord(key, e)
			}
			if rng.IntN(2) == 0 {
				next += int64(rng.IntN(8))
				appendRecord(keyOffset, offsetValue{Next: next})
			}
			appendRecord(keySeal, sealValue{Entries: len(live), Next: next})
			offset++ // transaction control record, invisible to store.apply
		}
		for _, frontier := range []int{0, len(log) / 3, len(log) / 2, len(log)} {
			last := map[string]int{}
			for i, r := range log[:frontier] {
				last[string(r.Key)] = i
			}
			var retained []*kgo.Record
			for i, r := range log {
				if i < frontier && (last[string(r.Key)] != i || r.Value == nil) {
					continue
				}
				retained = append(retained, r)
			}
			st := store{entries: map[string]*entry{}}
			for _, r := range retained {
				if err := verifyStateRecord(signer.Self(), r); err != nil {
					t.Fatal(err)
				}
				if err := st.apply(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.verifySeal(); err != nil || st.next != next || !reflect.DeepEqual(st.entries, live) {
				t.Fatalf("history=%d frontier=%d reconstructed wrong state: err=%v next=%d want=%d", history, frontier, err, st.next, next)
			}
			checked++
		}
		// Model a malicious tombstone after the last commit, compacted until
		// neither tombstone nor victim remains. The retained seal must reject.
		for key := range live {
			st := store{entries: map[string]*entry{}}
			for _, r := range log {
				if string(r.Key) == key {
					continue
				}
				if err := st.apply(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := st.verifySeal(); !errors.Is(err, errStateTampered) {
				t.Fatalf("deleted key=%s accepted: %v", key, err)
			}
		}
		for _, old := range log {
			replay := *old
			replay.Offset = offset
			if err := verifyStateRecord(signer.Self(), &replay); !errors.Is(err, errStateTampered) {
				t.Fatalf("replay from %d at %d accepted: %v", old.Offset, offset, err)
			}
		}
	}
	t.Logf("%d valid partial/full compactions restored; deleted live keys and old-record replays rejected", checked)
}
