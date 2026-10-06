package bridge

import (
	"encoding/base64"
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestStateV2RejectsLegacySignatures(t *testing.T) {
	signer := stateTestSigner(t, "legacy", 9)
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"entry", []byte(`{"phase":"done","requestId":"old"}`)},
		{"tombstone", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &kgo.Record{Topic: "http.bridge-state.legacy", Partition: 1, Offset: 37, Key: []byte("i:legacy-key"), Value: tc.value}
			// Deliberately independent of the current state's signing helpers: this
			// is the deployed v1 contract, which did not bind the Kafka offset.
			payload := []byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s", rec.Topic, rec.Partition, rec.Key, rec.Value))
			sig := signer.Sign("http-over-kafka/bridge-state/v1", payload)
			rec.Headers = []kgo.RecordHeader{
				{Key: "hok-kid", Value: []byte(signer.KeyID())},
				{Key: "hok-sig", Value: []byte(base64.StdEncoding.EncodeToString(sig))},
			}
			if err := verifyStateRecord(signer.Self(), rec); !errors.Is(err, errStateTampered) {
				t.Fatalf("legacy %s accepted or misclassified: %v", tc.name, err)
			}
		})
	}
}

func TestStateV2BindsPhysicalOffsetAndTombstoneKind(t *testing.T) {
	signer := stateTestSigner(t, "v2", 8)
	for _, tc := range []struct {
		name  string
		alter func(*kgo.Record)
	}{
		{"new offset", func(r *kgo.Record) { r.Offset++ }},
		{"nil replaced by empty bytes", func(r *kgo.Record) { r.Value = []byte{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := signedStateRecord(signer, StateTopic("payments"), 1, "i:key", nil)
			rec.Offset = 81
			bindStateRecord(signer, rec, rec.Offset)
			if err := verifyStateRecord(signer.Self(), rec); err != nil {
				t.Fatalf("original record rejected: %v", err)
			}
			tc.alter(rec)
			if err := verifyStateRecord(signer.Self(), rec); !errors.Is(err, errStateTampered) {
				t.Fatalf("modified record accepted: %v", err)
			}
		})
	}
}

func TestStateV2SealChecksFinalCompactedInventory(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(*store)
	}{
		{"missing seal", func(s *store) { s.seal = nil }},
		{"entry erased", func(s *store) { delete(s.entries, "i:key") }},
		{"unexpected entry", func(s *store) { s.entries["i:other"] = &entry{} }},
		{"offset changed", func(s *store) { s.next++ }},
		{"genesis changed", func(s *store) { s.genesis = "other-genesis" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := store{entries: map[string]*entry{"i:key": {}}, next: 11, genesis: "original-genesis",
				seal: &sealValue{Entries: 1, Next: 11, Genesis: "original-genesis"}}
			if err := s.verifySeal(); err != nil {
				t.Fatalf("valid final inventory rejected: %v", err)
			}
			tc.alter(&s)
			if err := s.verifySeal(); !errors.Is(err, errStateTampered) {
				t.Fatalf("invalid final inventory accepted: %v", err)
			}
		})
	}
}
