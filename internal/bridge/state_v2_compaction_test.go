package bridge_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func v2SignStateAt(signer *identity.Signer, topic string, partition int32, offset int64, key string, value []byte) *kgo.Record {
	payload := []byte(fmt.Sprintf("%s\x00%d\x00%d\x00%s\x00%t\x00%s", topic, partition, offset, key, value == nil, value))
	sig := signer.Sign("http-over-kafka/bridge-state/v2", payload)
	return &kgo.Record{Topic: topic, Partition: partition, Key: []byte(key), Value: value, Headers: []kgo.RecordHeader{
		{Key: "hok-kid", Value: []byte(signer.KeyID())},
		{Key: "hok-sig", Value: []byte(base64.StdEncoding.EncodeToString(sig))},
	}}
}

// The bridge is stopped while this fixture writes authenticated fences.
// These force segment rolls without altering the seal or the dedup inventory.
// Only the malicious tombstone below is an untrusted record.
func v2AppendCompactionFence(t *testing.T, e *env, producer *kgo.Client, partition int32) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ends, err := kadm.NewClient(producer).ListEndOffsets(ctx, bridge.StateTopic(e.service))
	if err != nil {
		t.Fatal(err)
	}
	end, ok := ends.Lookup(bridge.StateTopic(e.service), partition)
	if !ok || end.Err != nil {
		t.Fatalf("state end unavailable: %+v", end)
	}
	value, err := json.Marshal(map[string]any{"instance": "compaction-fixture", "at": time.Now().UTC(), "padding": strings.Repeat("x", 8192)})
	if err != nil {
		t.Fatal(err)
	}
	rec := v2SignStateAt(e.bridgeSigner, bridge.StateTopic(e.service), partition, end.Offset, "#fence", value)
	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if rec.Offset != end.Offset {
		t.Fatalf("fixture lost exclusive write position: got %d want %d", rec.Offset, end.Offset)
	}
	return rec.Offset
}

// A new consumer reads a finite broker snapshot through the latest fence.
// Checking offsets, rather than waiting a fixed duration, proves compaction
// actually removed the victim and tombstone before restoration begins.
func v2StateSnapshot(t *testing.T, e *env, partition int32, through int64) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(kafkatest.Brokers(t),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{bridge.StateTopic(e.service): {partition: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var records []*kgo.Record
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read state through %d: %v", through, ctx.Err())
		}
		fs.EachError(func(topic string, p int32, err error) { t.Fatalf("fetch %s[%d]: %v", topic, p, err) })
		for _, rec := range fs.Records() {
			if rec.Offset > through {
				continue
			}
			records = append(records, rec)
			if rec.Offset == through {
				return records
			}
		}
	}
}

func v2EnableFastCompaction(t *testing.T, topic string) {
	t.Helper()
	var changes []kadm.AlterConfig
	for name, value := range map[string]string{
		"segment.bytes":             "16384",
		"segment.ms":                "100",
		"min.cleanable.dirty.ratio": "0.001",
		"min.compaction.lag.ms":     "0",
		"max.compaction.lag.ms":     "1000",
		"delete.retention.ms":       "100",
		"file.delete.delay.ms":      "0",
	} {
		changes = append(changes, kadm.AlterConfig{Op: kadm.SetConfig, Name: name, Value: kadm.StringPtr(value)})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result, err := kafkatest.Admin(t).AlterTopicConfigs(ctx, changes, topic)
	if err != nil {
		t.Fatal(err)
	}
	response, err := result.On(topic, nil)
	if err != nil || response.Err != nil {
		t.Fatalf("enable compaction: response=%+v error=%v", response, err)
	}
}

func TestStateV2RestorationAfterRealCompaction(t *testing.T) {
	for _, attack := range []bool{false, true} {
		name := "normal inventory"
		if attack {
			name = "unsigned tombstone and victim erased"
		}
		t.Run(name, func(t *testing.T) {
			pay, srv := paymentsB(t)
			e := newEnv(t, "v2-compact", api.Payments, srv.URL)
			cfg := e.config()
			stop := e.start(cfg)
			cmd := e.charge("account", "retained-key")
			commandRecord := e.send(cmd)
			original := e.await(cmd.RequestID, 30*time.Second)
			if original.Status != 201 {
				t.Fatalf("initial response: %+v", original)
			}
			stop()
			producer := kafkatest.Client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerLinger(0))
			through := v2AppendCompactionFence(t, e, producer, commandRecord.Partition)
			before := v2StateSnapshot(t, e, commandRecord.Partition, through)
			countKey := func(records []*kgo.Record) int {
				n := 0
				for _, r := range records {
					if string(r.Key) == cmd.DedupKey() {
						n++
					}
				}
				return n
			}
			if n := countKey(before); n < 2 {
				t.Fatalf("need started and done versions before compaction, got %d", n)
			}
			var tombstoneOffset int64 = -1
			if attack {
				tombstone := &kgo.Record{Topic: bridge.StateTopic(e.service), Partition: commandRecord.Partition, Key: []byte(cmd.DedupKey())}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := producer.ProduceSync(ctx, tombstone).FirstErr(); err != nil {
					t.Fatal(err)
				}
				tombstoneOffset = tombstone.Offset
			}
			v2EnableFastCompaction(t, bridge.StateTopic(e.service))
			var compacted []*kgo.Record
			deadline := time.Now().Add(90 * time.Second)
			for {
				// A few fresh fences also roll the segment containing the last write.
				for range 3 {
					through = v2AppendCompactionFence(t, e, producer, commandRecord.Partition)
				}
				compacted = v2StateSnapshot(t, e, commandRecord.Partition, through)
				want := 1
				if attack {
					want = 0
				}
				if countKey(compacted) == want {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("real compaction did not finish: %d records for victim, want %d", countKey(compacted), want)
				}
				select {
				case <-t.Context().Done():
					t.Fatal(t.Context().Err())
				case <-time.After(500 * time.Millisecond):
				}
			}
			var seal struct {
				Entries int    `json:"entries"`
				Next    int64  `json:"next"`
				Genesis string `json:"genesis"`
			}
			sealFound := false
			for _, r := range compacted {
				if r.Offset == tombstoneOffset {
					t.Fatal("malicious tombstone still exists: signature verification would hide a missing-inventory bug")
				}
				if string(r.Key) == "#seal" {
					if err := json.Unmarshal(r.Value, &seal); err != nil {
						t.Fatal(err)
					}
					sealFound = true
				}
			}
			if !sealFound || seal.Entries != 1 || seal.Genesis == "" {
				t.Fatalf("retained inventory seal: found=%t %+v", sealFound, seal)
			}
			retry := cmd
			retry.RequestID = wire.NewRequestID()
			e.send(retry)
			if attack {
				requireStateV2RestoreRejected(t, cfg)
				if got := e.responsesFor(retry.RequestID); len(got) != 0 {
					t.Fatalf("untrusted compacted state served retry: %+v", got)
				}
			} else {
				e.start(cfg)
				got := e.await(retry.RequestID, 30*time.Second)
				if got.Status != original.Status || got.ReplayOf != cmd.RequestID || string(got.Body.Bytes()) != string(original.Body.Bytes()) {
					t.Fatalf("compacted state lost stored response: %+v", got)
				}
			}
			if count := pay.Account("account").DebitCount; count != 1 {
				t.Fatalf("compacted history repeated effect: %d debits", count)
			}
			t.Logf("broker compaction retained %d victim records; signed inventory still has %d entries", countKey(compacted), seal.Entries)
		})
	}
}
