package bridge_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func requireStateV2RestoreRejected(t *testing.T, cfg bridge.Config) {
	t.Helper()
	b, err := bridge.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err = b.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "dedup state tampered") {
		t.Fatalf("restoration must stop on untrusted state, got %v", err)
	}
}

// The attacker copies a previously authentic tombstone without any private
// key. Its old signature must not authorize deleting a newer claim at a new
// Kafka offset, whether the active signing key has rotated or not.
func TestStateV2AuthenticTombstoneReplayStopsRestoration(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		name := "same signing key"
		if rotate {
			name = "rotated signing key"
		}
		t.Run(name, func(t *testing.T) {
			pay, srv := paymentsB(t)
			e := newEnv(t, "v2-replay", api.Payments, srv.URL)
			active, trust := rotationKeys(t, e)
			var nanos atomic.Int64
			nanos.Store(time.Now().UTC().UnixNano())
			now := func() time.Time { return time.Unix(0, nanos.Load()).UTC() }
			cfg := e.config()
			cfg.Now = now
			cfg.MaxTTL = 10 * time.Second
			cfg.ClockSkew = time.Millisecond
			cfg.IdempotencyRetention = 20 * time.Second
			cfg.PurgeInterval = 10 * time.Millisecond
			cfg.StateKeys = trust
			command := func() wire.Command {
				cmd := e.charge("account", "reused-key")
				cmd.IssuedAt = now()
				cmd.ExpiresAt = now().Add(10 * time.Second)
				cmd.Deadline = now().Add(time.Second)
				return cmd
			}
			stop := e.start(cfg)
			first := command()
			firstRecord := e.send(first)
			if got := e.await(first.RequestID, 30*time.Second); got.Status != 201 {
				t.Fatalf("first: %+v", got)
			}
			nanos.Add(int64(time.Minute))
			// Trigger a purge on this partition without depending on an idle
			// purge timer from another branch. This is a different account/key.
			var maintenance wire.Command
			for i := 0; ; i++ {
				maintenance = e.charge("maintenance-account", fmt.Sprintf("maintenance-%d", i))
				p := kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service)).Partition(&kgo.Record{Key: []byte(maintenance.DedupKey())}, partitions)
				if int32(p) == firstRecord.Partition {
					break
				}
			}
			maintenance.IssuedAt, maintenance.ExpiresAt, maintenance.Deadline = now(), now().Add(10*time.Second), now().Add(time.Second)
			e.send(maintenance)
			if got := e.await(maintenance.RequestID, 30*time.Second); got.Status != 201 {
				t.Fatalf("maintenance command: %+v", got)
			}
			var captured *kgo.Record
			kafkatest.Consume(t, bridge.StateTopic(e.service), 30*time.Second, func(rs []*kgo.Record) bool {
				for _, r := range rs {
					if string(r.Key) == first.DedupKey() && r.Value == nil {
						captured = r
						return true
					}
				}
				return false
			})
			stop()
			if rotate {
				cfg.Signer = active
			}
			stop = e.start(cfg)
			second := command()
			e.send(second)
			if got := e.await(second.RequestID, 30*time.Second); got.Status != 201 || got.ReplayOf != "" {
				t.Fatalf("new window: %+v", got)
			}
			if count := pay.Account("account").DebitCount; count != 2 {
				t.Fatalf("before replay: %d", count)
			}
			stop()

			replay := &kgo.Record{Topic: captured.Topic, Partition: captured.Partition, Key: captured.Key, Value: nil, Headers: captured.Headers}
			attacker := kafkatest.Client(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := attacker.ProduceSync(ctx, replay).FirstErr(); err != nil {
				t.Fatal(err)
			}
			if replay.Offset <= captured.Offset {
				t.Fatalf("replay offset %d must follow original %d", replay.Offset, captured.Offset)
			}
			retry := command()
			e.send(retry)
			requireStateV2RestoreRejected(t, cfg)
			if count := pay.Account("account").DebitCount; count != 2 {
				t.Fatalf("historical tombstone repeated effect: %d debits", count)
			}
			if got := e.responsesFor(retry.RequestID); len(got) != 0 {
				t.Fatalf("untrusted state served a retry: %+v", got)
			}
			t.Logf("authentic tombstone from offset %d rejected at replay offset %d; debits remain 2", captured.Offset, replay.Offset)
		})
	}
}
