package bridge_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPurgeProtectsSignedReplayAcrossSkewedOwners(t *testing.T) {
	for _, kind := range []string{"request entry", "rejected request marker", "replayed request marker"} {
		t.Run(kind, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusCreated)
			}))
			defer srv.Close()
			e := newEnv(t, "clock-replay", api.Payments, srv.URL)
			cmd := e.charge("target", "")
			if kind != "request entry" {
				cmd.IdempotencyKey = "scope"
			}
			const skew = wire.DefaultClockSkew
			var clock atomic.Int64
			clock.Store(cmd.IssuedAt.Add(skew).UnixNano())
			cfg := e.config()
			cfg.Now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
			cfg.ClockSkew = skew
			cfg.IdempotencyRetention = time.Nanosecond // the replay bound must win
			cfg.PurgeInterval = time.Millisecond
			stop := e.start(cfg)
			if kind != "request entry" {
				original := cmd
				original.RequestID = wire.NewRequestID()
				e.send(original)
				if r := e.await(original.RequestID, 30*time.Second); r.Status != http.StatusCreated {
					t.Fatalf("initial response: %+v", r)
				}
				if kind == "rejected request marker" {
					cmd.Body = wire.NewBody([]byte(`{"accountId":"different","amountCents":1250,"currency":"EUR"}`))
				}
			}
			firstRecord := e.send(cmd)
			response := e.await(cmd.RequestID, 30*time.Second)
			if kind == "rejected request marker" {
				if response.Fault != wire.FaultIdempotencyKeyReused {
					t.Fatalf("initial rejection: %+v", response)
				}
			} else if response.Status != http.StatusCreated {
				t.Fatalf("initial response: %+v", response)
			}
			if kind == "replayed request marker" && response.ReplayOf == "" {
				t.Fatal("initial keyed retry did not use the stored outcome")
			}

			// A control on the same partition expires early. Its tombstone is
			// positive evidence that the fast owner's sweep has committed.
			control := e.charge("control", "")
			control.IssuedAt, control.Deadline = cmd.IssuedAt, cmd.Deadline
			control.ExpiresAt = cmd.ExpiresAt.Add(-2 * skew)
			hasher := kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service))
			for hasher.Partition(&kgo.Record{Key: []byte(control.DedupKey())}, partitions) != int(firstRecord.Partition) {
				control.RequestID = wire.NewRequestID()
			}
			e.send(control)
			if r := e.await(control.RequestID, 30*time.Second); r.Status != http.StatusCreated {
				t.Fatalf("control response: %+v", r)
			}
			clock.Store(cmd.ExpiresAt.Add(2*skew + time.Nanosecond).UnixNano())
			records := kafkatest.Consume(t, bridge.StateTopic(e.service), 30*time.Second, func(rs []*kgo.Record) bool {
				for _, r := range rs {
					if string(r.Key) == control.DedupKey() && r.Value == nil {
						return true
					}
				}
				return false
			})
			stateKey := cmd.DedupKey()
			if kind != "request entry" {
				stateKey = "q:" + cmd.RequestID
			}
			var retained *kgo.Record
			for _, r := range records {
				if string(r.Key) == stateKey {
					retained = r
				}
			}
			if retained == nil || retained.Value == nil {
				t.Fatalf("fast owner purged %s while the slow owner could still accept it", stateKey)
			}
			var state struct {
				PurgeAfter      time.Time
				RetentionPolicy uint8
			}
			if err := json.Unmarshal(retained.Value, &state); err != nil {
				t.Fatal(err)
			}
			if !state.PurgeAfter.Equal(cmd.ExpiresAt.Add(3*skew)) || state.RetentionPolicy != 1 {
				t.Fatalf("unsafe persisted retention: %+v", state)
			}
			stop()

			// At the same reference time another owner can be 2S behind.
			// It restores the state, then receives the exact signed bytes at
			// a new Kafka offset. Wait for its committed offset, not a sleep.
			clock.Store(cmd.ExpiresAt.Add(time.Nanosecond).UnixNano())
			stop = e.start(cfg)
			replay := &kgo.Record{Topic: firstRecord.Topic, Key: firstRecord.Key,
				Value: firstRecord.Value, Headers: firstRecord.Headers}
			e.produce(replay)
			admin := kafkatest.Admin(t)
			kafkatest.Eventually(t, 30*time.Second, func() bool {
				offsets, err := admin.FetchOffsets(t.Context(), "hok-bridge."+e.service)
				if err != nil {
					return false
				}
				o, ok := offsets.Lookup(wire.CommandTopic(e.service), replay.Partition)
				return ok && o.Err == nil && o.At > replay.Offset
			}, "restored slow owner settles the signed replay")
			stop()
			if n := calls.Load(); n != 2 {
				t.Fatalf("signed replay caused extra upstream execution: got %d calls, want target + control", n)
			}
			if rs := e.responsesFor(cmd.RequestID); len(rs) != 1 {
				t.Fatalf("signed replay caused duplicate responses: %+v", rs)
			}
		})
	}
}
