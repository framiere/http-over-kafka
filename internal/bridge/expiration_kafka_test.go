package bridge_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// A command can be valid on receipt and expire while its started transaction
// commits. Use the existing commit hook and a controlled clock, not sleeps.
func TestCommandExpirationAfterStartedCommit(t *testing.T) {
	const skew = 7 * time.Second
	for _, tc := range []struct {
		name  string
		after time.Duration
		want  wire.Fault
	}{
		{"before expiry", -time.Nanosecond, ""},
		{"clock skew boundary", skew, ""},
		{"past clock skew", skew + time.Nanosecond, wire.FaultCommandStale},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var cmd wire.Command
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(wire.RequestIDHeader) == cmd.RequestID {
					calls.Add(1)
				}
				w.WriteHeader(http.StatusCreated)
			}))
			defer srv.Close()
			e := newEnv(t, "expiry-start", api.Payments, srv.URL)
			cmd = e.charge("expiry", "")
			var now atomic.Int64
			now.Store(cmd.IssuedAt.UnixNano())
			cfg := e.config()
			cfg.ClockSkew = skew
			cfg.Now = func() time.Time { return time.Unix(0, now.Load()) }
			var started atomic.Int32
			cfg.Hooks.AfterStarted = func(id string) {
				if id == cmd.RequestID {
					started.Add(1)
					now.Store(cmd.ExpiresAt.Add(tc.after).UnixNano())
				}
			}
			e.start(cfg)
			e.send(cmd)
			got := e.await(cmd.RequestID, 30*time.Second)
			wantCalls := int32(1)
			if tc.want != "" {
				wantCalls = 0
			}
			if started.Load() != 1 || calls.Load() != wantCalls || got.Fault != tc.want {
				t.Fatalf("started=%d calls=%d response=%+v; want calls=%d fault=%q", started.Load(), calls.Load(), got, wantCalls, tc.want)
			}
			assertExpirationResult(t, e, cmd, got)
		})
	}
}

func TestReplayableCommandExpirationBeforeRetry(t *testing.T) {
	for _, scenario := range []string{"retry succeeds", "expires", "retry unavailable"} {
		t.Run(scenario, func(t *testing.T) {
			var now atomic.Int64
			var calls atomic.Int32
			var cmd wire.Command
			var srv *httptest.Server
			srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(wire.RequestIDHeader) == cmd.RequestID && calls.Add(1) == 1 {
					_, _ = io.Copy(io.Discard, r.Body)
					if scenario == "expires" {
						now.Store(cmd.ExpiresAt.Add(wire.DefaultClockSkew + time.Nanosecond).UnixNano())
					}
					if scenario == "retry unavailable" {
						_ = srv.Listener.Close()
					}
					conn, _, err := http.NewResponseController(w).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close() // B applied the mutation, but its answer is lost.
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			e := newEnv(t, "expiry-retry", api.Orders, srv.URL)
			cmd = e.command(http.MethodPut, "replaceOrder", "/orders/{orderId}", "/orders/o1",
				map[string]string{"orderId": "o1"}, `{"customerId":"c1","items":[]}`, "")
			now.Store(cmd.IssuedAt.UnixNano())
			cfg := e.config()
			cfg.Now = func() time.Time { return time.Unix(0, now.Load()) }
			e.start(cfg)
			e.send(cmd)
			got := e.await(cmd.RequestID, 30*time.Second)
			wantCalls, wantFault := int32(2), wire.Fault("")
			if scenario != "retry succeeds" {
				wantCalls, wantFault = 1, wire.FaultOutcomeUnknown
			}
			if calls.Load() != wantCalls || got.Fault != wantFault {
				t.Fatalf("calls=%d response=%+v; want calls=%d fault=%q", calls.Load(), got, wantCalls, wantFault)
			}
			assertExpirationResult(t, e, cmd, got)
		})
	}
}

// Failure to commit an outcome can require deciding against a restored store
// after the command expires. A known answer remains authoritative even for a
// replayable operation, which has no durable started marker.
func TestCommandExpirationDuringOutcomeCommitRecovery(t *testing.T) {
	for _, tc := range []struct {
		name          string
		replayable    bool
		foreignMarker bool
	}{
		{name: "non replayable"},
		{name: "replayable", replayable: true},
		{name: "replayable with foreign marker", replayable: true, foreignMarker: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var cmd wire.Command
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(wire.RequestIDHeader) == cmd.RequestID {
					calls.Add(1)
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"applied":true}`))
			}))
			defer srv.Close()
			spec := api.Payments
			if tc.replayable && !tc.foreignMarker {
				spec = api.Orders
			}
			e := newEnv(t, "expiry-commit", spec, srv.URL)
			cmd = e.charge("expiry", "")
			if tc.replayable && !tc.foreignMarker {
				cmd = e.command(http.MethodPut, "replaceOrder", "/orders/{orderId}", "/orders/o1",
					map[string]string{"orderId": "o1"}, `{"customerId":"c1","items":[]}`, "")
			}
			p := kgo.StickyKeyPartitioner(nil).ForTopic(wire.CommandTopic(e.service)).
				Partition(&kgo.Record{Key: []byte(cmd.DedupKey())}, partitions)
			var now atomic.Int64
			now.Store(cmd.IssuedAt.UnixNano())
			cfg := e.config()
			if tc.foreignMarker {
				op, ok := cfg.Spec.Operation(cmd.OperationID)
				if !ok {
					t.Fatal("missing operation")
				}
				op.RetrySafe = true // current contract; the old owner persisted started
			}
			cfg.Now = func() time.Time { return time.Unix(0, now.Load()) }
			var once sync.Once
			cfg.Hooks.BeforeCommit = func(id string) {
				if id != cmd.RequestID {
					return
				}
				once.Do(func() {
					cl := kafkatest.Client(t, kgo.TransactionalID(fmt.Sprintf("hok-bridge.%s.%d", e.service, p)),
						kgo.RecordPartitioner(kgo.ManualPartitioner()))
					if err := cl.BeginTransaction(); err != nil {
						t.Error(err)
						return
					}
					rec := signedState(e, int32(p), "#fence", []byte(`{"instance":"intruder"}`))
					if err := cl.ProduceSync(t.Context(), rec).FirstErr(); err != nil {
						t.Error(err)
						return
					}
					if tc.foreignMarker {
						// A previous owner may have used a contract where this
						// operation was non-replayable and persisted a marker.
						value, err := json.Marshal(map[string]any{
							"requestId": cmd.RequestID, "fingerprint": cmd.Fingerprint(),
							"phase": "started", "attempt": "foreign",
							"purgeAfter": cmd.ExpiresAt.Add(time.Hour),
						})
						if err != nil {
							t.Error(err)
							return
						}
						marker := signedState(e, int32(p), cmd.DedupKey(), value)
						if err := cl.ProduceSync(t.Context(), marker).FirstErr(); err != nil {
							t.Error(err)
							return
						}
					}
					if err := cl.EndTransaction(t.Context(), kgo.TryCommit); err != nil {
						t.Error(err)
						return
					}
					now.Store(cmd.ExpiresAt.Add(wire.DefaultClockSkew + time.Nanosecond).UnixNano())
				})
			}
			e.start(cfg)
			e.send(cmd)
			got := e.await(cmd.RequestID, 60*time.Second)
			if calls.Load() != 1 || got.Status != http.StatusCreated || got.Fault != "" || string(got.Body.Bytes()) != `{"applied":true}` {
				t.Fatalf("lost known result or repeated the call: calls=%d response=%+v", calls.Load(), got)
			}
			assertExpirationResult(t, e, cmd, got)
		})
	}
}

func assertExpirationResult(t *testing.T, e *env, cmd wire.Command, resp wire.Response) {
	t.Helper()
	rs := resultsFor(e.results(), cmd.RequestID)
	if len(rs) != 1 || rs[0].Outcome != wire.OutcomeOf(resp) || rs[0].Response.Fault != resp.Fault {
		t.Fatalf("durable result disagrees with the response: %+v", rs)
	}
	if n := len(e.responsesFor(cmd.RequestID)); n != 1 {
		t.Fatalf("got %d responses, want one", n)
	}
}
