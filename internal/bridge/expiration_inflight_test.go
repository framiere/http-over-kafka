package bridge_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// Expiration forbids starting another attempt. It must not cancel or relabel
// an attempt already in flight, including one whose response body is cut off.
func TestCommandExpirationKeepsInFlightOutcome(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replayable bool
		incomplete bool
	}{
		{name: "non replayable success"},
		{name: "replayable success", replayable: true},
		{name: "non replayable partial response", incomplete: true},
		{name: "replayable partial response", replayable: true, incomplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			var now atomic.Int64
			var cmd wire.Command
			entered, release := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			unblockCall := func() { unblock.Do(func() { close(release) }) }
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get(wire.RequestIDHeader) != cmd.RequestID {
					w.WriteHeader(http.StatusCreated)
					return
				}
				if calls.Add(1) == 1 {
					close(entered)
				}
				<-release
				if tc.incomplete {
					w.Header().Set("Content-Length", "100")
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(`{"applied":true}`))
			}))
			defer srv.Close()
			defer unblockCall()
			spec := api.Payments
			if tc.replayable {
				spec = api.Orders
			}
			e := newEnv(t, "expiry-inflight", spec, srv.URL)
			cmd = e.charge("expiry", "")
			if tc.replayable {
				cmd = e.command(http.MethodPut, "replaceOrder", "/orders/{orderId}", "/orders/o1",
					map[string]string{"orderId": "o1"}, `{"customerId":"c1","items":[]}`, "")
			}
			now.Store(cmd.IssuedAt.UnixNano())
			cfg := e.config()
			cfg.Now = func() time.Time { return time.Unix(0, now.Load()) }
			e.start(cfg)
			e.send(cmd)
			select {
			case <-entered:
			case <-time.After(30 * time.Second):
				t.Fatal("upstream call never started")
			}
			now.Store(cmd.ExpiresAt.Add(wire.DefaultClockSkew + time.Nanosecond).UnixNano())
			unblockCall()
			got := e.await(cmd.RequestID, 30*time.Second)
			if calls.Load() != 1 {
				t.Fatalf("in-flight call retried after expiration: %d calls", calls.Load())
			}
			if tc.incomplete {
				if got.Fault != wire.FaultResponseIncomplete || got.UpstreamStatus != http.StatusCreated {
					t.Fatalf("lost evidence of the upstream response: %+v", got)
				}
			} else if got.Fault != "" || got.Status != http.StatusCreated || string(got.Body.Bytes()) != `{"applied":true}` {
				t.Fatalf("known success changed after expiration: %+v", got)
			}
			assertExpirationResult(t, e, cmd, got)
		})
	}
}
