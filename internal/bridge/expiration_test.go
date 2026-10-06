package bridge

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// Recovery decisions depend on what survived in Kafka and what this process
// still knows. Exercise those combinations without a broker; the integration
// tests separately exercise the actual transaction boundaries.
func TestExpiredCommandRecoveryPreservesExecutionEvidence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		replayable bool
		marker     string // own or foreign started marker; empty: no marker
		prior      wire.Outcome
		called     bool
		known      bool
		advance    bool // expire between verification and the execution check
		want       wire.Fault
	}{
		{name: "own marker before call", marker: "own", want: wire.FaultCommandStale},
		{name: "foreign marker", marker: "foreign", want: wire.FaultOutcomeUnknown},
		{name: "foreign replayable marker", replayable: true, marker: "foreign", want: wire.FaultOutcomeUnknown},
		{name: "foreign replayable marker expires before call", replayable: true, marker: "foreign", advance: true, want: wire.FaultOutcomeUnknown},
		{name: "known result with own marker", marker: "own", called: true, known: true},
		{name: "known replayable result without marker", replayable: true, called: true, known: true},
		{name: "known replayable result over not executed entry", replayable: true, prior: wire.OutcomeNotExecuted, called: true, known: true},
		{name: "known replayable result over unknown entry", replayable: true, prior: wire.OutcomeUnknown, called: true, known: true},
		{name: "uncertain local attempt without marker", replayable: true, called: true, want: wire.FaultOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusCreated)
			}))
			defer srv.Close()
			cmd := command("/charges", []byte(`{}`), nil)
			cmd.Service = "expiration"
			specData := api.Payments
			if tc.replayable {
				cmd.Method, cmd.OperationID = http.MethodPut, "replaceOrder"
				cmd.Path, cmd.PathTemplate = "/orders/o1", "/orders/{orderId}"
				cmd.PathParams = map[string]string{"orderId": "o1"}
				specData = api.Orders
			}
			spec, err := apispec.Load(cmd.Service, specData)
			if err != nil {
				t.Fatal(err)
			}
			gw, err := identity.NewSigner(identity.RoleGateway, "gateway", bytes.Repeat([]byte{1}, 32))
			if err != nil {
				t.Fatal(err)
			}
			signer, err := identity.NewSigner(identity.RoleBridge, "bridge", bytes.Repeat([]byte{2}, 32))
			if err != nil {
				t.Fatal(err)
			}
			expired := cmd.ExpiresAt.Add(wire.DefaultClockSkew + time.Nanosecond)
			reads := 0
			b, err := New(Config{
				Service: cmd.Service, Spec: spec, Keys: gw.Self(), Signer: signer,
				Brokers: []string{"unused:9092"}, Upstream: srv.URL,
				Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
				Now: func() time.Time {
					reads++
					if tc.advance && reads == 1 {
						return cmd.IssuedAt
					}
					return expired
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			b.partitions = 1
			b.replyTopics[cmd.ReplyTo] = true
			w := newPartition(b, nil, 0)
			defer w.end()
			defer close(w.cgone)
			w.st.entries = map[string]*entry{}
			rec, err := wire.EncodeCommand(cmd, gw)
			if err != nil {
				t.Fatal(err)
			}
			l := &lane{key: cmd.DedupKey()}
			cur := w.attemptFor(l, rec, cmd)
			cur.called = tc.called
			if tc.known {
				cur.resp = &wire.Response{V: wire.Version, RequestID: cmd.RequestID,
					Status: http.StatusCreated, Headers: wire.Headers{}, Body: wire.NewBody([]byte(`{"applied":true}`))}
			}
			if tc.marker != "" {
				id := cur.id
				if tc.marker == "foreign" {
					id = "previous-owner"
				}
				w.st.entries[cmd.DedupKey()] = &entry{RequestID: cmd.RequestID,
					Fingerprint: cmd.Fingerprint(), Phase: started, Attempt: id}
			}
			if tc.prior != "" {
				w.st.entries[cmd.DedupKey()] = &entry{RequestID: wire.NewRequestID(),
					Fingerprint: cmd.Fingerprint(), Phase: done, Outcome: tc.prior}
			}
			processed := make(chan error, 1)
			go func() { processed <- w.process(l, rec, 1) }()
			var result *wire.Result
			var response *wire.Response
			var final *entry
		loop:
			for {
				select {
				case req := <-w.reqs:
					for _, out := range req.out {
						switch out.Topic {
						case wire.ResultTopic(cmd.Service):
							r, err := wire.DecodeResult(out, signer.Self())
							if err != nil {
								t.Fatal(err)
							}
							result = &r
						case cmd.ReplyTo:
							r, err := wire.DecodeResponse(out, signer.Self())
							if err != nil {
								t.Fatal(err)
							}
							response = &r
						}
					}
					for key, e := range req.writes {
						w.st.entries[key] = e
						if key == cmd.DedupKey() && e.Phase == done {
							final = e
						}
					}
					req.err <- nil
				case err := <-processed:
					if err != nil {
						t.Fatal(err)
					}
					break loop
				case <-time.After(5 * time.Second):
					t.Fatal("processing did not finish")
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("expired command reached upstream %d times", calls.Load())
			}
			if response == nil || result == nil || final == nil {
				t.Fatalf("missing durable output: response=%+v result=%+v entry=%+v", response, result, final)
			}
			if response.Fault != tc.want || result.Response.Fault != tc.want || final.Outcome != wire.OutcomeOf(*response) {
				t.Fatalf("inconsistent outcome: response=%+v result=%+v entry=%+v; want fault %q", response, result, final, tc.want)
			}
			if tc.known && (response.Status != http.StatusCreated || !bytes.Equal(response.Body.Bytes(), cur.resp.Body.Bytes())) {
				t.Fatalf("lost the known upstream response: %+v", response)
			}
		})
	}
}
