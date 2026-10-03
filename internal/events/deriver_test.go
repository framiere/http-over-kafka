package events_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/api"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/events"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkatest"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) != "" {
		childMain() // never returns
	}
	kafkatest.Main(m)
}

const partitions = 3

// testWindow is the dedup window in tests: Results completed earlier than
// that are outside it.
const testWindow = time.Hour

// fixture is one isolated service: its own result, event and failure topics.
type fixture struct {
	t        *testing.T
	svc      *apispec.Service
	name     string
	evTopic  string
	producer *kgo.Client
	bridge   *identity.Signer // signs the Results, as the bridge does (D10)
	trusted  string           // its public key, in env-var format
}

func newFixture(t *testing.T) *fixture {
	name := kafkatest.Service(t, "orders")
	evTopic := name + ".events"
	svc := load(t, name, ordersSpec(name))
	kafkatest.CreateTopics(t, kafkaenv.ServiceTopics(name, partitions)...)
	kafkatest.CreateTopics(t, events.Topics(svc, partitions, testWindow)...)
	bridge, trusted := bridgeSigner(t, "bridge-test")
	return &fixture{t: t, svc: svc, name: name, evTopic: evTopic, producer: kafkatest.Client(t), bridge: bridge, trusted: trusted}
}

// bridgeSigner returns a fresh bridge signing key and its public half.
func bridgeSigner(t *testing.T, kid string) (*identity.Signer, string) {
	t.Helper()
	signing, trusted, err := identity.Generate(kid)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(identity.RoleBridge), signing)
	s, err := identity.SignerFromEnv(identity.RoleBridge)
	if err != nil {
		t.Fatal(err)
	}
	return s, trusted
}

// ordersSpec is the demo orders spec with an event topic private to service.
func ordersSpec(service string) []byte {
	return []byte(strings.Replace(string(api.Orders), "topic: orders.events", "topic: "+service+".events", 1))
}

func (f *fixture) produce(recs ...*kgo.Record) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := f.producer.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) resultRecord(res wire.Result) *kgo.Record {
	f.t.Helper()
	rec, err := wire.EncodeResult(res, f.bridge)
	if err != nil {
		f.t.Fatal(err)
	}
	return rec
}

// created produces n Results of a successful createOrder and returns their
// requestIds mapped to the order id B assigned.
func (f *fixture) created(n int) map[string]string {
	f.t.Helper()
	want := map[string]string{}
	var recs []*kgo.Record
	for range n {
		cmd := createOrder(f.t, f.name, orderReq)
		id := "ord_" + strings.ToLower(cmd.RequestID)
		want[cmd.RequestID] = id
		recs = append(recs, f.resultRecord(result(f.t, cmd, answered(cmd, 201, orderResp(id)))))
	}
	f.produce(recs...)
	return want
}

func (f *fixture) failed(n int) {
	f.t.Helper()
	var recs []*kgo.Record
	for range n {
		cmd := createOrder(f.t, f.name, `{"items":[]}`)
		recs = append(recs, f.resultRecord(result(f.t, cmd, answered(cmd, 400, `{"title":"items must not be empty"}`))))
	}
	f.produce(recs...)
}

func (f *fixture) deriver(instance string) *events.Deriver {
	f.t.Helper()
	d, err := events.New(events.Config{Brokers: kafkatest.Brokers(f.t), Service: f.svc, Instance: instance,
		BridgeKeys: f.bridge.Self(), SessionTimeout: shortSession, DedupWindow: testWindow})
	if err != nil {
		f.t.Fatal(err)
	}
	return d
}

type running struct {
	cancel context.CancelFunc
	done   chan error
}

func start(d *events.Deriver) *running {
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- d.Run(ctx) }()
	return r
}

func (r *running) stop(t *testing.T) {
	t.Helper()
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("deriver: %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("deriver did not stop")
	}
}

// caughtUp waits until the deriver group has committed every Result offset.
// A transactional offset is only visible once its transaction committed, so
// from then on the outputs for all those Results are readable.
func (f *fixture) caughtUp(timeout time.Duration) {
	f.t.Helper()
	adm := kafkatest.Admin(f.t)
	kafkatest.Eventually(f.t, timeout, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ends, err := adm.ListEndOffsets(ctx, wire.ResultTopic(f.name))
		if err != nil {
			return false
		}
		committed, err := adm.FetchOffsets(ctx, events.GroupID(f.name))
		if err != nil {
			return false
		}
		ok := true
		ends.Each(func(o kadm.ListedOffset) {
			c, found := committed.Lookup(o.Topic, o.Partition)
			if o.Offset > 0 && (!found || c.At < o.Offset) {
				ok = false
			}
		})
		return ok
	}, "deriver group did not commit up to the end of %s", wire.ResultTopic(f.name))
}

// drain returns every data record of topic up to its current end offsets.
// Control records are kept internally so positions can reach the end even
// when a partition finishes with a transaction marker.
func drain(t *testing.T, topic string, level kgo.IsolationLevel) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ends, err := kafkatest.Admin(t).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	need := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			need[o.Partition] = o.Offset
		}
	})
	cl := kafkatest.Client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(level),
		kgo.KeepControlRecords(),
	)
	var out []*kgo.Record
	for len(need) > 0 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("drain %s: still waiting for %v", topic, need)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if !r.Attrs.IsControl() {
				out = append(out, r)
			}
			if end, ok := need[r.Partition]; ok && r.Offset+1 >= end {
				delete(need, r.Partition)
			}
		})
	}
	return out
}

func header(r *kgo.Record, k string) string {
	for _, h := range r.Headers {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}

// checkEvents asserts the committed event stream holds exactly one event per
// expected requestId, with the key and value the mapping prescribes.
func (f *fixture) checkEvents(want map[string]string) []*kgo.Record {
	f.t.Helper()
	recs := drain(f.t, f.evTopic, kgo.ReadCommitted())
	seen := map[string]bool{}
	for _, r := range recs {
		id := header(r, "ce_id")
		orderID, ok := want[id]
		switch {
		case !ok:
			f.t.Errorf("event %s for no successful createOrder: %s", id, r.Value)
		case seen[id]:
			f.t.Errorf("duplicate event for %s", id)
		case string(r.Key) != orderID:
			f.t.Errorf("event %s: key %s, want %s", id, r.Key, orderID)
		default:
			var v map[string]any
			if err := json.Unmarshal(r.Value, &v); err != nil || v["id"] != orderID || v["customerId"] != "c-42" {
				f.t.Errorf("event %s: value %s", id, r.Value)
			}
			if header(r, "ce_type") != "OrderCreated" || header(r, "ce_specversion") != "1.0" ||
				header(r, "ce_source") != events.Source(f.name, "createOrder") {
				f.t.Errorf("event %s: headers %v", id, r.Headers)
			}
		}
		seen[id] = true
	}
	if len(recs) != len(want) {
		f.t.Errorf("%d committed events, want %d", len(recs), len(want))
	}
	return recs
}

func TestDeriverEmitsOnlyMappedFacts(t *testing.T) {
	f := newFixture(t)
	want := f.created(3)
	f.failed(2)

	unknown := createOrder(t, f.name, orderReq)
	notJSON := createOrder(t, f.name, orderReq)
	del := command(t, f.name, "deleteOrder", "DELETE", "/orders/ord_1", "/orders/{orderId}", "")
	del.PathParams = map[string]string{"orderId": "ord_1"}
	f.produce(
		f.resultRecord(result(t, unknown, wire.FaultResponse(unknown.RequestID, wire.FaultOutcomeUnknown, "bridge restarted"))),
		f.resultRecord(result(t, notJSON, answered(notJSON, 201, "created"))),
		f.resultRecord(result(t, del, answered(del, 204, ""))),
		// A poison record on the result topic must not stop the stream.
		&kgo.Record{Topic: wire.ResultTopic(f.name), Key: []byte("junk"), Value: []byte("not a result")},
	)
	for id, ord := range f.created(2) { // after the failures: the flow keeps going
		want[id] = ord
	}

	r := start(f.deriver("d1"))
	f.caughtUp(time.Minute)
	r.stop(t)

	f.checkEvents(want)

	fails := drain(t, events.FailureTopic(f.name), kgo.ReadCommitted())
	reasons := map[string]events.Failure{}
	for _, r := range fails {
		var fl events.Failure
		if err := json.Unmarshal(r.Value, &fl); err != nil {
			t.Fatal(err)
		}
		reasons[fl.Reason] = fl
	}
	if len(fails) != 2 {
		t.Fatalf("%d failure records, want 2: %v", len(fails), reasons)
	}
	nj := reasons[events.ReasonEvaluationFailed]
	if nj.RequestID != notJSON.RequestID || nj.OperationID != "createOrder" || nj.Result.Topic != wire.ResultTopic(f.name) ||
		!strings.Contains(strings.Join(nj.Details, ";"), "response.body is not JSON") {
		t.Errorf("evaluation failure: %+v", nj)
	}
	if u := reasons[events.ReasonNotAuthentic]; u.Result.Topic != wire.ResultTopic(f.name) || len(u.Details) == 0 {
		t.Errorf("undecodable failure: %+v", u)
	}
}

// abortedResult writes a successful createOrder Result inside a transaction
// that is then aborted, as a fenced or failing bridge would leave it: the
// outcome was never recorded, so no event may come from it.
func (f *fixture) abortedResult() wire.Command {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl := kafkatest.Client(f.t, kgo.TransactionalID("bridge-zombie-"+f.name))
	if err := cl.BeginTransaction(); err != nil {
		f.t.Fatal(err)
	}
	cmd := createOrder(f.t, f.name, orderReq)
	rec := f.resultRecord(result(f.t, cmd, answered(cmd, 201, orderResp("ord_never"))))
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		f.t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		f.t.Fatal(err)
	}
	return cmd
}

// A zombie bridge's aborted Result says 201 ord_never; the bridge that
// fenced it re-processes the same command and commits 201 ord_real. Same
// key, same partition, aborted record first: only ord_real is a fact.
func TestDeriverIgnoresAbortedResults(t *testing.T) {
	f := newFixture(t)
	cmd := f.abortedResult()
	f.produce(f.resultRecord(result(t, cmd, answered(cmd, 201, orderResp("ord_real")))))
	want := f.created(2)
	want[cmd.RequestID] = "ord_real"

	if dirty := drain(t, wire.ResultTopic(f.name), kgo.ReadUncommitted()); len(dirty) != 4 {
		t.Fatalf("read_uncommitted sees %d results, want the aborted one plus 3", len(dirty))
	}

	r := start(f.deriver("d1"))
	f.caughtUp(time.Minute)
	r.stop(t)

	f.checkEvents(want) // fails on ord_never, or on a second event for cmd
	if n := len(drain(t, events.FailureTopic(f.name), kgo.ReadCommitted())); n != 0 {
		t.Errorf("%d failure records, want 0", n)
	}
}

// Instances join and leave while Results flow: rebalances abort in-flight
// transactions, and the committed stream still holds each event once.
func TestDeriverRebalance(t *testing.T) {
	f := newFixture(t)
	want := map[string]string{}
	add := func(m map[string]string) {
		for k, v := range m {
			want[k] = v
		}
	}
	a := start(f.deriver("a"))
	add(f.created(20))
	b := start(f.deriver("b"))
	for i := range 5 {
		add(f.created(10))
		f.failed(2)
		if i == 2 {
			a.stop(t)
		}
	}
	c := start(f.deriver("c"))
	add(f.created(10))
	f.caughtUp(2 * time.Minute)
	b.stop(t)
	c.stop(t)

	f.checkEvents(want)
	if n := len(drain(t, events.FailureTopic(f.name), kgo.ReadCommitted())); n != 0 {
		t.Errorf("%d failure records, want 0", n)
	}
}
