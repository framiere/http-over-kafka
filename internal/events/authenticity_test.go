package events_test

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/events"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkatest"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The verifier's C6 attack and its variants: whoever can write to the result
// topic tries to mint an OrderCreated. None may become an event; each must be
// visible in the failure topic as not_authentic, and the stream keeps going.
func TestDeriverRejectsForgedResults(t *testing.T) {
	f := newFixture(t)
	genuine := createOrder(t, f.name, orderReq)
	genuineRec := f.resultRecord(result(t, genuine, answered(genuine, 201, orderResp("ord_real"))))

	// C6: the genuine record, customerId and requestId rewritten, signature
	// headers kept.
	forged := genuine
	forged.RequestID = wire.NewRequestID()
	forged.Body = wire.NewBody([]byte(strings.Replace(orderReq, "c-42", "mallory", 1)))
	rewritten := f.resultRecord(result(t, forged, answered(forged, 201, orderResp("ord_mallory"))))
	rewritten.Headers = genuineRec.Headers

	// Signed, but by a bridge key nobody trusts.
	rogue, _ := bridgeSigner(t, "rogue-bridge")
	minted := createOrder(t, f.name, orderReq)
	mintedRec, err := wire.EncodeResult(result(t, minted, answered(minted, 201, orderResp("ord_rogue"))), rogue)
	if err != nil {
		t.Fatal(err)
	}

	// No signature at all.
	unsigned := createOrder(t, f.name, orderReq)
	unsignedRec := f.resultRecord(result(t, unsigned, answered(unsigned, 201, orderResp("ord_unsigned"))))
	unsignedRec.Headers = slices.DeleteFunc(slices.Clone(unsignedRec.Headers), func(h kgo.RecordHeader) bool {
		return h.Key == wire.HeaderSig
	})

	// A genuine result of another service, replayed onto ours.
	other := kafkatest.Service(t, "orders")
	moved := createOrder(t, other, orderReq)
	movedRec := f.resultRecord(result(t, moved, answered(moved, 201, orderResp("ord_moved"))))
	movedRec.Topic = wire.ResultTopic(f.name)

	forgeries := []string{forged.RequestID, minted.RequestID, unsigned.RequestID, moved.RequestID}
	f.produce(rewritten, mintedRec, unsignedRec, movedRec, genuineRec)
	want := map[string]string{genuine.RequestID: "ord_real"}
	for id, ord := range f.created(2) { // after the forgeries: not blocked
		want[id] = ord
	}

	r := start(f.deriver("d1"))
	f.caughtUp(time.Minute)
	r.stop(t)

	for _, ev := range f.checkEvents(want) {
		if slices.Contains(forgeries, header(ev, "ce_id")) || bytes.Contains(ev.Value, []byte("mallory")) {
			t.Fatalf("event derived from a forged result: %s", ev.Value)
		}
	}
	fails := drain(t, events.FailureTopic(f.name), kgo.ReadCommitted())
	if len(fails) != len(forgeries) {
		t.Fatalf("%d failure records, want %d", len(fails), len(forgeries))
	}
	for i, r := range fails {
		var fl events.Failure
		if err := json.Unmarshal(r.Value, &fl); err != nil {
			t.Fatal(err)
		}
		if fl.Reason != events.ReasonNotAuthentic || fl.RequestID != "" || fl.Result.Topic != wire.ResultTopic(f.name) {
			t.Errorf("failure %d: %+v", i, fl)
		}
		t.Logf("failure record: %s", r.Value)
	}
}

// Without bridge keys there is nothing to tell a real Result from a forged
// one: the deriver refuses to exist. Gateway keys are not a substitute.
func TestDeriverRequiresBridgeKeys(t *testing.T) {
	svc := load(t, "orders", ordersSpec("orders"))
	signing, _, err := identity.Generate("gw")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(identity.RoleGateway), signing)
	gw, err := identity.SignerFromEnv(identity.RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	for name, keys := range map[string]identity.TrustedKeys{"none": {}, "gateway keys": gw.Self()} {
		if _, err := events.New(events.Config{Brokers: []string{"localhost:1"}, Service: svc, Instance: "d1", BridgeKeys: keys}); err == nil {
			t.Errorf("%s: deriver created", name)
		}
	}
}
