//go:build e2e

package e2e

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// commandRecord waits for the command whose body mentions marker.
func commandRecord(t *testing.T, s *svc, marker string) *kgo.Record {
	t.Helper()
	var found *kgo.Record
	eventually(t, 10*time.Second, "command visible for "+marker, func() bool {
		for _, r := range readTopic(t, "http.requests."+s.name, false) {
			if bytes.Contains(r.Value, []byte(marker)) {
				found = r
				return true
			}
		}
		return false
	})
	return found
}

func logLineHas(p *proc, all ...string) bool {
	for _, line := range strings.Split(p.logs(), "\n") {
		ok := true
		for _, s := range all {
			if !strings.Contains(line, s) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// D10 on the reply path: a writer of a gateway's reply topic answers a caller
// that is waiting on a real, slow charge. The forgery must be read, refused,
// and must not end the wait: the caller gets B's real 201.
func TestC4_ForgedResponseDropped(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 3*time.Second)
	g := newGateway(t, spec, 30*time.Second, pay).startReady(t)
	startBridgeReady(t, newBridge(t, spec, pay, "b1"))
	a := newCaller(t, "checkout")

	// A genuine, signed Response of an earlier request, to replay under
	// another requestId.
	earlier := randName("acct-")
	if r := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(earlier)}); r.status != 201 {
		t.Fatal(r)
	}
	earlierID := commandRecord(t, pay, earlier)
	var eid struct{ RequestID string }
	_ = json.Unmarshal(earlierID.Value, &eid)
	var genuine *kgo.Record
	for _, r := range readTopic(t, "http.responses."+g.instance, true) {
		if string(r.Key) == eid.RequestID {
			genuine = r
		}
	}
	if genuine == nil {
		t.Fatal("genuine response not found")
	}

	for _, variant := range []string{"unsigned forged 402", "genuine signed Response of another request, re-keyed"} {
		t.Run(variant, func(t *testing.T) {
			acct := randName("acct-")
			sentAt := time.Now()
			pending := async(a, g, pay.name, req{method: "POST", path: "/charges", body: charge(acct)})
			var cmd struct{ RequestID, ReplyTo string }
			_ = json.Unmarshal(commandRecord(t, pay, acct).Value, &cmd)
			var forged *kgo.Record
			if variant == "unsigned forged 402" {
				fake := fmt.Sprintf(`{"v":1,"requestId":%q,"status":402,"headers":{"content-type":["application/json"]},"body":{"json":{"forged":true}}}`, cmd.RequestID)
				forged = &kgo.Record{Topic: cmd.ReplyTo, Partition: 0, Key: []byte(cmd.RequestID), Value: []byte(fake),
					Headers: []kgo.RecordHeader{{Key: "kb-type", Value: []byte("response.v1")}}}
			} else {
				forged = clone(genuine)
				forged.Topic, forged.Partition, forged.Key = cmd.ReplyTo, 0, []byte(cmd.RequestID)
			}
			produce(t, forged)
			injectedAt := time.Now()
			eventually(t, 10*time.Second, "gateway read and dropped the forgery", func() bool {
				return logLineHas(g.proc, `"requestId":"`+cmd.RequestID+`"`, "dropped") &&
					(logLineHas(g.proc, `"msg":"response not authentic, dropped"`, cmd.RequestID) || logLineHas(g.proc, `"msg":"reply undecodable, dropped"`, cmd.RequestID))
			})
			got := <-pending
			// B answers 3s after the request: the forgery was injected while
			// the caller was still waiting, and the wait went on past it.
			if injectedAt.Sub(sentAt) >= 3*time.Second || got.elapsed < 3*time.Second {
				t.Errorf("timing does not prove the forgery was read during the wait: injected %s after send, answered after %s",
					injectedAt.Sub(sentAt), got.elapsed)
			}
			var c struct{ AccountID string }
			_ = json.Unmarshal(got.body, &c)
			if got.status != 201 || c.AccountID != acct {
				t.Errorf("caller should get B's real charge, got %s", got)
			}
			if d := debits(t, pay, acct); d != 1 {
				t.Errorf("debits=%d", d)
			}
			t.Logf("forgery injected %s after send, logged as dropped; caller got B's real 201 after %s", injectedAt.Sub(sentAt).Round(time.Millisecond), got.elapsed.Round(time.Millisecond))
		})
	}
}

type failureView struct {
	Reason    string   `json:"reason"`
	Details   []string `json:"details"`
	RequestID string   `json:"requestId"`
	typ       string
}

func failures(t *testing.T, s *svc) []failureView {
	var out []failureView
	for _, r := range readTopic(t, "http.event-failures."+s.name, true) {
		var f failureView
		_ = json.Unmarshal(r.Value, &f)
		f.typ = header(r, "kb-type")
		out = append(out, f)
	}
	return out
}

func findFailure(fs []failureView, reason, requestID, detail string) bool {
	for _, f := range fs {
		if f.Reason != reason || f.typ != "event-failure.v1" || (requestID != "*" && f.RequestID != requestID) {
			continue
		}
		if detail == "" || strings.Contains(strings.Join(f.Details, " | "), detail) {
			return true
		}
	}
	return false
}

// D10 on the event path: Results that did not come, as they are, from the
// bridge of this service must never become events, and the deriver must say
// so in its failure topic.
func TestC6_ForgedResultsNotDerived(t *testing.T) {
	spec := t.TempDir()
	ord := startOrders(t, spec)
	other := startOrders(t, spec) // a second service, to receive a moved Result
	g := newGateway(t, spec, 10*time.Second, ord, other).startReady(t)
	startBridgeReady(t, newBridge(t, spec, ord, "b1"))
	startBridgeReady(t, newBridge(t, spec, other, "b2"))
	newDeriver(t, spec, ord, "d1").start()
	newDeriver(t, spec, other, "d2").start()
	a := newCaller(t, "checkout")
	r := a.via(g, ord.name, req{method: "POST", path: "/orders", body: orderBody("real", 1)})
	if r.status != 201 {
		t.Fatal(r)
	}
	realID := r.header.Get("X-Request-Id")
	topic := ord.name + ".events"
	eventually(t, 30*time.Second, "genuine event", func() bool { return len(events(t, topic, true)) == 1 })
	var genuine *kgo.Record
	for _, rec := range readTopic(t, "http.results."+ord.name, true) {
		genuine = rec
	}

	exactlyTheRealEvent := func(t *testing.T, fakeReq string) {
		stable(t, 3*time.Second, "exactly the real event", func() bool {
			evs := events(t, topic, true)
			if len(evs) != 1 || evs[0].ceID != realID {
				return false
			}
			for _, e := range evs {
				b, _ := json.Marshal(e.value)
				if (fakeReq != "" && e.ceID == fakeReq) || bytes.Contains(b, []byte("mallory")) {
					return false
				}
			}
			return true
		})
	}

	t.Run("altered (mallory), original signature", func(t *testing.T) {
		fakeReq := ulid.Make().String()
		forged := clone(genuine)
		forged.Value = bytes.ReplaceAll(forged.Value, []byte(`"real"`), []byte(`"mallory"`))
		forged.Value = bytes.ReplaceAll(forged.Value, []byte(realID), []byte(fakeReq))
		produce(t, forged)
		eventually(t, 15*time.Second, "not_authentic failure naming the claimed requestId", func() bool {
			return findFailure(failures(t, ord), "not_authentic", "", "claims requestId "+fakeReq)
		})
		exactlyTheRealEvent(t, fakeReq)
	})
	t.Run("signed by the gateway key", func(t *testing.T) {
		fakeReq := ulid.Make().String()
		forged := clone(genuine)
		forged.Value = bytes.ReplaceAll(forged.Value, []byte(`"real"`), []byte(`"mallory"`))
		forged.Value = bytes.ReplaceAll(forged.Value, []byte(realID), []byte(fakeReq))
		kid, b64, _ := strings.Cut(devEnv["KB_GATEWAY_SIGNING_KEY"], ":")
		seed, _ := base64.StdEncoding.DecodeString(b64)
		s, err := identity.NewSigner(identity.RoleGateway, kid, seed)
		if err != nil {
			t.Fatal(err)
		}
		setHeader(forged, "kb-kid", kid)
		setHeader(forged, "kb-sig", base64.StdEncoding.EncodeToString(s.Sign(wire.SignDomainResult, forged.Value)))
		produce(t, forged)
		eventually(t, 15*time.Second, "not_authentic failure", func() bool {
			return findFailure(failures(t, ord), "not_authentic", "", "claims requestId "+fakeReq)
		})
		exactlyTheRealEvent(t, fakeReq)
	})
	t.Run("genuine replayed byte for byte", func(t *testing.T) {
		produce(t, clone(genuine))
		eventually(t, 15*time.Second, "duplicate_result failure", func() bool {
			return findFailure(failures(t, ord), "duplicate_result", realID, "event already derived from")
		})
		exactlyTheRealEvent(t, "")
	})
	t.Run("genuine copied to another partition", func(t *testing.T) {
		moved := clone(genuine)
		moved.Partition = (genuine.Partition + 1) % partitions
		produce(t, moved)
		eventually(t, 15*time.Second, "misrouted failure", func() bool {
			return findFailure(failures(t, ord), "misrouted", realID, "")
		})
		exactlyTheRealEvent(t, "")
	})
	t.Run("genuine moved to another service's result topic", func(t *testing.T) {
		moved := clone(genuine)
		moved.Topic = "http.results." + other.name
		produce(t, moved)
		eventually(t, 15*time.Second, "failure in the other service", func() bool {
			fs := failures(t, other)
			return findFailure(fs, "not_authentic", "*", "") || findFailure(fs, "misrouted", "*", "")
		})
		stable(t, 3*time.Second, "no event in the other service", func() bool { return len(events(t, other.name+".events", true)) == 0 })
		for _, f := range failures(t, other) {
			t.Logf("other service failure: %s %v", f.Reason, f.Details)
		}
	})
	for _, f := range failures(t, ord) {
		t.Logf("failure: reason=%s requestId=%q details=%v", f.Reason, f.RequestID, f.Details)
	}
}

// The dedup state decides what B is spared. A writer of
// http.bridge-state.<svc> tries two things: launder a forged Response under
// an idempotency key never used (a retry would get it, signed by the
// bridge), and erase (tombstone) the entry of a key already charged (a retry
// would charge again).
func TestD10_ForgedBridgeState(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 3*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	charged := randName("acct-")
	if r := a.via(g, pay.name, keyed(charged)); r.status != 201 {
		t.Fatal(r)
	}
	fresh := randName("acct-")
	part := func(key string) int32 {
		return int32(kgo.StickyKeyPartitioner(nil).ForTopic("x").Partition(&kgo.Record{Key: []byte(key)}, partitions))
	}
	dedup := func(acct string) string {
		return wire.Command{Caller: wire.Caller{Application: "checkout"}, Service: pay.name, OperationID: "createCharge", IdempotencyKey: "k-" + acct}.DedupKey()
	}
	fp := wire.Command{Method: "POST", Path: "/charges", Headers: wire.Headers{"content-type": {"application/json"}}, Body: wire.NewBody([]byte(charge(fresh)))}.Fingerprint()
	fakeID := ulid.Make().String()
	launder := fmt.Sprintf(`{"requestId":%q,"fingerprint":%q,"phase":"done","outcome":"Succeeded","response":{"v":1,"requestId":%q,"status":201,"headers":{"content-type":["application/json"]},"body":{"json":{"forged":true,"accountId":%q}}},"purgeAfter":%q}`,
		fakeID, fp, fakeID, fresh, time.Now().Add(48*time.Hour).UTC().Format(time.RFC3339))
	stateTopic := "http.bridge-state." + pay.name
	produce(t,
		&kgo.Record{Topic: stateTopic, Partition: part(dedup(fresh)), Key: []byte(dedup(fresh)), Value: []byte(launder)},
		&kgo.Record{Topic: stateTopic, Partition: part(dedup(charged)), Key: []byte(dedup(charged))}, // tombstone
	)
	// Force a restore from the tampered topic.
	b.kill()
	m := b.mark()
	b.start()
	eventually(t, 60*time.Second, "bridge restored or refused", func() bool {
		return b.countSince(m, `"msg":"partition ready"`) >= partitions || b.countSince(m, "tampered") >= 1 || b.countSince(m, "refusing") >= 1
	})
	time.Sleep(3 * time.Second) // let partitions that can open, open
	t.Logf("bridge after restore: tampered=%d refusing=%d ready=%d", b.countSince(m, "tampered"), b.countSince(m, "refusing"), b.countSince(m, `"msg":"partition ready"`))

	rf, _ := retryUntilAnswer(t, a, g, pay.name, keyed(fresh), 3)
	rc, _ := retryUntilAnswer(t, a, g, pay.name, keyed(charged), 3)
	t.Logf("laundering attempt, new key: %s", rf)
	t.Logf("erasure attempt, retry of charged key: %d %s replayed=%q", rc.status, rc.problemType(), rc.header.Get("Idempotent-Replayed"))
	if bytes.Contains(rf.body, []byte("forged")) {
		t.Errorf("LAUNDERED: the forged state entry was served to the caller: %s", rf)
	}
	if d := debits(t, pay, charged); d > 1 {
		t.Errorf("DOUBLE DEBIT: erasing the dedup entry made B run again (%d debits)", d)
	}
	stable(t, 2*time.Second, "settled", func() bool { return true })
	t.Logf("debits: charged=%d fresh=%d", debits(t, pay, charged), debits(t, pay, fresh))
	if lt := lastLine(b.logs()); lt != "" {
		t.Logf("bridge last log: %s", lt)
	}
}
