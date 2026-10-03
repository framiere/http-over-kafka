package audit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/audit"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkatest"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) { kafkatest.Main(m) }

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// The secret values below travel in places a careless audit would print:
// the body, the query string, the idempotency key, B's Set-Cookie, and an
// Authorization header the gateway (wire.RequestHeaders) must have dropped.
const secret = "S3CR3T"

func command(t *testing.T, service string) wire.Command {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Authorization", "Bearer "+secret)
	now := time.Now().UTC()
	return wire.Command{
		V: wire.Version, RequestID: wire.NewRequestID(), Service: service,
		OperationID: "createOrder", Method: "POST", PathTemplate: "/orders", Path: "/orders",
		RawQuery: "token=" + secret,
		Caller:   wire.Caller{Application: "checkout", Instance: "checkout-prod-1"},
		Headers:  wire.RequestHeaders(h), Body: wire.NewBody([]byte(`{"card":"` + secret + `"}`)),
		IdempotencyKey: "idem-" + secret, ReplyTo: wire.ReplyTopic("gw-1"),
		IssuedAt: now, Deadline: now.Add(5 * time.Second), ExpiresAt: now.Add(time.Minute),
	}
}

func TestAuditReadsMutationStream(t *testing.T) {
	svc := kafkatest.Service(t, "orders")
	kafkatest.CreateTopics(t, kafkaenv.ServiceTopics(svc, 3)...)
	signer := signerFor(t, identity.RoleGateway)
	bridge := signerFor(t, identity.RoleBridge)

	ok := command(t, svc)
	okRec, err := wire.EncodeCommand(ok, signer)
	if err != nil {
		t.Fatal(err)
	}
	// Someone with write access to the topic impersonates "admin-console".
	forged := command(t, svc)
	forgedRec, err := wire.EncodeCommand(forged, signer)
	if err != nil {
		t.Fatal(err)
	}
	forgedRec.Value = bytes.Replace(forgedRec.Value, []byte(`"checkout"`), []byte(`"admin-console"`), 1)

	created := wire.Response{V: wire.Version, RequestID: ok.RequestID, Status: 201,
		Headers: wire.Headers{"content-type": {"application/json"}, "set-cookie": {"sid=" + secret}},
		Body:    wire.NewBody([]byte(`{"id":"ord_1","card":"` + secret + `"}`))}
	res, err := wire.NewResult(ok, created, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	resRec, err := wire.EncodeResult(res, bridge)
	if err != nil {
		t.Fatal(err)
	}
	failed := command(t, svc)
	failedRes, err := wire.NewResult(failed, wire.FaultResponse(failed.RequestID, wire.FaultOutcomeUnknown, "bridge crashed"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	failedRec, err := wire.EncodeResult(failedRes, bridge)
	if err != nil {
		t.Fatal(err)
	}
	// C6: a genuine result rewritten (new requestId, caller "mallory"),
	// genuine signature headers kept.
	mallory := command(t, svc)
	mallory.Caller.Application = "mallory"
	malloryResp := created
	malloryResp.RequestID = mallory.RequestID
	malloryRes, err := wire.NewResult(mallory, malloryResp, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	malloryRec, err := wire.EncodeResult(malloryRes, bridge)
	if err != nil {
		t.Fatal(err)
	}
	malloryRec.Headers = resRec.Headers
	junk := &kgo.Record{Topic: wire.ResultTopic(svc), Value: []byte(`{"card":"` + secret + `"}`)}

	ctx := context.Background()

	// A zombie bridge's aborted transaction: a Result for a retry of the same
	// idempotency scope (so the same partition, ahead of resRec) that was
	// never recorded. The audit must not report it.
	ghost := ok
	ghost.RequestID = wire.NewRequestID()
	ghostRes, err := wire.NewResult(ghost, wire.Response{V: wire.Version, RequestID: ghost.RequestID, Status: 201,
		Headers: wire.Headers{}, Body: wire.NewBody([]byte(`{"id":"ord_never"}`))}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ghostRec, err := wire.EncodeResult(ghostRes, bridge)
	if err != nil {
		t.Fatal(err)
	}
	zombie := kafkatest.Client(t, kgo.TransactionalID("bridge-zombie-"+svc))
	if err := zombie.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := zombie.ProduceSync(ctx, ghostRec).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := zombie.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}

	if err := kafkatest.Client(t).ProduceSync(ctx, okRec, forgedRec, resRec, failedRec, malloryRec, junk).FirstErr(); err != nil {
		t.Fatal(err)
	}

	var out syncBuffer
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- audit.Run(runCtx, audit.Config{
			Brokers:     kafkatest.Brokers(t),
			Group:       "audit-" + svc,
			Topics:      `^http\.(requests|results)\.` + regexp.QuoteMeta(svc) + `$`,
			GatewayKeys: signer.Self(),
			BridgeKeys:  bridge.Self(),
			Out:         &out,
		})
	}()
	kafkatest.Eventually(t, time.Minute, func() bool { return strings.Count(out.String(), "\n") >= 6 }, "6 audit lines")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Logf("audit output:\n%s", out.String())

	// Same partition, lower offset: reading resRec means the auditor went past it.
	if ghostRec.Partition != resRec.Partition || ghostRec.Offset >= resRec.Offset {
		t.Fatalf("aborted result at %d/%d, committed one at %d/%d: the test proves nothing",
			ghostRec.Partition, ghostRec.Offset, resRec.Partition, resRec.Offset)
	}
	if strings.Contains(out.String(), ghost.RequestID) {
		t.Fatal("audit reports a Result from an aborted transaction")
	}
	if strings.Contains(out.String(), secret) {
		t.Fatal("audit output leaks a secret")
	}
	byKey := map[string]audit.Entry{}
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		var e audit.Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		byKey[e.Kind+"/"+e.RequestID] = e
	}
	if len(byKey) != 6 {
		t.Fatalf("want 6 distinct entries, got %d", len(byKey))
	}
	check := func(key, authenticity string, outcome wire.Outcome, status int) {
		t.Helper()
		e, found := byKey[key]
		switch {
		case !found:
			t.Errorf("%s: missing", key)
		case e.Authenticity != authenticity || (authenticity == audit.NotAuthentic) != (e.AuthError != "") || e.Outcome != outcome || e.Status != status:
			t.Errorf("%s: %+v", key, e)
		case e.Caller == nil || e.Method != "POST" || e.Path != "/orders" || e.OperationID != "createOrder" || !e.Idempotent:
			t.Errorf("%s: who/what missing: %+v", key, e)
		}
	}
	check("requested/"+ok.RequestID, audit.Authentic, "", 0)
	check("requested/"+forged.RequestID, audit.NotAuthentic, "", 0)
	check("completed/"+ok.RequestID, audit.Authentic, wire.OutcomeSucceeded, 201)
	check("completed/"+failed.RequestID, audit.Authentic, wire.OutcomeUnknown, 502)
	check("completed/"+mallory.RequestID, audit.NotAuthentic, wire.OutcomeSucceeded, 201)
	if c := byKey["completed/"+mallory.RequestID].Caller; c == nil || c.Application != "mallory" {
		t.Errorf("forged result must stay visible with its claimed caller: %+v", c)
	}
	if c := byKey["requested/"+forged.RequestID].Caller; c == nil || c.Application != "admin-console" {
		t.Errorf("forged entry must show the claimed caller, flagged: %+v", c)
	}
	if u := byKey["unreadable/"]; u.Topic != wire.ResultTopic(svc) || u.Error == "" || u.Authenticity != audit.NotAuthentic {
		t.Errorf("junk record: %+v", u)
	}

	// Without keys nothing is called authentic; with the wrong role's keys
	// nothing verifies.
	for name, c := range map[string]struct {
		a    audit.Auditor
		want string
	}{
		"no keys":       {audit.Auditor{}, audit.Unverified},
		"swapped roles": {audit.Auditor{GatewayKeys: bridge.Self(), BridgeKeys: signer.Self()}, audit.NotAuthentic},
	} {
		for _, rec := range []*kgo.Record{okRec, resRec} {
			if got := c.a.Entry(rec).Authenticity; got != c.want {
				t.Errorf("%s, %s: %s, want %s", name, typeOf(rec), got, c.want)
			}
		}
	}
}

func signerFor(t *testing.T, role identity.Role) *identity.Signer {
	t.Helper()
	signing, _, err := identity.Generate("audit-test-" + string(role))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(role), signing)
	s, err := identity.SignerFromEnv(role)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func typeOf(r *kgo.Record) string {
	for _, h := range r.Headers {
		if h.Key == wire.HeaderType {
			return string(h.Value)
		}
	}
	return ""
}
