package kafkatest_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"net/http"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMain(m *testing.M) { kafkatest.Main(m) }

func signerAndRing(t *testing.T, role identity.Role) (*identity.Signer, identity.TrustedKeys) {
	signing, trusted, err := identity.Generate(string(role) + "-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(role), signing)
	t.Setenv(identity.TrustedKeysEnv(role), trusted)
	s, err := identity.SignerFromEnv(role)
	if err != nil {
		t.Fatal(err)
	}
	k, err := identity.TrustedKeysFromEnv(role)
	if err != nil {
		t.Fatal(err)
	}
	return s, k
}

// The full contract over a real broker: signed command → verified by the
// "bridge" side → response on the gateway's reply topic → result with the
// largest bodies the contract allows.
func TestContractOverRealKafka(t *testing.T) {
	svc := kafkatest.Service(t, "orders")
	gw := kafkatest.Service(t, "gw")
	kafkatest.CreateTopics(t, append(kafkaenv.ServiceTopics(svc, 3), kafkaenv.ReplyTopic(gw))...)
	signer, ring := signerAndRing(t, identity.RoleGateway)
	bridge, bridgeRing := signerAndRing(t, identity.RoleBridge)
	producer := kafkatest.Client(t)
	ctx := context.Background()

	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("Authorization", "Bearer must-not-leak")
	now := time.Now().UTC()
	cmd := wire.Command{
		V: wire.Version, RequestID: wire.NewRequestID(), Service: svc,
		OperationID: "createOrder", Method: "POST", PathTemplate: "/orders", Path: "/orders",
		Caller:  wire.Caller{Application: "checkout", Instance: "checkout-1"},
		Headers: wire.RequestHeaders(h), Body: wire.NewBody([]byte(`{"customerId":"c1"}`)),
		IdempotencyKey: "k-1", ReplyTo: wire.ReplyTopic(gw),
		IssuedAt: now, Deadline: now.Add(5 * time.Second), ExpiresAt: now.Add(time.Hour),
	}
	rec, err := wire.EncodeCommand(cmd, signer)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}

	got := kafkatest.Consume(t, wire.CommandTopic(svc), 30*time.Second, func(r []*kgo.Record) bool { return len(r) == 1 })
	if bytes.Contains(got[0].Value, []byte("must-not-leak")) {
		t.Fatal("Authorization reached Kafka")
	}
	verified, err := wire.CommandVerifier{Keys: ring, Service: svc}.Verify(got[0])
	if err != nil {
		t.Fatalf("verify after a real round trip: %v", err)
	}

	resp := wire.Response{V: wire.Version, RequestID: verified.RequestID, Status: 201,
		Headers: wire.Headers{"content-type": {"application/json"}, "location": {"/orders/1"}},
		Body:    wire.NewBody([]byte(`{"id":"1"}`))}
	rr, err := wire.EncodeResponse(resp, verified.ReplyTo, bridge)
	if err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, rr).FirstErr(); err != nil {
		t.Fatal(err)
	}
	replies := kafkatest.Consume(t, wire.ReplyTopic(gw), 30*time.Second, func(r []*kgo.Record) bool { return len(r) == 1 })
	back, err := wire.DecodeResponse(replies[0], bridgeRing)
	if err != nil || back.Status != 201 || back.Headers.Get("location") != "/orders/1" {
		t.Fatalf("%v %+v", err, back)
	}

	// Worst case size: incompressible, non-JSON bodies at the limit on both
	// sides, so both are base64 inside the result.
	big := func() wire.Body {
		b := make([]byte, wire.MaxBodyBytes)
		_, _ = rand.Read(b)
		return wire.NewBody(b)
	}
	bigCmd := verified
	bigCmd.Body = big()
	bigResp := resp
	bigResp.Body = big()
	res, err := wire.NewResult(bigCmd, bigResp, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	resRec, err := wire.EncodeResult(res, bridge)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("worst-case result record: %d bytes (limit %d)", len(resRec.Value), kafkaenv.MaxMessageBytes)
	if err := producer.ProduceSync(ctx, resRec).FirstErr(); err != nil {
		t.Fatalf("worst-case result rejected by broker: %v", err)
	}
	results := kafkatest.Consume(t, wire.ResultTopic(svc), 30*time.Second, func(r []*kgo.Record) bool { return len(r) == 1 })
	decoded, err := wire.DecodeResult(results[0], bridgeRing)
	if err != nil || !bytes.Equal(decoded.Response.Body.Bytes(), bigResp.Body.Bytes()) {
		t.Fatalf("result round trip: %v", err)
	}
	if results[0].Partition != got[0].Partition {
		t.Fatalf("result partition %d != command partition %d", results[0].Partition, got[0].Partition)
	}
}

func TestEndOffsetsSeesWrites(t *testing.T) {
	svc := kafkatest.Service(t, "svc")
	kafkatest.CreateTopics(t, kafkaenv.ServiceTopics(svc, 1)...)
	before := kafkatest.EndOffsets(t, wire.ResultTopic(svc))
	if err := kafkatest.Client(t).ProduceSync(context.Background(), &kgo.Record{Topic: wire.ResultTopic(svc), Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if after := kafkatest.EndOffsets(t, wire.ResultTopic(svc)); after != before+1 {
		t.Fatalf("before %d after %d", before, after)
	}
}

// Restarted binaries and concurrent instances provision the same topics.
func TestEnsureTopicsIsIdempotent(t *testing.T) {
	topics := kafkaenv.ServiceTopics(kafkatest.Service(t, "svc"), 2)
	kafkatest.CreateTopics(t, topics...)
	kafkatest.CreateTopics(t, topics...)

	// Tolerating "already exists" must not swallow real failures.
	bad := "bogus"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := kafkaenv.EnsureTopics(ctx, kafkatest.Client(t), kafkaenv.Topic{
		Name: kafkatest.Service(t, "svc"), Partitions: 1, Configs: map[string]*string{"retention.ms": &bad},
	})
	if err == nil {
		t.Fatal("invalid topic config accepted")
	}
}

// Readiness checks the actual layout; it must not turn provisioning into
// partition reconciliation for topics that already exist.
func TestEnsureTopicsPreservesExistingLayout(t *testing.T) {
	topic := kafkaenv.CommandTopic(kafkatest.Service(t, "existing"), 1)
	kafkatest.CreateTopics(t, topic)
	topic.Partitions = 3
	kafkatest.CreateTopics(t, topic)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	details, err := kafkatest.Admin(t).ListTopics(ctx, topic.Name)
	if err != nil {
		t.Fatal(err)
	}
	if actual, ok := details[topic.Name]; !ok || actual.Err != nil || len(actual.Partitions) != 1 {
		t.Fatalf("existing layout changed or is not ready: %+v", actual)
	}
}

// A failed metadata lookup may be cached longer than a startup attempt.
// Provisioning must refresh it, and the same client must immediately be
// able to query offsets after creation, without resetting its other state.
func TestEnsureTopicsRefreshesCachedMetadata(t *testing.T) {
	cl := kafkatest.Client(t, kgo.MetadataMinAge(time.Minute))
	adm := kadm.NewClient(cl)
	topic := kafkaenv.CommandTopic(kafkatest.Service(t, "fresh"), 1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	details, err := adm.ListTopics(ctx, topic.Name)
	if err != nil || details[topic.Name].Err != kerr.UnknownTopicOrPartition {
		t.Fatalf("expected cached negative lookup: details=%+v error=%v", details, err)
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, 3*time.Second)
	defer readyCancel()
	if err := kafkaenv.EnsureTopics(readyCtx, cl, topic); err != nil {
		t.Fatal(err)
	}
	ends, err := adm.ListEndOffsets(readyCtx, topic.Name)
	if err != nil {
		t.Fatal(err)
	}
	if end, ok := ends.Lookup(topic.Name, 0); !ok || end.Err != nil || end.Offset != 0 {
		t.Fatalf("provisioning left stale metadata for offset reads: %+v", ends)
	}
}
