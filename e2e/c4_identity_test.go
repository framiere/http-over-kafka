//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func produce(t *testing.T, recs ...*kgo.Record) {
	t.Helper()
	cl := kclient(t, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func recordBytes(r *kgo.Record) []byte {
	var b bytes.Buffer
	b.Write(r.Key)
	b.Write(r.Value)
	for _, h := range r.Headers {
		b.WriteString(h.Key)
		b.Write(h.Value)
	}
	return b.Bytes()
}

func setHeader(r *kgo.Record, k, v string) {
	for i, h := range r.Headers {
		if h.Key == k {
			r.Headers[i].Value = []byte(v)
			return
		}
	}
	r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
}

func dropHeader(r *kgo.Record, k string) {
	var hs []kgo.RecordHeader
	for _, h := range r.Headers {
		if h.Key != k {
			hs = append(hs, h)
		}
	}
	r.Headers = hs
}

func clone(r *kgo.Record) *kgo.Record {
	c := &kgo.Record{Topic: r.Topic, Partition: r.Partition, Key: bytes.Clone(r.Key), Value: bytes.Clone(r.Value)}
	for _, h := range r.Headers {
		c.Headers = append(c.Headers, kgo.RecordHeader{Key: h.Key, Value: bytes.Clone(h.Value)})
	}
	return c
}

// Criterion 4a: no caller secret in any topic the system writes. Run twice:
// with a contract that declares no credential location, and with one that
// declares X-Auth-Token and access_token as B's credentials (D12).
func TestC4_NoSecretInKafka(t *testing.T) {
	for _, kind := range []string{"echo", "echo-secured"} {
		t.Run(kind, func(t *testing.T) { noSecretInKafka(t, kind) })
	}
}

func noSecretInKafka(t *testing.T, kind string) {
	spec := t.TempDir()
	e := startEchoSpec(t, spec, kind)
	g := newGateway(t, spec, 10*time.Second, e).startReady(t)
	startBridgeReady(t, newBridge(t, spec, e, "b1"))
	a := newCaller(t, "checkout")

	r := req{method: "POST", path: "/echo/1?access_token=SECRET-QUERY", body: `{"a":1}`, header: map[string]string{
		"Cookie": "sid=SECRET-COOKIE", "X-Api-Key": "SECRET-APIKEY", "Proxy-Authorization": "Basic SECRET-PROXY",
		"X-Auth-Token": "SECRET-XAUTH",
	}}
	if got := a.via(g, e.name, r); got.status != 200 {
		t.Fatalf("%s", got)
	}
	// Lower-case and odd spellings of the header, sent raw by a client that
	// does not canonicalize.
	if got := a.via(g, e.name, req{method: "PUT", path: "/echo/2", body: `{}`, header: map[string]string{"authorization": "Bearer " + a.token}}); got.status != 200 {
		t.Fatalf("%s", got)
	}
	jwtSig := a.token[strings.LastIndex(a.token, ".")+1:]
	needles := map[string]string{
		"bearer JWT (signature part)": jwtSig,
		"Cookie":                      "SECRET-COOKIE",
		"X-Api-Key":                   "SECRET-APIKEY",
		"Proxy-Authorization":         "SECRET-PROXY",
		"X-Auth-Token (not a standard secret header)": "SECRET-XAUTH",
		"token in query string":                       "SECRET-QUERY",
	}
	topics := []string{"http.requests." + e.name, "http.results." + e.name, "http.bridge-state." + e.name, "http.responses." + g.instance}
	found := map[string][]string{}
	for _, tp := range topics {
		for _, rec := range readTopic(t, tp, false) {
			raw := recordBytes(rec)
			for what, n := range needles {
				if bytes.Contains(raw, []byte(n)) {
					found[what] = append(found[what], tp)
				}
			}
			if bytes.Contains(bytes.ToLower(rec.Value), []byte(`"authorization"`)) {
				found["authorization header name"] = append(found["authorization header name"], tp)
			}
		}
	}
	for what := range needles {
		t.Logf("%-45s in Kafka: %v", what, uniq(found[what]))
	}
	must := []string{"bearer JWT (signature part)", "Cookie", "X-Api-Key", "Proxy-Authorization", "authorization header name"}
	if kind == "echo-secured" {
		must = append(must, "X-Auth-Token (not a standard secret header)", "token in query string")
	}
	for _, what := range must {
		if len(found[what]) > 0 {
			t.Errorf("caller secret %s found in %v", what, uniq(found[what]))
		}
	}
}

func uniq(s []string) []string {
	m := map[string]bool{}
	var out []string
	for _, v := range s {
		if !m[v] {
			m[v] = true
			out = append(out, v)
		}
	}
	return out
}

// Criterion 4b: an attacker with write access to the command topic, holding
// genuine signed commands, tries to make B act. Ground truth: debits.
func TestC4_ForgedCommandsRejected(t *testing.T) {
	spec := t.TempDir()
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 10*time.Second, pay).startReady(t)
	b := newBridge(t, spec, pay, "b1")
	startBridgeReady(t, b)
	a := newCaller(t, "checkout")

	victim := randName("acct-")
	// Two genuine commands to steal: one keyed, one not.
	if got := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(victim), header: map[string]string{"Idempotency-Key": "k-" + victim}}); got.status != 201 {
		t.Fatal(got)
	}
	if got := a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge(victim)}); got.status != 201 {
		t.Fatal(got)
	}
	eventually(t, 5*time.Second, "2 genuine debits", func() bool { return debits(t, pay, victim) == 2 })
	var keyed, plain *kgo.Record
	for _, r := range readTopic(t, "http.requests."+pay.name, true) {
		if bytes.HasPrefix(r.Key, []byte("i:")) {
			keyed = r
		} else {
			plain = r
		}
	}
	if keyed == nil || plain == nil {
		t.Fatal("genuine commands not found")
	}
	thief := randName("thief-")
	retarget := func(r *kgo.Record) *kgo.Record {
		c := clone(r)
		c.Value = bytes.ReplaceAll(c.Value, []byte(victim), []byte(thief))
		return c
	}

	attacks := map[string]*kgo.Record{}
	// 1. Altered body (account) and caller, signature kept.
	alt := retarget(plain)
	alt.Value = bytes.Replace(alt.Value, []byte(`"application":"checkout"`), []byte(`"application":"admin"`), 1)
	attacks["altered body+caller, original signature"] = alt
	// 2. Signature removed.
	uns := retarget(plain)
	dropHeader(uns, "hok-sig")
	attacks["unsigned"] = uns
	// 3. Signed with the attacker's own key under the trusted kid.
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	own := retarget(plain)
	msg := append([]byte("http-over-kafka/command/v1\x00"), own.Value...) // identity.signedBytes layout
	setHeader(own, "hok-sig", base64.StdEncoding.EncodeToString(ed25519.Sign(priv, msg)))
	attacks["signed by attacker key, trusted kid"] = own
	// 4. Unknown kid.
	kid := retarget(plain)
	setHeader(kid, "hok-kid", "attacker-1")
	attacks["unknown kid"] = kid
	// 5. A genuine command copied to another partition (a second consumer
	// would own it and run it in parallel with the original's retries).
	moved := clone(plain)
	moved.Partition = (plain.Partition + 1) % partitions
	attacks["genuine command on another partition"] = moved

	m := b.mark()
	for _, r := range attacks {
		produce(t, r)
	}
	// 6. Exact replays of genuine commands, same partition.
	produce(t, clone(plain), clone(keyed))

	eventually(t, 15*time.Second, "bridge processed the forgeries", func() bool {
		return b.countSince(m, "not authentic") >= 4 && b.countSince(m, "wrong partition") >= 1
	})
	stable(t, 3*time.Second, "no debit for the thief, no extra debit for the victim", func() bool {
		return debits(t, pay, thief) == 0 && debits(t, pay, victim) == 2
	})
	t.Logf("bridge logs: %d not-authentic drops, %d wrong-partition drops; debits thief=%d victim=%d",
		b.countSince(m, "not authentic"), b.countSince(m, "wrong partition"), debits(t, pay, thief), debits(t, pay, victim))
	for name := range attacks {
		t.Logf("rejected: %s", name)
	}
	t.Logf("rejected: exact replay of 2 genuine signed commands (dedup by requestId)")
}
