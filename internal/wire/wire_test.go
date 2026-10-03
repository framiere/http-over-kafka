package wire_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBodyRoundTripIsByteExact(t *testing.T) {
	cases := map[string]struct {
		in   []byte
		form string // "json", "base64", "null"
	}{
		"empty":               {nil, "null"},
		"compact json":        {[]byte(`{"sku":"A1","q":2}`), "json"},
		"pretty json":         {[]byte("{\n  \"sku\": \"A1\"\n}"), "base64"},
		"html chars":          {[]byte(`{"q":"a<b&c"}`), "base64"},
		"escaped unicode":     {[]byte(`{"q":"é"}`), "json"},
		"line separator":      {[]byte("{\"q\":\"\u2028\"}"), "base64"},
		"binary":              {[]byte{0, 1, 2, 0xff}, "base64"},
		"invalid utf8 string": {[]byte("\"\xff\""), "base64"},
		"json scalar":         {[]byte(`42`), "json"},
		"form":                {[]byte("a=1&b=2"), "base64"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// json.Marshal (HTML escaping on) is the hostile encoder here.
			out, err := json.Marshal(struct {
				B wire.Body `json:"b"`
			}{wire.NewBody(c.in)})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out), `"b":`+map[string]string{"json": `{"json"`, "base64": `{"base64"`, "null": "null"}[c.form]) {
				t.Fatalf("form %s expected, got %s", c.form, out)
			}
			var back struct {
				B wire.Body `json:"b"`
			}
			if err := json.Unmarshal(out, &back); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(back.B.Bytes(), c.in) {
				t.Fatalf("round trip changed bytes: %q -> %q", c.in, back.B.Bytes())
			}
		})
	}
}

func TestBodyRejectsAmbiguousWireForms(t *testing.T) {
	for _, in := range []string{`{}`, `{"json":1,"base64":"AA=="}`, `{"other":1}`, `{"base64":"!!"}`} {
		var b wire.Body
		if err := json.Unmarshal([]byte(in), &b); err == nil {
			t.Errorf("%s accepted", in)
		}
	}
}

func TestRequestHeadersDropSecretsAndHopByHop(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer secret")
	h.Set("Cookie", "session=1")
	h.Set("Connection", "keep-alive, X-Custom-Hop")
	h.Set("X-Custom-Hop", "1")
	h.Set("Transfer-Encoding", "chunked")
	h.Set("Content-Type", "application/json")
	h.Add("Accept", "application/json")
	h.Add("Accept", "text/plain")

	got := wire.RequestHeaders(h)
	for _, gone := range []string{"authorization", "cookie", "connection", "x-custom-hop", "transfer-encoding"} {
		if _, ok := got[gone]; ok {
			t.Errorf("%s transported", gone)
		}
	}
	if v := got["accept"]; len(v) != 2 || v[0] != "application/json" || v[1] != "text/plain" {
		t.Errorf("multi-valued header order lost: %v", v)
	}
	if got.HTTP().Get("Content-Type") != "application/json" {
		t.Error("content-type lost")
	}

	resp := http.Header{}
	resp.Add("Set-Cookie", "a=1")
	resp.Add("Set-Cookie", "b=2")
	if v := wire.ResponseHeaders(resp)["set-cookie"]; len(v) != 2 {
		t.Errorf("response Set-Cookie must be kept, got %v", v)
	}
}

var t0 = time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

func validCommand() wire.Command {
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	return wire.Command{
		V:            wire.Version,
		RequestID:    wire.NewRequestID(),
		Service:      "orders",
		OperationID:  "createOrder",
		Method:       http.MethodPost,
		PathTemplate: "/orders",
		Path:         "/orders",
		Caller:       wire.Caller{Application: "checkout", Instance: "checkout-1"},
		Headers:      wire.RequestHeaders(h),
		Body:         wire.NewBody([]byte(`{"customerId":"c1","items":[{"sku":"A","quantity":1}]}`)),
		ReplyTo:      wire.ReplyTopic("gw-1"),
		IssuedAt:     t0,
		Deadline:     t0.Add(10 * time.Second),
		ExpiresAt:    t0.Add(time.Hour),
	}
}

func TestCommandValidateRejectsSecretsAndBadShapes(t *testing.T) {
	mutate := map[string]func(*wire.Command){
		"authorization header": func(c *wire.Command) { c.Headers["authorization"] = []string{"Bearer x"} },
		"uppercase header":     func(c *wire.Command) { c.Headers["X-Foo"] = []string{"1"} },
		"crlf in header":       func(c *wire.Command) { c.Headers["x-foo"] = []string{"a\r\nb: c"} },
		"GET":                  func(c *wire.Command) { c.Method = http.MethodGet },
		"lowercase method":     func(c *wire.Command) { c.Method = "post" },
		"request id":           func(c *wire.Command) { c.RequestID = "abc" },
		"no caller":            func(c *wire.Command) { c.Caller = wire.Caller{} },
		"reply topic":          func(c *wire.Command) { c.ReplyTo = "http.requests.orders" },
		"deadline after exp":   func(c *wire.Command) { c.Deadline = c.ExpiresAt },
		"service name":         func(c *wire.Command) { c.Service = "Orders" },
		"idempotency key":      func(c *wire.Command) { c.IdempotencyKey = "a\nb" },
		"version":              func(c *wire.Command) { c.V = 2 },
	}
	if err := validCommand().Validate(); err != nil {
		t.Fatalf("baseline invalid: %v", err)
	}
	for name, m := range mutate {
		c := validCommand()
		m(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestDedupKeyScope(t *testing.T) {
	a, b := validCommand(), validCommand()
	if a.DedupKey() == b.DedupKey() {
		t.Fatal("without idempotency key, distinct requests must not share a dedup key")
	}
	a.IdempotencyKey, b.IdempotencyKey = "k1", "k1"
	if a.DedupKey() != b.DedupKey() {
		t.Fatal("same caller, operation and key must share a dedup key")
	}
	b.Caller.Application = "other"
	if a.DedupKey() == b.DedupKey() {
		t.Fatal("idempotency keys are scoped per caller application")
	}
	b = a
	b.Caller.Instance = "checkout-2"
	if a.DedupKey() != b.DedupKey() {
		t.Fatal("a retry from another instance of the same application is the same scope")
	}
	b = a
	b.OperationID = "replaceOrder"
	if a.DedupKey() == b.DedupKey() {
		t.Fatal("idempotency keys are scoped per operation")
	}
}

func TestFingerprintDetectsBodyChangeOnly(t *testing.T) {
	a, b := validCommand(), validCommand()
	b.Headers["user-agent"] = []string{"retry/2"}
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("incidental headers must not change the fingerprint")
	}
	b.Body = wire.NewBody([]byte(`{"customerId":"c2","items":[{"sku":"A","quantity":1}]}`))
	if a.Fingerprint() == b.Fingerprint() {
		t.Fatal("body change must change the fingerprint")
	}
}

// roleKeys loads a fresh key pair of role through the env, as binaries do.
func roleKeys(t *testing.T, role identity.Role, kid string) (*identity.Signer, identity.TrustedKeys) {
	t.Helper()
	signing, trusted, err := identity.Generate(kid)
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

func keys(t *testing.T, kid string) (*identity.Signer, identity.TrustedKeys) {
	return roleKeys(t, identity.RoleGateway, kid)
}

func verifier(k identity.TrustedKeys, now time.Time) wire.CommandVerifier {
	return wire.CommandVerifier{Keys: k, Service: "orders", Now: func() time.Time { return now }}
}

func TestCommandSignVerify(t *testing.T) {
	signer, ring := keys(t, "gw-a")
	cmd := validCommand()
	rec, err := wire.EncodeCommand(cmd, signer)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Topic != "http.requests.orders" || string(rec.Key) != cmd.DedupKey() {
		t.Fatalf("topic/key: %s %s", rec.Topic, rec.Key)
	}
	if bytes.Contains(rec.Value, []byte("Bearer")) {
		t.Fatal("secret in record")
	}
	got, err := verifier(ring, t0.Add(time.Second)).Verify(rec)
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestID != cmd.RequestID || !bytes.Equal(got.Body.Bytes(), cmd.Body.Bytes()) || !got.IssuedAt.Equal(cmd.IssuedAt) {
		t.Fatalf("decoded command differs: %+v", got)
	}
}

func signB64(s *identity.Signer, value []byte) string {
	return base64.StdEncoding.EncodeToString(s.Sign(wire.SignDomainCommand, value))
}

func clone(r *kgo.Record) *kgo.Record {
	c := *r
	c.Value = bytes.Clone(r.Value)
	c.Headers = append([]kgo.RecordHeader(nil), r.Headers...)
	return &c
}

func setHeader(r *kgo.Record, k, v string) {
	for i := range r.Headers {
		if r.Headers[i].Key == k {
			r.Headers[i].Value = []byte(v)
			return
		}
	}
	r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
}

func TestVerifyRejectsForgeries(t *testing.T) {
	signer, ring := keys(t, "gw-a")
	otherSigner, _ := keys(t, "gw-a") // same kid, different key: an impostor
	rec, err := wire.EncodeCommand(validCommand(), signer)
	if err != nil {
		t.Fatal(err)
	}
	forged, _ := wire.EncodeCommand(validCommand(), otherSigner)

	cases := map[string]*kgo.Record{
		"impostor key": forged,
	}
	r := clone(rec)
	r.Value = bytes.Replace(r.Value, []byte(`"checkout"`), []byte(`"admin-app"`), 1)
	cases["altered caller"] = r
	r = clone(rec)
	r.Headers = r.Headers[:2] // drop signature
	cases["unsigned"] = r
	r = clone(rec)
	setHeader(r, wire.HeaderKeyID, "unknown")
	cases["unknown kid"] = r
	r = clone(rec)
	r.Topic = "http.requests.payments"
	cases["moved to another service topic"] = r
	r = clone(rec)
	r.Key = []byte("r:other")
	cases["rekeyed"] = r
	r = clone(rec)
	setHeader(r, wire.HeaderType, wire.TypeResponse)
	cases["wrong type"] = r

	v := verifier(ring, t0)
	for name, rec := range cases {
		if _, err := v.Verify(rec); !errors.Is(err, wire.ErrNotAuthentic) {
			t.Errorf("%s: want ErrNotAuthentic, got %v", name, err)
		}
	}
}

func TestVerifyRejectsUnknownFieldsEvenWhenSigned(t *testing.T) {
	signer, ring := keys(t, "gw-a")
	rec, err := wire.EncodeCommand(validCommand(), signer)
	if err != nil {
		t.Fatal(err)
	}
	// A newer gateway adding a field without a version bump.
	rec.Value = append(bytes.TrimSuffix(rec.Value, []byte("}")), []byte(`,"skipDedup":true}`)...)
	setHeader(rec, wire.HeaderSig, signB64(signer, rec.Value))
	if _, err := verifier(ring, t0).Verify(rec); !errors.Is(err, wire.ErrNotAuthentic) || !strings.Contains(err.Error(), "skipDedup") {
		t.Fatalf("want rejection naming the field, got %v", err)
	}
}

func TestVerifyStaleCommandsAreAuthenticButNotExecutable(t *testing.T) {
	signer, ring := keys(t, "gw-a")
	cmd := validCommand()
	rec, _ := wire.EncodeCommand(cmd, signer)

	for name, now := range map[string]time.Time{
		"expired":          cmd.ExpiresAt.Add(wire.DefaultClockSkew + time.Second),
		"issued in future": cmd.IssuedAt.Add(-wire.DefaultClockSkew - time.Second),
	} {
		got, err := verifier(ring, now).Verify(rec)
		if !errors.Is(err, wire.ErrStale) {
			t.Errorf("%s: want ErrStale, got %v", name, err)
		}
		if got.RequestID != cmd.RequestID || got.ReplyTo != cmd.ReplyTo {
			t.Errorf("%s: stale command must still be returned so the bridge can reply", name)
		}
	}
	long := validCommand()
	long.ExpiresAt = long.IssuedAt.Add(wire.DefaultMaxTTL + time.Second)
	longRec, _ := wire.EncodeCommand(long, signer)
	if _, err := verifier(ring, t0).Verify(longRec); !errors.Is(err, wire.ErrStale) {
		t.Errorf("lifetime above MaxTTL: want ErrStale, got %v", err)
	}
	for name, now := range map[string]time.Time{
		"within skew after expiry": cmd.ExpiresAt.Add(wire.DefaultClockSkew - time.Second),
		"within skew before issue": cmd.IssuedAt.Add(-wire.DefaultClockSkew + time.Second),
	} {
		if _, err := verifier(ring, now).Verify(rec); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestKeyRotation(t *testing.T) {
	oldS, _, _ := identity.Generate("k-old")
	newS, newP, _ := identity.Generate("k-new")
	_, oldP, _ := identity.Generate("k-old")
	_ = oldS
	ring, err := identity.ParseTrustedKeys(identity.RoleGateway, oldP+","+newP)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(identity.SigningKeyEnv(identity.RoleGateway), newS)
	s, err := identity.SignerFromEnv(identity.RoleGateway)
	if err != nil {
		t.Fatal(err)
	}
	rec, _ := wire.EncodeCommand(validCommand(), s)
	if _, err := verifier(ring, t0).Verify(rec); err != nil {
		t.Fatalf("new key must verify while old is still trusted: %v", err)
	}
	if _, err := identity.ParseKeyring(oldP + "," + oldP); err == nil {
		t.Fatal("duplicate kid accepted")
	}
}

func TestResponseAndFaults(t *testing.T) {
	bridge, bridgeKeys := roleKeys(t, identity.RoleBridge, "br-a")
	id := wire.NewRequestID()
	ok := wire.Response{V: wire.Version, RequestID: id, Status: 201, Headers: wire.Headers{"location": {"/orders/1"}}, Body: wire.NewBody([]byte(`{"id":"1"}`))}
	rec, err := wire.EncodeResponse(ok, wire.ReplyTopic("gw-1"), bridge)
	if err != nil {
		t.Fatal(err)
	}
	back, err := wire.DecodeResponse(rec, bridgeKeys)
	if err != nil || back.Status != 201 || back.Headers.Get("Location") != "/orders/1" {
		t.Fatalf("%v %+v", err, back)
	}
	if _, err := wire.EncodeResponse(ok, "orders.events", bridge); err == nil {
		t.Fatal("reply to a non-reply topic accepted")
	}

	for f, want := range map[wire.Fault]wire.Outcome{
		wire.FaultOutcomeUnknown:       wire.OutcomeUnknown,
		wire.FaultUpstreamUnavailable:  wire.OutcomeNotExecuted,
		wire.FaultIdempotencyKeyReused: wire.OutcomeNotExecuted,
		wire.FaultIdempotencyInFlight:  wire.OutcomeNotExecuted,
		wire.FaultCommandStale:         wire.OutcomeNotExecuted,
		wire.FaultOperationMismatch:    wire.OutcomeNotExecuted,
		wire.FaultSecretInCommand:      wire.OutcomeNotExecuted,
	} {
		r := wire.FaultResponse(id, f, "detail")
		if err := r.Validate(); err != nil {
			t.Errorf("%s: %v", f, err)
		}
		if got := wire.OutcomeOf(r); got != want {
			t.Errorf("%s: outcome %s, want %s", f, got, want)
		}
		var p wire.Problem
		if err := json.Unmarshal(r.Body.Bytes(), &p); err != nil || p.RequestID != id || p.Status != f.Status() {
			t.Errorf("%s: problem body %v %+v", f, err, p)
		}
	}
	if wire.FaultIdempotencyKeyReused.Status() != 422 {
		t.Error("D4: key reuse is 422")
	}

	tooLarge := wire.FaultResponse(id, wire.FaultResponseTooLarge, "")
	if tooLarge.Validate() == nil {
		t.Error("response_too_large without upstreamStatus accepted")
	}
	tooLarge.UpstreamStatus = 201
	if err := tooLarge.Validate(); err != nil || wire.OutcomeOf(tooLarge) != wire.OutcomeSucceeded {
		t.Errorf("B ran and said 201: %v %s", err, wire.OutcomeOf(tooLarge))
	}
	cut := wire.FaultResponse(id, wire.FaultResponseIncomplete, "")
	cut.UpstreamStatus = 400
	if err := cut.Validate(); err != nil || wire.OutcomeOf(cut) != wire.OutcomeFailed || cut.Fault.Execution() != wire.ExecutionDone {
		t.Errorf("cut-off body after B said 400: %v %s", err, wire.OutcomeOf(cut))
	}

	lying := wire.FaultResponse(id, wire.FaultOutcomeUnknown, "")
	lying.Status = 200
	if lying.Validate() == nil {
		t.Error("fault with a non-fault status accepted")
	}
}

func TestResult(t *testing.T) {
	bridge, bridgeKeys := roleKeys(t, identity.RoleBridge, "br-a")
	cmd := validCommand()
	resp := wire.Response{V: wire.Version, RequestID: cmd.RequestID, Status: 400, Body: wire.NewBody([]byte(`{}`))}
	res, err := wire.NewResult(cmd, resp, t0)
	if err != nil {
		t.Fatal(err)
	}
	if res.Type != "CreateOrderFailed" || res.Outcome != wire.OutcomeFailed {
		t.Fatalf("got %s %s", res.Type, res.Outcome)
	}
	rec, err := wire.EncodeResult(res, bridge)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Topic != "http.results.orders" || string(rec.Key) != cmd.DedupKey() {
		t.Fatalf("topic/key %s %s", rec.Topic, rec.Key)
	}
	if _, err := wire.DecodeResult(rec, bridgeKeys); err != nil {
		t.Fatal(err)
	}

	replay := resp
	replay.ReplayOf = wire.NewRequestID()
	if _, err := wire.NewResult(cmd, replay, t0); err == nil {
		t.Fatal("a replay must never produce a result")
	}
	other := resp
	other.RequestID = wire.NewRequestID()
	if _, err := wire.NewResult(cmd, other, t0); err == nil {
		t.Fatal("result pairing a response with another command accepted")
	}
}

func TestIsMutation(t *testing.T) {
	for m, want := range map[string]bool{"POST": true, "PUT": true, "PATCH": true, "DELETE": true, "GET": false, "HEAD": false, "OPTIONS": false, "post": false} {
		if wire.IsMutation(m) != want {
			t.Errorf("%s", m)
		}
	}
}

// D10: whoever can write to a topic must not be able to speak for a role.
func TestD10OnlyTheRightRoleCanSign(t *testing.T) {
	gw, gwKeys := roleKeys(t, identity.RoleGateway, "gw-a")
	br, brKeys := roleKeys(t, identity.RoleBridge, "br-a")
	_, otherBrKeys := roleKeys(t, identity.RoleBridge, "br-a") // same kid, other key

	cmd := validCommand()
	resp := wire.Response{V: wire.Version, RequestID: cmd.RequestID, Status: 201, Body: wire.NewBody([]byte(`{"id":"o1"}`))}
	res, err := wire.NewResult(cmd, resp, t0)
	if err != nil {
		t.Fatal(err)
	}

	// Encoding refuses the wrong role: no code path signs a result with the
	// gateway key or a command with the bridge key.
	if _, err := wire.EncodeCommand(cmd, br); err == nil {
		t.Error("bridge signed a command")
	}
	if _, err := wire.EncodeResult(res, gw); err == nil {
		t.Error("gateway signed a result")
	}
	if _, err := wire.EncodeResponse(resp, cmd.ReplyTo, gw); err == nil {
		t.Error("gateway signed a response")
	}
	if _, err := wire.EncodeResult(res, nil); err == nil {
		t.Error("nil signer accepted")
	}

	resRec, _ := wire.EncodeResult(res, br)
	respRec, _ := wire.EncodeResponse(resp, cmd.ReplyTo, br)
	cmdRec, _ := wire.EncodeCommand(cmd, gw)

	// A gateway key holder crafting a result by hand, with the result domain:
	// still rejected, because results are verified against bridge keys only.
	handmade := clone(resRec)
	setHeader(handmade, wire.HeaderSig, base64.StdEncoding.EncodeToString(gw.Sign(wire.SignDomainResult, handmade.Value)))
	setHeader(handmade, wire.HeaderKeyID, gw.KeyID())
	if _, err := wire.DecodeResult(handmade, brKeys); !errors.Is(err, wire.ErrNotAuthentic) {
		t.Errorf("gateway-signed result: %v", err)
	}
	// Same for a command crafted by a bridge key holder.
	handCmd := clone(cmdRec)
	setHeader(handCmd, wire.HeaderSig, base64.StdEncoding.EncodeToString(br.Sign(wire.SignDomainCommand, handCmd.Value)))
	setHeader(handCmd, wire.HeaderKeyID, br.KeyID())
	if _, err := verifier(gwKeys, t0).Verify(handCmd); !errors.Is(err, wire.ErrNotAuthentic) {
		t.Errorf("bridge-signed command: %v", err)
	}
	// Verifying with the wrong role's keys is refused outright.
	if _, err := wire.DecodeResult(resRec, gwKeys); !errors.Is(err, wire.ErrNotAuthentic) {
		t.Errorf("result verified with gateway keys: %v", err)
	}
	if _, err := wire.DecodeResult(resRec, identity.TrustedKeys{}); !errors.Is(err, wire.ErrNotAuthentic) {
		t.Errorf("result verified with no keys: %v", err)
	}

	// The two confirmed attacks, at the codec level.
	forgedResult := clone(resRec) // C6: rewrite a genuine result
	forgedResult.Value = bytes.Replace(forgedResult.Value, []byte(`"c1"`), []byte(`"mallory"`), 1)
	unsignedResp := clone(respRec) // C4: hand-written response without signature
	unsignedResp.Headers = unsignedResp.Headers[:1]
	alteredResp := clone(respRec) // genuine response, status rewritten
	alteredResp.Value = bytes.Replace(alteredResp.Value, []byte(`"status":201`), []byte(`"status":402`), 1)
	for name, check := range map[string]error{
		"forged result":            func() error { _, err := wire.DecodeResult(forgedResult, brKeys); return err }(),
		"unsigned response":        func() error { _, err := wire.DecodeResponse(unsignedResp, brKeys); return err }(),
		"altered response":         func() error { _, err := wire.DecodeResponse(alteredResp, brKeys); return err }(),
		"response from other key":  func() error { _, err := wire.DecodeResponse(respRec, otherBrKeys); return err }(),
		"command as response type": func() error { _, err := wire.DecodeResponse(cmdRec, brKeys); return err }(),
	} {
		if !errors.Is(check, wire.ErrNotAuthentic) {
			t.Errorf("%s: want ErrNotAuthentic, got %v", name, check)
		}
	}

	// A genuine result moved to another service's result topic.
	moved := clone(resRec)
	moved.Topic = wire.ResultTopic("payments")
	if _, err := wire.DecodeResult(moved, brKeys); !errors.Is(err, wire.ErrNotAuthentic) {
		t.Errorf("result on foreign topic: %v", err)
	}

	// Genuine records pass, and the audit path reports instead of enforcing.
	if _, err := wire.DecodeResult(resRec, brKeys); err != nil {
		t.Fatal(err)
	}
	if _, err := wire.DecodeResponse(respRec, brKeys); err != nil {
		t.Fatal(err)
	}
	if _, err := wire.DecodeResultUnverified(forgedResult); err != nil {
		t.Fatalf("audit must still be able to read a forgery: %v", err)
	}
	if err := wire.VerifySignature(forgedResult, brKeys); !errors.Is(err, wire.ErrNotAuthentic) {
		t.Fatalf("audit marks forgery: %v", err)
	}
	if err := wire.VerifySignature(cmdRec, gwKeys); err != nil {
		t.Fatalf("audit marks genuine command: %v", err)
	}
}
