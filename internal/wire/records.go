package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka record headers. Every record type is signed (D10): commands by the
// gateway, responses and results by the bridge. The signature covers the
// exact record value bytes, not a re-serialization: no canonical-JSON
// problem, and the value stays a plain JSON document that audit consumers
// read without our code.
const (
	HeaderType  = "hok-type"
	HeaderKeyID = "hok-kid"
	HeaderSig   = "hok-sig"

	TypeCommand  = "command.v1"
	TypeResponse = "response.v1"
	TypeResult   = "result.v1"

	// Signature domains: a signature made for one record type never verifies
	// as another, even under the same key.
	SignDomainCommand  = "http-over-kafka/command/v1"
	SignDomainResponse = "http-over-kafka/response/v1"
	SignDomainResult   = "http-over-kafka/result/v1"
)

// signing maps a record type to its signature domain and the only role
// allowed to produce it.
var signing = map[string]struct {
	domain string
	role   identity.Role
}{
	TypeCommand:  {SignDomainCommand, identity.RoleGateway},
	TypeResponse: {SignDomainResponse, identity.RoleBridge},
	TypeResult:   {SignDomainResult, identity.RoleBridge},
}

func signedRecord(typ, topic string, key, value []byte, s *identity.Signer) (*kgo.Record, error) {
	spec := signing[typ]
	if s == nil || s.Role() != spec.role {
		return nil, fmt.Errorf("%s records must be signed with a %s key", typ, spec.role)
	}
	return &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: HeaderType, Value: []byte(typ)},
			{Key: HeaderKeyID, Value: []byte(s.KeyID())},
			{Key: HeaderSig, Value: []byte(base64.StdEncoding.EncodeToString(s.Sign(spec.domain, value)))},
		},
	}, nil
}

// VerifySignature authenticates any record of this contract against the keys
// of the role that must have produced its type. It fails with ErrNotAuthentic
// when unsigned, altered, signed by an unknown key, or when trust holds the
// keys of the wrong role (gateway keys never vouch for a result).
func VerifySignature(rec *kgo.Record, trust identity.TrustedKeys) error {
	typ := header(rec, HeaderType)
	spec, ok := signing[typ]
	if !ok {
		return fmt.Errorf("%w: record type %q", ErrNotAuthentic, typ)
	}
	if trust.Role() != spec.role {
		return fmt.Errorf("%w: %s records are signed by the %s, verifier holds %q keys", ErrNotAuthentic, typ, spec.role, trust.Role())
	}
	sig, err := base64.StdEncoding.DecodeString(header(rec, HeaderSig))
	if err != nil || len(sig) == 0 {
		return fmt.Errorf("%w: missing or malformed signature", ErrNotAuthentic)
	}
	if err := trust.Verify(header(rec, HeaderKeyID), spec.domain, rec.Value, sig); err != nil {
		return fmt.Errorf("%w: %v", ErrNotAuthentic, err)
	}
	return nil
}

// DefaultClockSkew is tolerated between gateway and bridge clocks.
const DefaultClockSkew = 30 * time.Second

// DefaultMaxTTL caps ExpiresAt-IssuedAt. It is what lets the bridge size its
// dedup retention: retention >= MaxTTL + 2*ClockSkew closes the replay window.
const DefaultMaxTTL = time.Hour

var (
	// ErrNotAuthentic: unsigned, altered, unknown key, malformed, or addressed
	// to another service. Whatever the record type, nothing derived from it
	// may reach a caller, B, or an event topic. For commands: do not execute
	// and do not reply (ReplyTo itself is untrusted). Log and drop.
	ErrNotAuthentic = errors.New("record not authentic")
	// ErrStale: authentic but outside its validity window (or with a lifetime
	// above MaxTTL). Do not execute;
	// reply with FaultCommandStale and record a Result.
	ErrStale = errors.New("command stale")
)

// EncodeCommand validates, serializes and signs cmd into a record ready to
// produce. It is the only supported way to put a command on Kafka. s must be
// a gateway signer.
func EncodeCommand(cmd Command, s *identity.Signer) (*kgo.Record, error) {
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	value, err := marshal(cmd)
	if err != nil {
		return nil, err
	}
	return signedRecord(TypeCommand, CommandTopic(cmd.Service), []byte(cmd.DedupKey()), value, s)
}

// CommandVerifier is the bridge's single entry point for incoming commands.
type CommandVerifier struct {
	Keys      identity.TrustedKeys // gateway keys
	Service   string               // the service this bridge fronts
	Now       func() time.Time     // nil: time.Now
	ClockSkew time.Duration        // 0: DefaultClockSkew
	MaxTTL    time.Duration        // 0: DefaultMaxTTL
}

// Verify authenticates rec and returns its command. On ErrStale the returned
// command is authentic and may be replied to; on any other error it is zero.
func (v CommandVerifier) Verify(rec *kgo.Record) (Command, error) {
	fail := func(format string, a ...any) (Command, error) {
		return Command{}, fmt.Errorf("%w: %s", ErrNotAuthentic, fmt.Sprintf(format, a...))
	}
	if h := header(rec, HeaderType); h != TypeCommand {
		return fail("record type %q", h)
	}
	if err := VerifySignature(rec, v.Keys); err != nil {
		return Command{}, err
	}
	// Strict decode: an unknown field may carry semantics this bridge would
	// silently ignore. New fields require a Version bump.
	var cmd Command
	dec := json.NewDecoder(bytes.NewReader(rec.Value))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cmd); err != nil {
		return fail("decode: %v", err)
	}
	if dec.More() {
		return fail("trailing data after command")
	}
	if err := cmd.Validate(); err != nil {
		return fail("%v", err)
	}
	if cmd.Service != v.Service || rec.Topic != CommandTopic(v.Service) {
		return fail("command for service %q on topic %q, this bridge serves %q", cmd.Service, rec.Topic, v.Service)
	}
	if string(rec.Key) != cmd.DedupKey() {
		return fail("record key does not match command dedup key")
	}
	now, skew := time.Now(), DefaultClockSkew
	if v.Now != nil {
		now = v.Now()
	}
	if v.ClockSkew != 0 {
		skew = v.ClockSkew
	}
	maxTTL := DefaultMaxTTL
	if v.MaxTTL != 0 {
		maxTTL = v.MaxTTL
	}
	switch {
	case cmd.ExpiresAt.Sub(cmd.IssuedAt) > maxTTL:
		return cmd, fmt.Errorf("%w: lifetime %s exceeds %s", ErrStale, cmd.ExpiresAt.Sub(cmd.IssuedAt), maxTTL)
	case cmd.IssuedAt.After(now.Add(skew)):
		return cmd, fmt.Errorf("%w: issued %s in the future", ErrStale, cmd.IssuedAt.Sub(now))
	case now.After(cmd.ExpiresAt.Add(skew)):
		return cmd, fmt.Errorf("%w: expired %s ago", ErrStale, now.Sub(cmd.ExpiresAt))
	}
	return cmd, nil
}

// EncodeResponse targets the reply topic of the gateway instance that holds
// the caller's connection. Key: requestId. s must be a bridge signer.
func EncodeResponse(resp Response, replyTo string, s *identity.Signer) (*kgo.Record, error) {
	if err := resp.Validate(); err != nil {
		return nil, err
	}
	if err := validReplyTopic(replyTo); err != nil {
		return nil, err
	}
	value, err := marshal(resp)
	if err != nil {
		return nil, err
	}
	return signedRecord(TypeResponse, replyTo, []byte(resp.RequestID), value, s)
}

// DecodeResponse authenticates rec against bridge keys, then decodes it. On
// ErrNotAuthentic the gateway drops the record and keeps waiting for the
// genuine one: a forgery must neither reach the caller nor end the wait.
// Lenient on unknown fields: a gateway must not drop a genuine reply because
// a newer bridge added information.
func DecodeResponse(rec *kgo.Record, bridge identity.TrustedKeys) (Response, error) {
	var r Response
	if h := header(rec, HeaderType); h != TypeResponse {
		return r, fmt.Errorf("%w: record type %q, want %q", ErrNotAuthentic, h, TypeResponse)
	}
	if err := VerifySignature(rec, bridge); err != nil {
		return r, err
	}
	if err := json.Unmarshal(rec.Value, &r); err != nil {
		return Response{}, err
	}
	if string(rec.Key) != r.RequestID {
		return Response{}, errors.New("record key does not match requestId")
	}
	return r, r.Validate()
}

// EncodeResult signs res; s must be a bridge signer.
func EncodeResult(res Result, s *identity.Signer) (*kgo.Record, error) {
	if err := res.Validate(); err != nil {
		return nil, err
	}
	value, err := marshal(res)
	if err != nil {
		return nil, err
	}
	return signedRecord(TypeResult, ResultTopic(res.Command.Service), []byte(res.Command.DedupKey()), value, s)
}

// DecodeResult authenticates rec against bridge keys, then decodes it. The
// only way to get a Result that may feed event derivation. It also rejects a
// genuine result copied to another service's result topic.
//
// A byte-identical copy of a genuine result record verifies: signatures prove
// origin, not uniqueness. Consumers dedup by Command.RequestID.
func DecodeResult(rec *kgo.Record, bridge identity.TrustedKeys) (Result, error) {
	if h := header(rec, HeaderType); h != TypeResult {
		return Result{}, fmt.Errorf("%w: record type %q, want %q", ErrNotAuthentic, h, TypeResult)
	}
	if err := VerifySignature(rec, bridge); err != nil {
		return Result{}, err
	}
	r, err := DecodeResultUnverified(rec)
	if err != nil {
		return Result{}, err
	}
	if rec.Topic != ResultTopic(r.Command.Service) {
		return Result{}, fmt.Errorf("%w: result for %q found on topic %q", ErrNotAuthentic, r.Command.Service, rec.Topic)
	}
	return r, nil
}

// DecodeResultUnverified is for readers that report authenticity instead of
// enforcing it (audit: decode, then VerifySignature). Never derive anything
// from its output.
func DecodeResultUnverified(rec *kgo.Record) (Result, error) {
	var r Result
	if h := header(rec, HeaderType); h != TypeResult {
		return r, fmt.Errorf("record type %q, want %q", h, TypeResult)
	}
	if err := json.Unmarshal(rec.Value, &r); err != nil {
		return Result{}, err
	}
	return r, r.Validate()
}

// DecodeCommandUnverified is for passive readers of the mutation stream
// (audit, analytics). It checks shape, not authenticity (pair it with
// VerifySignature against gateway keys): never use it to decide whether to
// call B.
func DecodeCommandUnverified(rec *kgo.Record) (Command, error) {
	var c Command
	if h := header(rec, HeaderType); h != TypeCommand {
		return c, fmt.Errorf("record type %q, want %q", h, TypeCommand)
	}
	if err := json.Unmarshal(rec.Value, &c); err != nil {
		return Command{}, err
	}
	return c, c.Validate()
}

func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

func header(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
