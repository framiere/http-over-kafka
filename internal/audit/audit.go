// Package audit is an independent reader of the mutation stream (§4). It
// subscribes to command and result topics; neither A, B, the gateway nor the
// bridge knows it exists.
//
// It answers who did what, and with which outcome, and nothing more: bodies,
// headers, query strings and idempotency keys are never printed. They can hold
// personal data or tokens, and an audit log is read by more people than the
// services that produced them.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultTopics matches every service's commands and results.
const DefaultTopics = `^http\.(requests|results)\.`

// Authenticity of every entry, checked against the public keys of the role
// that must have signed the record (D10): the gateway for commands, the
// bridge for results.
const (
	Authentic    = "authentic"
	NotAuthentic = "not-authentic" // unsigned, altered, unknown key, or misplaced: its content is a claim
	Unverified   = "unverified"    // no keys configured for that role: nothing was checked
)

// Entry is one audit line.
type Entry struct {
	Kind        string    `json:"kind"` // "requested", "completed" or "unreadable"
	At          time.Time `json:"at,omitzero"`
	RequestID   string    `json:"requestId,omitempty"`
	Service     string    `json:"service,omitempty"`
	OperationID string    `json:"operationId,omitempty"`
	Method      string    `json:"method,omitempty"`
	Path        string    `json:"path,omitempty"`
	// Caller is who the record says made the call; trust it only when
	// Authenticity is Authentic.
	Caller       *wire.Caller `json:"caller,omitempty"`
	Authenticity string       `json:"authenticity"`
	AuthError    string       `json:"authError,omitempty"`
	Idempotent   bool         `json:"idempotent,omitempty"` // an Idempotency-Key was sent
	Outcome      wire.Outcome `json:"outcome,omitempty"`
	Status       int          `json:"status,omitempty"`
	Fault        wire.Fault   `json:"fault,omitempty"`
	Error        string       `json:"error,omitempty"`
	Topic        string       `json:"topic"`
	Partition    int32        `json:"partition"`
	Offset       int64        `json:"offset"`
}

// Auditor turns records into entries. Public keys only: it can check
// identities, never mint them. A zero TrustedKeys leaves that role's records
// Unverified.
type Auditor struct {
	GatewayKeys identity.TrustedKeys
	BridgeKeys  identity.TrustedKeys
}

// Entry describes r. Forgeries are reported, never dropped: a forged
// mutation is exactly what an audit trail must show.
func (a Auditor) Entry(r *kgo.Record) Entry {
	e := Entry{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}
	switch typeOf(r) {
	case wire.TypeCommand:
		e.Authenticity, e.AuthError = a.check(a.GatewayKeys, func() error {
			// Commands are audited long after their validity window:
			// staleness is irrelevant here, authenticity is all that matters.
			cmd, err := wire.DecodeCommandUnverified(r)
			if err != nil {
				return wire.VerifySignature(r, a.GatewayKeys)
			}
			if _, err := (wire.CommandVerifier{Keys: a.GatewayKeys, Service: cmd.Service}).Verify(r); err != nil && !errors.Is(err, wire.ErrStale) {
				return err
			}
			return nil
		})
		cmd, err := wire.DecodeCommandUnverified(r)
		if err != nil {
			return unreadable(e, err)
		}
		e.Kind, e.At = "requested", cmd.IssuedAt
		describe(&e, cmd)
	case wire.TypeResult:
		e.Authenticity, e.AuthError = a.check(a.BridgeKeys, func() error {
			_, err := wire.DecodeResult(r, a.BridgeKeys)
			if err != nil && !errors.Is(err, wire.ErrNotAuthentic) {
				return wire.VerifySignature(r, a.BridgeKeys) // authentic but malformed
			}
			return err
		})
		res, err := wire.DecodeResultUnverified(r)
		if err != nil {
			return unreadable(e, err)
		}
		e.Kind, e.At = "completed", res.CompletedAt
		describe(&e, res.Command)
		e.Outcome, e.Status, e.Fault = res.Outcome, res.Response.Status, res.Response.Fault
		if res.Response.UpstreamStatus != 0 {
			e.Status = res.Response.UpstreamStatus
		}
	default:
		e.Authenticity, e.AuthError = NotAuthentic, "not a record of this contract"
		return unreadable(e, fmt.Errorf("record type %q", typeOf(r)))
	}
	return e
}

func (a Auditor) check(keys identity.TrustedKeys, verify func() error) (string, string) {
	if keys.IsZero() {
		return Unverified, ""
	}
	if err := verify(); err != nil {
		return NotAuthentic, truncate(err.Error())
	}
	return Authentic, ""
}

func describe(e *Entry, cmd wire.Command) {
	caller := cmd.Caller
	e.RequestID, e.Service, e.OperationID = cmd.RequestID, cmd.Service, cmd.OperationID
	e.Method, e.Path, e.Caller, e.Idempotent = cmd.Method, cmd.Path, &caller, cmd.IdempotencyKey != ""
}

// unreadable keeps the error short and free of record content.
func unreadable(e Entry, err error) Entry {
	e.Kind, e.Error = "unreadable", truncate(err.Error())
	return e
}

func truncate(s string) string {
	if len(s) > 200 {
		return s[:200]
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

type Config struct {
	Brokers     []string
	Group       string
	Topics      string               // regex; DefaultTopics when empty
	GatewayKeys identity.TrustedKeys // zero: commands reported Unverified
	BridgeKeys  identity.TrustedKeys // zero: results reported Unverified
	Out         io.Writer            // one JSON entry per line
	Logger      *slog.Logger
}

// Run prints entries until ctx is done. Delivery is at-least-once: offsets
// are committed after entries are written, so a crash can repeat lines;
// (kind, requestId) identifies duplicates.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Topics == "" {
		cfg.Topics = DefaultTopics
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(cfg.Brokers,
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(cfg.Topics),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.DisableAutoCommit(),
		// New services appear as new topics; find them within seconds.
		kgo.MetadataMaxAge(10*time.Second),
	)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	a := Auditor{GatewayKeys: cfg.GatewayKeys, BridgeKeys: cfg.BridgeKeys}
	enc := json.NewEncoder(cfg.Out)
	enc.SetEscapeHTML(false)
	log.Info("auditing", "topics", cfg.Topics, "group", cfg.Group, "gatewayKeys", !cfg.GatewayKeys.IsZero(), "bridgeKeys", !cfg.BridgeKeys.IsZero())
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		var ferr error
		fs.EachError(func(t string, p int32, err error) {
			ferr = errors.Join(ferr, fmt.Errorf("fetch %s[%d]: %w", t, p, err))
		})
		if ferr != nil {
			return ferr
		}
		var werr error
		fs.EachRecord(func(r *kgo.Record) {
			if werr == nil {
				werr = enc.Encode(a.Entry(r))
			}
		})
		if werr != nil {
			return werr
		}
		if err := cl.CommitUncommittedOffsets(context.WithoutCancel(ctx)); err != nil {
			return err
		}
	}
}
