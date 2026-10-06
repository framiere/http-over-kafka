package bridge

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StateTopic is the dedup store of a service: a compacted changelog with one
// partition per command partition. Partition p is written only by the
// transactional producer owning command partition p, in the same transactions
// that publish Responses and Results, so the store can never disagree with
// what the gateway and the result stream saw.
func StateTopic(service string) string { return "http.bridge-state." + service }

func stateTopicDef(service string, partitions int32) kafkaenv.Topic {
	compact := "compact"
	maxBytes := strconv.Itoa(kafkaenv.MaxMessageBytes)
	return kafkaenv.Topic{Name: StateTopic(service), Partitions: partitions, Configs: map[string]*string{
		"cleanup.policy":    &compact,
		"max.message.bytes": &maxBytes,
	}}
}

// Reserved state keys. Dedup keys start with "r:" or "i:" (wire.DedupKey).
const (
	keyOffset  = "#offset"  // next command offset to process
	keyFence   = "#fence"   // written by each new owner; see partition.restore
	keyGenesis = "#genesis" // the layout genesis whose claims this partition holds
)

type phase string

const (
	// started is committed before B is called for a non-replayable operation.
	// Finding it without a matching "done" means B may have run.
	started phase = "started"
	done    phase = "done"
)

// entry is the dedup state of one wire.DedupKey.
type entry struct {
	RequestID   string `json:"requestId"`
	Fingerprint string `json:"fingerprint"`
	Phase       phase  `json:"phase"`
	// Attempt identifies the process attempt that wrote "started", so the
	// writer can tell its own marker (and what it knows about it) from one
	// left by another instance.
	Attempt string       `json:"attempt,omitempty"`
	Outcome wire.Outcome `json:"outcome,omitempty"`
	// Response is kept only for idempotency-keyed scopes: it is what a retry
	// with the same key receives. Plain redeliveries never need it.
	Response *wire.Response `json:"response,omitempty"`
	// Unanswered marks a claim made by a genesis scan: the command itself
	// has not been answered yet (no Response, no Result).
	Unanswered bool `json:"unanswered,omitempty"`
	// PurgeAfter is the inclusive retention boundary on any owner's clock:
	// at least ExpiresAt + 3*ClockSkew, to cover the verifier's tolerance and
	// the 2*ClockSkew difference between the fastest and slowest owners.
	PurgeAfter time.Time `json:"purgeAfter"`
	// RetentionPolicy distinguishes the legacy 2*ClockSkew deadline from
	// the current bound. Restore upgrades legacy deadlines exactly once.
	RetentionPolicy uint8 `json:"retentionPolicy,omitempty"`
}

// claims reports whether the entry pins its idempotency scope to its outcome.
// A retry with the same key executes anew only when B certainly did not run,
// or when the outcome is unknown but the operation may be replayed (D3).
func (e *entry) claims(replayable bool) bool {
	switch {
	case e.Phase == started:
		return true
	case e.Outcome == wire.OutcomeNotExecuted:
		return false
	case e.Outcome == wire.OutcomeUnknown:
		return !replayable
	}
	return true
}

type offsetValue struct {
	Next int64 `json:"next"`
}

type fenceValue struct {
	Instance string    `json:"instance"`
	At       time.Time `json:"at"`
}

// State records are signed by the bridge's own key: the state decides what
// a retry is served and whether B may run, so a record anyone with write
// access forged would launder a fake Response under the bridge's signature
// (D10) or erase a "started" marker. The signature binds topic, partition
// and key with the value, so a genuine record copied elsewhere does not
// verify either. Tombstones are signed too (empty value).
const signDomainState = "http-over-kafka/bridge-state/v1"

const (
	headerKeyID = "hok-kid"
	headerSig   = "hok-sig"
)

func stateSigned(topic string, partition int32, key string, value []byte) []byte {
	return []byte(fmt.Sprintf("%s\x00%d\x00%s\x00%s", topic, partition, key, value))
}

func signedStateRecord(s *identity.Signer, topic string, partition int32, key string, value []byte) *kgo.Record {
	sig := s.Sign(signDomainState, stateSigned(topic, partition, key, value))
	return &kgo.Record{Topic: topic, Partition: partition, Key: []byte(key), Value: value, Headers: []kgo.RecordHeader{
		{Key: headerKeyID, Value: []byte(s.KeyID())},
		{Key: headerSig, Value: []byte(base64.StdEncoding.EncodeToString(sig))},
	}}
}

// stateRecord encodes v (nil: tombstone) as a signed state record.
func stateRecord(s *identity.Signer, topic string, partition int32, key string, v any) (*kgo.Record, error) {
	if v == nil {
		return signedStateRecord(s, topic, partition, key, nil), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("state %s: %w", key, err)
	}
	return signedStateRecord(s, topic, partition, key, b), nil
}

// errStateTampered: a record of the state topic was not written by this
// bridge's key. Nothing about the partition can be trusted any more: a
// forged record may have replaced (and, after compaction, erased) a genuine
// entry, so ignoring it would not restore what it hid.
var errStateTampered = errors.New("dedup state tampered")

func verifyStateRecord(trust identity.TrustedKeys, rec *kgo.Record) error {
	var kid, sig string
	for _, h := range rec.Headers {
		switch h.Key {
		case headerKeyID:
			kid = string(h.Value)
		case headerSig:
			sig = string(h.Value)
		}
	}
	raw, err := base64.StdEncoding.DecodeString(sig)
	if err != nil || len(raw) == 0 {
		return fmt.Errorf("%w: unsigned record %q at offset %d", errStateTampered, rec.Key, rec.Offset)
	}
	if err := trust.Verify(kid, signDomainState, stateSigned(rec.Topic, rec.Partition, string(rec.Key), rec.Value), raw); err != nil {
		return fmt.Errorf("%w: record %q at offset %d: %v", errStateTampered, rec.Key, rec.Offset, err)
	}
	return nil
}

// store is the in-memory view of one state partition, rebuilt by restore.
type store struct {
	entries map[string]*entry
	next    int64  // persisted next offset; records below it are already handled
	genesis string // see keyGenesis
}

func (s *store) apply(rec *kgo.Record) error {
	key := string(rec.Key)
	switch {
	case key == keyFence:
		return nil
	case key == keyGenesis:
		if err := json.Unmarshal(rec.Value, &s.genesis); err != nil {
			return fmt.Errorf("state genesis at %d: %w", rec.Offset, err)
		}
	case key == keyOffset:
		var o offsetValue
		if err := json.Unmarshal(rec.Value, &o); err != nil {
			return fmt.Errorf("state offset at %d: %w", rec.Offset, err)
		}
		s.next = max(s.next, o.Next)
	case rec.Value == nil:
		delete(s.entries, key)
	default:
		var e entry
		if err := json.Unmarshal(rec.Value, &e); err != nil {
			return fmt.Errorf("state %s at %d: %w", key, rec.Offset, err)
		}
		s.entries[key] = &e
	}
	return nil
}
