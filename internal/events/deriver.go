package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Record headers. Events use the CloudEvents Kafka binding, binary mode: the
// value is exactly the mapped payload, metadata travels in ce_* headers, so
// consumers need neither our code nor an envelope to read them.
const (
	TypeFailure = "event-failure.v1"

	ceSpecVersion = "ce_specversion"
	ceID          = "ce_id"
	ceSource      = "ce_source"
	ceType        = "ce_type"
	ceTime        = "ce_time"
	ceTraceParent = "ce_traceparent"
	contentType   = "content-type"
)

// FailureTopic receives one record per Result whose mapping applied but could
// not be honoured. It sits under the reserved http. prefix: the deriver is
// infrastructure, and no mapping can name it as an event topic.
func FailureTopic(service string) string { return "http.event-failures." + service }

// GroupID is shared by all deriver instances of a service.
func GroupID(service string) string { return "kb-deriver." + service }

// TransactionalID must be stable per instance across restarts: a restarted
// instance fences its predecessor and aborts its open transaction at once.
// A fresh id leaves that transaction open until it times out, which stalls
// read_committed consumers of the event topics meanwhile.
func TransactionalID(service, instance string) string {
	return "kb-deriver." + service + "." + instance
}

// Topics are the topics the deriver writes to (dedupWindow 0: the default). The result topic is the
// bridge's and is not provisioned here.
func Topics(svc *apispec.Service, partitions int32, dedupWindow time.Duration) []kafkaenv.Topic {
	if dedupWindow == 0 {
		dedupWindow = DefaultDedupWindow
	}
	var names []string
	for _, op := range svc.Operations() {
		if op.Event != nil && !slices.Contains(names, op.Event.Topic) {
			names = append(names, op.Event.Topic)
		}
	}
	out := []kafkaenv.Topic{kafkaenv.EventTopic(FailureTopic(svc.Name()), partitions), dedupTopic(svc.Name(), dedupWindow)}
	for _, n := range names {
		out = append(out, kafkaenv.EventTopic(n, partitions))
	}
	return out
}

// Failure is the value of a FailureTopic record. It points at the Result
// instead of copying it: bodies stay where their retention is decided.
type Failure struct {
	V           int      `json:"v"`
	Reason      string   `json:"reason"`
	Details     []string `json:"details"`
	Service     string   `json:"service"`
	OperationID string   `json:"operationId,omitempty"`
	RequestID   string   `json:"requestId,omitempty"`
	Result      Position `json:"result"`
}

type Position struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type Config struct {
	Brokers  []string
	Service  *apispec.Service
	Instance string // stable across restarts, see TransactionalID
	// BridgeKeys authenticate Results (D10). Required: without them any
	// writer of the result topic could mint domain events.
	BridgeKeys identity.TrustedKeys
	Logger     *slog.Logger
	// SessionTimeout (default 10s) bounds how long a dead instance's
	// partitions stay frozen before another instance takes them, and is
	// also the transaction timeout, which bounds how long its open
	// transaction blocks read_committed readers of the event topics. The
	// price of a low value: an instance that stalls longer (VM freeze,
	// network partition) is evicted, its transaction aborted and its batch
	// re-derived elsewhere. That costs a replay, never a duplicate.
	SessionTimeout time.Duration
	// DedupWindow (default DefaultDedupWindow) bounds the dedup memory: a
	// Result completed longer ago than this yields no event, only a
	// beyond_dedup_window failure. It must exceed the longest lag the
	// deriver may have behind the bridge.
	DedupWindow time.Duration
}

// Deriver consumes a service's Results and produces its domain events.
//
// Only Results signed by the bridge (D10) are derived from. Anything else on
// the result topic goes to the failure topic as not_authentic.
//
// Guarantee: every Result offset is consumed, and its output (one event, one
// failure record, or nothing) produced, in a single Kafka transaction with the
// offset commit and the dedup entry. For read_committed consumers each Result
// yields its output exactly once, through crashes, restarts and rebalances,
// and a requestId yields at most one event however many copies of its Result
// appear (see dedup.go). read_uncommitted consumers can see records of
// aborted attempts.
type Deriver struct {
	cfg Config
	log *slog.Logger

	// Test hooks: beforeCommit runs after a batch is flushed (fail to
	// simulate a crash, block to hold the transaction open), onEnd sees how
	// each transaction ended.
	beforeCommit func() error
	onEnd        func(committed bool)

	// Owned by the Run goroutine, except needLoad (set by group callbacks).
	seen       dedupSet
	needLoad   atomic.Bool
	prunedAt   int
	partitions int32 // of the result topic, for the routing check
	hash       kgo.TopicPartitioner
	now        func() time.Time
}

func New(cfg Config) (*Deriver, error) {
	if cfg.Service == nil || len(cfg.Brokers) == 0 {
		return nil, errors.New("events: service and brokers are required")
	}
	if cfg.BridgeKeys.Role() != identity.RoleBridge {
		return nil, fmt.Errorf("events: bridge keys are required to authenticate results (have %q keys)", cfg.BridgeKeys.Role())
	}
	if err := wire.ValidateName("deriver instance", cfg.Instance); err != nil {
		return nil, err
	}
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 10 * time.Second
	}
	if cfg.DedupWindow == 0 {
		cfg.DedupWindow = DefaultDedupWindow
	}
	l := cfg.Logger
	if l == nil {
		l = slog.Default()
	}
	return &Deriver{
		cfg:  cfg,
		log:  l.With("service", cfg.Service.Name(), "instance", cfg.Instance),
		hash: kgo.StickyKeyPartitioner(nil).ForTopic(wire.ResultTopic(cfg.Service.Name())),
		now:  time.Now,
	}, nil
}

// endTimeout bounds producing and committing one batch. The batch runs on a
// context detached from Run's, so shutdown finishes the batch in hand
// instead of abandoning an open transaction.
const endTimeout = 30 * time.Second

// Run derives until ctx is done (nil) or a non-recoverable error occurs. On
// error the open transaction is left to be aborted by the next instance with
// the same transactional id: restart the process.
func (d *Deriver) Run(ctx context.Context) error {
	svc := d.cfg.Service.Name()
	sess, err := kgo.NewGroupTransactSession(kafkaenv.ClientOpts(d.cfg.Brokers,
		kgo.TransactionalID(TransactionalID(svc, d.cfg.Instance)),
		kgo.TransactionTimeout(d.cfg.SessionTimeout),
		kgo.SessionTimeout(d.cfg.SessionTimeout),
		kgo.HeartbeatInterval(max(d.cfg.SessionTimeout/6, 500*time.Millisecond)),
		kgo.ConsumerGroup(GroupID(svc)),
		kgo.ConsumeTopics(wire.ResultTopic(svc)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.RequireStableFetchOffsets(),
		// Partitions acquired may have been derived by another instance
		// since the dedup set was loaded.
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { d.needLoad.Store(true) }),
	)...)
	if err != nil {
		return err
	}
	defer sess.Close()
	// Initializing the producer id now fences a predecessor with the same
	// transactional id and aborts its open transaction, before the dedup load
	// would otherwise wait for that transaction to time out.
	initCtx, cancel := context.WithTimeout(ctx, endTimeout)
	_, _, err = sess.Client().ProducerID(initCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("init producer id: %w", err)
	}
	d.log.Info("deriving events", "from", wire.ResultTopic(svc), "failures", FailureTopic(svc))

	for {
		fs := sess.PollFetches(ctx)
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
		if fs.NumRecords() == 0 {
			continue
		}
		if err := d.batch(ctx, sess, fs); err != nil {
			return err
		}
	}
}

func (d *Deriver) batch(parent context.Context, sess *kgo.GroupTransactSession, fs kgo.Fetches) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), endTimeout)
	defer cancel()
	if d.needLoad.Swap(false) || d.seen == nil {
		seen, err := loadDedup(ctx, d.cfg.Brokers, d.cfg.Service.Name(), d.now().Add(-d.cfg.DedupWindow))
		if err != nil {
			return err
		}
		d.seen = seen
		d.log.Info("dedup set loaded", "entries", len(seen))
	}
	if err := sess.Begin(); err != nil {
		return err
	}
	prod := kgo.AbortingFirstErrPromise(sess.Client())
	pending := dedupSet{}
	var events, failures int
	fs.EachRecord(func(r *kgo.Record) {
		for _, out := range d.process(ctx, r, pending) {
			switch out.Topic {
			case FailureTopic(d.cfg.Service.Name()):
				failures++
			case DedupTopic(d.cfg.Service.Name()):
			default:
				events++
			}
			sess.Produce(ctx, out, prod.Promise())
		}
	})
	if d.beforeCommit != nil {
		if err := sess.Client().Flush(ctx); err != nil {
			return err
		}
		if err := d.beforeCommit(); err != nil {
			return err
		}
	}
	committed, err := sess.End(ctx, prod.Err() == nil)
	if err == nil && d.onEnd != nil {
		d.onEnd(committed)
	}
	switch {
	case err != nil:
		return fmt.Errorf("end transaction: %w", err)
	case prod.Err() != nil:
		return fmt.Errorf("produce (transaction aborted): %w", prod.Err())
	case !committed:
		// Rebalance: aborted, the records will be consumed again by
		// whoever owns their partitions now.
		d.log.Info("batch aborted by rebalance, will be reprocessed", "results", fs.NumRecords())
		return nil
	}
	maps.Copy(d.seen, pending)
	if len(d.seen) > 2*d.prunedAt {
		d.seen.prune(d.now().Add(-d.cfg.DedupWindow))
		d.prunedAt = max(len(d.seen), 1024)
	}
	d.log.Debug("batch committed", "results", fs.NumRecords(), "events", events, "failures", failures)
	return nil
}

// process maps one Result record to its output records: nothing, one
// failure, or one event and its dedup entry.
func (d *Deriver) process(ctx context.Context, r *kgo.Record, pending dedupSet) []*kgo.Record {
	svc := d.cfg.Service.Name()
	pos := Position{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}
	fail := func(f Failure) []*kgo.Record { return []*kgo.Record{d.failure(f, pos)} }
	res, err := wire.DecodeResult(r, d.cfg.BridgeKeys)
	switch {
	case errors.Is(err, wire.ErrNotAuthentic):
		// Not from the bridge: whatever it claims never happened as far as
		// we can prove. Recorded, never derived, never retried.
		details := []string{err.Error()}
		if claim, cerr := wire.DecodeResultUnverified(r); cerr == nil {
			details = append(details, fmt.Sprintf("claims requestId %s, operation %s, status %d",
				claim.Command.RequestID, claim.Command.OperationID, claim.Response.Status))
		}
		return fail(Failure{Reason: ReasonNotAuthentic, Details: details})
	case err != nil:
		return fail(Failure{Reason: ReasonUndecodableResult, Details: []string{err.Error()}})
	}
	f := Failure{Service: svc, OperationID: res.Command.OperationID, RequestID: res.Command.RequestID}
	if err := d.routed(ctx, r, res.Command); err != nil {
		f.Reason, f.Details = ReasonMisrouted, []string{err.Error()}
		return fail(f)
	}
	ev, err := Derive(d.cfg.Service, res)
	if err != nil {
		de, ok := AsError(err)
		if !ok {
			de = &Error{Reason: ReasonEvaluationFailed, Details: []string{err.Error()}}
		}
		f.Reason, f.Details = de.Reason, de.Details
		return fail(f)
	}
	if ev == nil {
		return nil
	}
	id := res.Command.RequestID
	if age := d.now().Sub(res.CompletedAt); age > d.cfg.DedupWindow {
		f.Reason, f.Details = ReasonBeyondDedupWindow, []string{fmt.Sprintf(
			"completed %s ago, dedup window is %s: cannot prove no event was emitted for it", age.Round(time.Second), d.cfg.DedupWindow)}
		return fail(f)
	}
	prev, dup := pending[id]
	if !dup {
		prev, dup = d.seen[id]
	}
	if dup {
		f.Reason, f.Details = ReasonDuplicateResult, []string{fmt.Sprintf(
			"event already derived from %s[%d]@%d", prev.Result.Topic, prev.Result.Partition, prev.Result.Offset)}
		return fail(f)
	}
	e := dedupEntry{V: wire.Version, RequestID: id, CompletedAt: res.CompletedAt, Event: ev.Topic, Result: pos}
	pending[id] = e
	return []*kgo.Record{EventRecord(*ev), dedupRecord(svc, e)}
}

// routed checks the Result sits where the bridge puts it: keyed by its
// command's DedupKey, on that key's partition. A copy written anywhere else
// would escape the owner's dedup set.
func (d *Deriver) routed(ctx context.Context, r *kgo.Record, cmd wire.Command) error {
	if string(r.Key) != cmd.DedupKey() {
		return errors.New("record key is not the command's dedup key")
	}
	for refreshed := false; ; refreshed = true {
		if d.partitions == 0 || refreshed {
			n, err := d.resultPartitions(ctx)
			if err != nil {
				return err
			}
			d.partitions = n
		}
		want := d.hash.Partition(&kgo.Record{Key: r.Key}, int(d.partitions))
		if int32(want) == r.Partition {
			return nil
		}
		if refreshed {
			return fmt.Errorf("on partition %d, its key routes to partition %d of %d", r.Partition, want, d.partitions)
		}
	}
}

func (d *Deriver) resultPartitions(ctx context.Context) (int32, error) {
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(d.cfg.Brokers)...)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	topic := wire.ResultTopic(d.cfg.Service.Name())
	td, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return 0, err
	}
	t, ok := td[topic]
	if !ok || t.Err != nil {
		return 0, fmt.Errorf("metadata of %s: %v", topic, t.Err)
	}
	return int32(len(t.Partitions)), nil
}

func (d *Deriver) failure(f Failure, pos Position) *kgo.Record {
	f.V, f.Service, f.Result = wire.Version, d.cfg.Service.Name(), pos
	d.log.Warn("event not derived", "reason", f.Reason, "details", f.Details,
		"requestId", f.RequestID, "operationId", f.OperationID, "result", pos)
	value, err := marshalCompact(f)
	if err != nil {
		panic(err) // strings and ints only
	}
	key := f.RequestID
	if key == "" {
		key = fmt.Sprintf("%s/%d/%d", pos.Topic, pos.Partition, pos.Offset)
	}
	return &kgo.Record{
		Topic:   FailureTopic(f.Service),
		Key:     []byte(key),
		Value:   value,
		Headers: []kgo.RecordHeader{{Key: wire.HeaderType, Value: []byte(TypeFailure)}},
	}
}

// EventRecord encodes ev with the CloudEvents Kafka binding (binary mode).
func EventRecord(ev Event) *kgo.Record {
	h := []kgo.RecordHeader{
		{Key: ceSpecVersion, Value: []byte("1.0")},
		{Key: ceID, Value: []byte(ev.ID)},
		{Key: ceSource, Value: []byte(ev.Source)},
		{Key: ceType, Value: []byte(ev.Type)},
		{Key: ceTime, Value: []byte(ev.Time.UTC().Format(time.RFC3339Nano))},
		{Key: contentType, Value: []byte("application/json")},
	}
	if ev.TraceParent != "" {
		h = append(h, kgo.RecordHeader{Key: ceTraceParent, Value: []byte(ev.TraceParent)})
	}
	return &kgo.Record{Topic: ev.Topic, Key: ev.Key, Value: ev.Value, Headers: h}
}
