// Package bridge is the provider side: it consumes the commands of one
// service, calls the real service B over plain HTTP, and publishes each
// outcome (Response to the waiting gateway, Result to the durable stream).
//
// What it promises, per requestId: B is called at most once unless the
// operation is replayable (apispec.Operation.Replayable); when the bridge
// cannot prove whether B ran, the outcome is reported as unknown, never
// retried silently. Retries sharing an Idempotency-Key get the original
// outcome without B being called again. See partition for the argument.
package bridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Fetching for a partition pauses when this many records wait for its worker,
// and resumes below lowWater: a slow B holds back its own partitions only.
const (
	highWater = 2048
	lowWater  = 256
)

type Config struct {
	Service string
	Brokers []string
	Spec    *apispec.Service
	// Keys verify commands (gateway role). Signer signs everything the
	// bridge publishes and its own state (bridge role, D10). The bridge
	// never holds a gateway signing key: it cannot mint commands.
	Keys   identity.TrustedKeys
	Signer *identity.Signer
	// Credentials are B's own credentials for operations whose contract
	// requires one (D12). Checked at startup against every mutation.
	Credentials apispec.Credentials
	Upstream    string // B's base URL, e.g. http://payments:8082
	Instance    string // for logs and fence records

	// UpstreamTimeout bounds one call to B. Past it, the outcome is unknown.
	// Keep it well under the group rebalance timeout (60s): a revoked
	// partition finishes its current call before being handed over.
	UpstreamTimeout time.Duration
	// MaxInFlight caps the commands of one partition processed at once
	// (distinct dedup keys). It bounds the concurrency imposed on B.
	MaxInFlight int
	// ReplayAttempts is how many times a replayable operation is tried when
	// the outcome stays unknown. Non-replayable operations: always 1.
	ReplayAttempts int
	// IdempotencyRetention is how long an outcome stays replayable to a
	// retry carrying the same Idempotency-Key.
	IdempotencyRetention time.Duration
	// ClockSkew bounds each bridge's clock difference from the gateway.
	// Two owners may therefore differ from one another by twice this bound.
	ClockSkew      time.Duration
	MaxTTL         time.Duration
	TxnTimeout     time.Duration
	RestoreTimeout time.Duration
	// PurgeInterval is the delay between complete dedup sweeps. A sweep
	// drains in bounded chunks, even when no commands arrive.
	PurgeInterval time.Duration
	// SessionTimeout is how long a dead bridge's partitions stay frozen
	// before the group hands them over (franz-go default: 45s). Shorter
	// means a long pause (GC, network) also triggers a takeover; that is
	// safe (the paused owner is fenced) but turns its in-flight calls into
	// unknown outcomes.
	SessionTimeout time.Duration

	Log   *slog.Logger
	Now   func() time.Time
	Hooks Hooks
}

// Hooks are observation points for tests (crash injection, timings). They
// run synchronously on the partition goroutine.
type Hooks struct {
	// AfterStarted: "started" is committed, B not called yet.
	AfterStarted func(requestID string)
	// AfterUpstream: B answered (or failed), nothing published yet.
	AfterUpstream func(requestID string)
	// BeforeStartedCommit: "started" is produced in an open transaction.
	BeforeStartedCommit func(requestID string)
	// BeforeCommit: the outcome is produced in an open transaction.
	BeforeCommit func(requestID string)
	// AfterCommit: the outcome is committed, consumer offset not marked yet.
	AfterCommit func(requestID string)
	OnTiming    func(Timing)
}

func (h Hooks) afterStarted(id string) {
	if h.AfterStarted != nil {
		h.AfterStarted(id)
	}
}

func (h Hooks) afterUpstream(id string) {
	if h.AfterUpstream != nil {
		h.AfterUpstream(id)
	}
}

func (h Hooks) beforeStartedCommit(id string) {
	if h.BeforeStartedCommit != nil {
		h.BeforeStartedCommit(id)
	}
}

func (h Hooks) beforeCommit(id string) {
	if h.BeforeCommit != nil {
		h.BeforeCommit(id)
	}
}

func (h Hooks) afterCommit(id string) {
	if h.AfterCommit != nil {
		h.AfterCommit(id)
	}
}

func (h Hooks) onTiming(t Timing) {
	if h.OnTiming != nil {
		h.OnTiming(t)
	}
}

// Timing splits the time the bridge spends on one executed command.
type Timing struct {
	RequestID   string
	OperationID string
	Started     time.Duration // committing the "started" marker (0 when replayable)
	Upstream    time.Duration // calling B
	Complete    time.Duration // committing Response + Result + state
	Total       time.Duration
}

type Bridge struct {
	cfg        Config
	log        *slog.Logger
	cmdTopic   string
	stateTopic string
	verifier   wire.CommandVerifier
	spec       *apispec.Service
	up         *upstream
	hasher     kgo.TopicPartitioner
	partitions int // of the command topic, fixed at startup

	admin   *kadm.Client
	base    *kgo.Client
	abandon context.CancelCauseFunc // stops Run with a fatal error
	// stateTopicID is the state topic identity checked at startup; every
	// partition open re-checks it (a topic recreated while running).
	stateTopicID string
	layout       layout

	warnedSingleBroker    bool
	sawCommandTopicAbsent bool

	mu      sync.Mutex
	workers map[int32]*partition

	replyMu     sync.Mutex
	replyTopics map[string]bool

	dropped atomic.Int64
	replays atomic.Int64
}

func New(cfg Config) (*Bridge, error) {
	if err := wire.ValidateName("service", cfg.Service); err != nil {
		return nil, err
	}
	if cfg.Spec == nil || cfg.Spec.Name() != cfg.Service {
		return nil, errors.New("bridge: spec missing or for another service")
	}
	if cfg.Keys.Role() != identity.RoleGateway {
		return nil, errors.New("bridge: command keys must be trusted gateway keys")
	}
	if cfg.Signer == nil || cfg.Signer.Role() != identity.RoleBridge {
		return nil, errors.New("bridge: a bridge-role signer is required (D10)")
	}
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("bridge: no brokers")
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	def(&cfg.UpstreamTimeout, 15*time.Second)
	def(&cfg.IdempotencyRetention, 24*time.Hour)
	def(&cfg.ClockSkew, wire.DefaultClockSkew)
	def(&cfg.MaxTTL, wire.DefaultMaxTTL)
	if err := validateDedupWindow(cfg); err != nil {
		return nil, err
	}
	def(&cfg.TxnTimeout, 10*time.Second)
	def(&cfg.RestoreTimeout, 2*time.Minute)
	def(&cfg.PurgeInterval, time.Minute)
	if cfg.PurgeInterval < 0 {
		return nil, errors.New("bridge: purge interval must be positive")
	}
	def(&cfg.SessionTimeout, 10*time.Second)
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 64
	}
	if cfg.ReplayAttempts <= 0 {
		cfg.ReplayAttempts = 3
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	up, err := newUpstream(cfg.Upstream, cfg.UpstreamTimeout)
	if err != nil {
		return nil, err
	}
	if err := cfg.Credentials.Check(cfg.Spec, (*apispec.Operation).Transported); err != nil {
		return nil, err
	}
	up.spec, up.creds = cfg.Spec, cfg.Credentials
	cmdTopic := wire.CommandTopic(cfg.Service)
	return &Bridge{
		cfg:        cfg,
		log:        cfg.Log.With("service", cfg.Service, "instance", cfg.Instance),
		cmdTopic:   cmdTopic,
		stateTopic: StateTopic(cfg.Service),
		verifier: wire.CommandVerifier{Keys: cfg.Keys, Service: cfg.Service, Now: cfg.Now,
			ClockSkew: cfg.ClockSkew, MaxTTL: cfg.MaxTTL},
		spec:        cfg.Spec,
		up:          up,
		hasher:      kgo.StickyKeyPartitioner(nil).ForTopic(cmdTopic),
		workers:     map[int32]*partition{},
		replyTopics: map[string]bool{},
	}, nil
}

func (b *Bridge) now() time.Time { return b.cfg.Now() }

// Dropped counts commands refused as not authentic (no reply, no result).
func (b *Bridge) Dropped() int64 { return b.dropped.Load() }

// beforeMemory reports whether the record predates the dedup state.
func (b *Bridge) beforeMemory(rec *kgo.Record) bool {
	ms := b.layout.MemoryStart
	return int(rec.Partition) < len(ms) && rec.Offset < ms[rec.Partition]
}

// idemWindow bounds the history needed by a genesis scan, including command
// lifetime, all owners' clock differences, and idempotency retention.
func (b *Bridge) idemWindow() time.Duration {
	return b.cfg.IdempotencyRetention + b.cfg.MaxTTL + b.replayMargin()
}

func (b *Bridge) expectedPartition(key []byte) int32 {
	return int32(b.hasher.Partition(&kgo.Record{Key: key}, b.partitions))
}

// Run consumes until ctx ends, then hands its partitions over cleanly.
func (b *Bridge) Run(ctx context.Context) error {
	base, err := kgo.NewClient(kafkaenv.ClientOpts(b.cfg.Brokers)...)
	if err != nil {
		return err
	}
	defer base.Close()
	b.base = base
	b.admin = kadm.NewClient(base)
	for backoff := 100 * time.Millisecond; ; backoff = min(backoff*2, time.Second) {
		err := b.prepareTopics(ctx)
		if err == nil {
			break
		}
		var f fatal
		if errors.As(err, &f) {
			return err
		}
		b.log.Warn("kafka not ready, retrying", "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup("hok-bridge." + b.cfg.Service),
		kgo.ConsumeTopics(b.cmdTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// Committed group offsets are only a resume hint: the authoritative
		// position is the one stored with the outcomes (store.next), and a
		// record is marked only after its outcome is committed, so the hint
		// never runs ahead of it.
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsAssigned(b.assigned),
		kgo.OnPartitionsRevoked(b.revoked),
		kgo.OnPartitionsLost(b.lost),
	}
	opts = append(opts, kgo.SessionTimeout(b.cfg.SessionTimeout), kgo.HeartbeatInterval(max(b.cfg.SessionTimeout/6, 500*time.Millisecond)))
	ctx, b.abandon = context.WithCancelCause(ctx)
	consumer, err := kgo.NewClient(kafkaenv.ClientOpts(b.cfg.Brokers, opts...)...)
	if err != nil {
		return err
	}
	b.log.Info("bridge consuming", "topic", b.cmdTopic, "partitions", b.partitions, "upstream", b.cfg.Upstream)

	for {
		fs := consumer.PollFetches(ctx)
		if ctx.Err() != nil || fs.IsClientClosed() {
			break
		}
		fs.EachError(func(t string, p int32, err error) {
			b.log.Error("fetch", "topic", t, "partition", p, "err", err)
		})
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			b.mu.Lock()
			w := b.workers[p.Partition]
			b.mu.Unlock()
			if w == nil {
				return // revoked meanwhile; the next owner fetches them
			}
			w.push(p.Records)
		})
	}
	// Close leaves the group, which revokes (and drains) every partition.
	consumer.Close()
	b.stopAll(true)
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}

// prepareTopics provisions the service topics and checks the invariant the
// dedup design rests on: command, result and state topics have the same
// partition count, the one keys were hashed with.
func (b *Bridge) prepareTopics(ctx context.Context) error {
	// The command topic belongs to the gateway (its producer creates it):
	// wait for it. It decides the partition count; results and state
	// follow it (an operator may have grown it).
	counts := func(topics ...string) (map[string]int, error) {
		td, err := b.admin.ListTopics(ctx, topics...)
		if err != nil {
			return nil, err
		}
		out := map[string]int{}
		for _, t := range topics {
			d, ok := td[t]
			if !ok || d.Err != nil {
				return nil, fmt.Errorf("topic %s: %v", t, d.Err)
			}
			out[t] = len(d.Partitions)
		}
		return out, nil
	}
	c, err := counts(b.cmdTopic)
	if err != nil {
		// Seen absent by this process, as the broker states it (not a
		// failed request): whatever it holds once created was produced
		// while this bridge was waiting, before any memory.
		if td, lerr := b.admin.ListTopics(ctx, b.cmdTopic); lerr == nil {
			if d, ok := td[b.cmdTopic]; !ok || errors.Is(d.Err, kerr.UnknownTopicOrPartition) {
				b.sawCommandTopicAbsent = true
			}
		}
		return fmt.Errorf("waiting for %s, created by the gateway: %w", b.cmdTopic, err)
	}
	n := c[b.cmdTopic]
	md, err := b.admin.BrokerMetadata(ctx)
	if err != nil {
		return err
	}
	state := stateTopicDef(b.cfg.Service, int32(n))
	if brokers := len(md.Brokers); brokers > 1 {
		state.ReplicationFactor = int16(min(3, brokers))
		two := "2"
		state.Configs["min.insync.replicas"] = &two
	}
	if err := kafkaenv.EnsureTopics(ctx, b.admin, kafkaenv.ResultTopic(b.cfg.Service, int32(n)), state); err != nil {
		return err
	}
	resultTopic := wire.ResultTopic(b.cfg.Service)
	if c, err = counts(b.cmdTopic, resultTopic, b.stateTopic); err != nil {
		return err
	}
	if c[resultTopic] != n || c[b.stateTopic] != n {
		return &partitionMismatchError{c}
	}
	if err := b.checkLayout(ctx, n); err != nil {
		return err
	}
	b.partitions = n
	return nil
}

// fatal marks startup errors that retrying cannot fix.
type fatal interface{ fatal() }

type partitionMismatchError struct{ counts map[string]int }

func (*partitionMismatchError) fatal() {}

func (e *partitionMismatchError) Error() string {
	return fmt.Sprintf("partition counts differ: %v; dedup state and results must mirror command partitions", e.counts)
}

// LayoutTopic holds the identity of the dedup state: the command partition
// count it was built with and the ids of the command and state topics. A
// dedup key lives in partition hash(key) mod N of a given state topic; if N
// changes, or either topic is deleted and recreated, a retry no longer finds
// its original's outcome and B would run again. Equal partition counts prove
// nothing after an operator grew all topics together; this record does.
// Any mismatch fails closed: the bridge refuses to serve.
func LayoutTopic(service string) string { return "http.bridge-layout." + service }

const layoutKey = "layout"

type layout struct {
	Partitions     int    `json:"partitions"`
	CommandTopicID string `json:"commandTopicId"`
	StateTopicID   string `json:"stateTopicId"`
	// MemoryStart is, per command partition, the end offset when this
	// dedup state began. Commands below it may have been executed by a
	// bridge whose memory is gone: they are never executed (see process).
	MemoryStart []int64 `json:"memoryStart"`
	// Genesis identifies this memory. Each partition owner, before serving,
	// scans the commands of the idempotency window that predate the memory
	// (ScanFrom..MemoryStart on every command partition) and claims, in its
	// own state, every idempotency scope that now hashes to it.
	Genesis  string  `json:"genesis"`
	ScanFrom []int64 `json:"scanFrom"`
	// BlindUntil is set when that scan cannot be complete (commands of the
	// window were deleted from the command topic). Until then, a keyed
	// command whose scope is unknown is not executed.
	BlindUntil time.Time `json:"blindUntil,omitzero"`
}

type layoutError struct{ reason string }

func (*layoutError) fatal() {}

func (e *layoutError) Error() string {
	return "dedup state identity broken: " + e.reason + "; refusing to serve, since retries could execute B again. " +
		"Either restore what changed, or reset the dedup memory: stop the bridges, delete http.bridge-layout.<svc> and " +
		"http.bridge-state.<svc>, and restart. The bridge recreates them with the command topic's partition count, " +
		"never executes commands that predate the new memory, and rebuilds idempotency claims from the command topic " +
		"(results history and consumer group can stay)"
}

func (b *Bridge) checkLayout(ctx context.Context, n int) error {
	compact := "compact"
	topic := LayoutTopic(b.cfg.Service)
	if err := kafkaenv.EnsureTopics(ctx, b.admin, kafkaenv.Topic{Name: topic, Partitions: 1, Configs: map[string]*string{"cleanup.policy": &compact}}); err != nil {
		return err
	}
	if err := b.checkStateConfig(ctx); err != nil {
		return err
	}
	td, err := b.admin.ListTopics(ctx, b.cmdTopic, b.stateTopic)
	if err != nil {
		return err
	}
	current := layout{Partitions: n, CommandTopicID: td[b.cmdTopic].ID.String(), StateTopicID: td[b.stateTopic].ID.String()}
	recorded, found, err := b.readLayout(ctx, topic)
	if err != nil {
		return err
	}
	b.stateTopicID = current.StateTopicID
	if found {
		switch {
		case recorded.Partitions != current.Partitions:
			return &layoutError{fmt.Sprintf("command partitions changed from %d to %d", recorded.Partitions, current.Partitions)}
		case recorded.StateTopicID != current.StateTopicID:
			return &layoutError{"the dedup state topic " + b.stateTopic + " was deleted and recreated"}
		case recorded.CommandTopicID != current.CommandTopicID:
			// Its offsets restarted at 0: commands below the stored
			// watermark would be skipped, those above judged on stale state.
			return &layoutError{"the command topic " + b.cmdTopic + " was deleted and recreated"}
		}
		b.layout = recorded
		return nil
	}
	return b.genesis(ctx, topic, current)
}

// genesis starts a new dedup memory: on a service's first run, after a total
// wipe, or after an operator reset. It is safe whatever ran before: commands
// already in the topic are never executed (MemoryStart), retries of keys used
// in the window find a claim (Genesis scan), and if the window cannot be
// scanned completely, unknown keys are not executed until it has passed.
func (b *Bridge) genesis(ctx context.Context, topic string, current layout) error {
	n := current.Partitions
	cends, err := b.admin.ListEndOffsets(ctx, b.cmdTopic)
	if err != nil {
		return err
	}
	starts, err := b.admin.ListStartOffsets(ctx, b.cmdTopic)
	if err != nil {
		return err
	}
	now := b.now()
	windowStart := now.Add(-b.idemWindow())
	after, err := b.admin.ListOffsetsAfterMilli(ctx, windowStart.UnixMilli(), b.cmdTopic)
	if err != nil {
		return err
	}
	current.MemoryStart = make([]int64, n)
	current.ScanFrom = make([]int64, n)
	blind := false
	for p := range int32(n) {
		end, ok1 := cends.Lookup(b.cmdTopic, p)
		start, ok2 := starts.Lookup(b.cmdTopic, p)
		from, ok3 := after.Lookup(b.cmdTopic, p)
		if !ok1 || !ok2 || !ok3 || end.Err != nil || start.Err != nil || from.Err != nil {
			return fmt.Errorf("command offsets for partition %d unavailable", p)
		}
		scanFrom := from.Offset
		if scanFrom < 0 || scanFrom > end.Offset { // no record at or after windowStart
			scanFrom = end.Offset
		}
		if b.sawCommandTopicAbsent {
			// The topic did not exist when this bridge started: nothing in
			// it can have been executed by a bridge before (any bridge
			// serves only after writing a layout, and none was found; a
			// concurrent one that wrote first wins below). The memory
			// starts at the topic's beginning, so a gateway that accepts
			// traffic before this genesis loses nothing.
			current.MemoryStart[p] = start.Offset
			current.ScanFrom[p] = start.Offset
			continue
		}
		current.MemoryStart[p] = end.Offset
		current.ScanFrom[p] = max(scanFrom, start.Offset)
		// Complete iff nothing was deleted, or a retained record predates
		// the window (so whatever was deleted predates it too).
		if start.Offset > 0 && scanFrom <= start.Offset {
			blind = true
		}
	}
	if blind {
		current.BlindUntil = now.Add(b.idemWindow()).UTC()
	}
	gid := make([]byte, 8)
	_, _ = rand.Read(gid)
	current.Genesis = hex.EncodeToString(gid)
	v, err := json.Marshal(current)
	if err != nil {
		return err
	}
	// One key per genesis: compaction keeps them all, so "earliest wins"
	// survives it.
	if err := b.base.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(layoutKey + "/" + current.Genesis), Value: v}).FirstErr(); err != nil {
		return err
	}
	// Concurrent first runs: the earliest layout record wins for everyone.
	recorded, found, err := b.readLayout(ctx, topic)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("layout written but not readable")
	}
	b.layout = recorded
	b.log.Warn("dedup memory started: commands already in the topic will not be executed",
		"memoryStart", recorded.MemoryStart, "scanFrom", recorded.ScanFrom, "blindUntil", recorded.BlindUntil, "genesis", recorded.Genesis)
	return nil
}

func (b *Bridge) readLayout(ctx context.Context, topic string) (layout, bool, error) {
	ends, err := b.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return layout{}, false, err
	}
	end, ok := ends.Lookup(topic, 0)
	if !ok || end.Err != nil {
		return layout{}, false, fmt.Errorf("layout end offset: %v", end.Err)
	}
	if end.Offset == 0 {
		return layout{}, false, nil
	}
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(b.cfg.Brokers,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))...)
	if err != nil {
		return layout{}, false, err
	}
	defer cl.Close()
	var (
		l     layout
		found bool
		perr  error
	)
	for last := int64(-1); last < end.Offset-1; {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return layout{}, false, err
		}
		if err := fs.Err(); err != nil {
			return layout{}, false, err
		}
		fs.EachRecord(func(r *kgo.Record) {
			last = r.Offset
			if strings.HasPrefix(string(r.Key), layoutKey) && !found && perr == nil {
				perr = json.Unmarshal(r.Value, &l)
				found = perr == nil
			}
		})
	}
	if perr != nil {
		return layout{}, false, &layoutError{"unreadable layout record: " + perr.Error()}
	}
	return l, found, nil
}

// verifyState re-checks, before a partition restores its state, that the
// state topic is still the one the layout names and still compact-only.
func (b *Bridge) verifyState(ctx context.Context) error {
	err := b.checkState(ctx)
	// Fatal only when the broker answered and the answer proves the state
	// broken; when it did not answer, the partition open is retried.
	var f fatal
	if errors.As(err, &f) {
		b.abandon(err)
	}
	return err
}

func (b *Bridge) checkState(ctx context.Context) error {
	td, err := b.admin.ListTopics(ctx, b.stateTopic)
	if err != nil {
		return err // no answer
	}
	d, ok := td[b.stateTopic]
	switch {
	case !ok || errors.Is(d.Err, kerr.UnknownTopicOrPartition):
		return &layoutError{"the dedup state topic " + b.stateTopic + " was deleted while running"}
	case d.Err != nil:
		return fmt.Errorf("state topic %s unavailable: %w", b.stateTopic, d.Err) // e.g. no leader yet
	case d.ID.String() != b.stateTopicID:
		return &layoutError{"the dedup state topic " + b.stateTopic + " was deleted and recreated while running"}
	}
	return b.checkStateConfig(ctx)
}

// checkStateConfig refuses topic settings under which Kafka itself could
// drop committed dedup entries: retention by time or size, or a commit that
// a single broker failure can erase. Losing a "started" marker means the next
// owner calls B again.
func (b *Bridge) checkStateConfig(ctx context.Context) error {
	rcs, err := b.admin.DescribeTopicConfigs(ctx, b.stateTopic)
	if err != nil {
		return err
	}
	rc, err := rcs.On(b.stateTopic, nil)
	if err != nil {
		return err
	}
	if rc.Err != nil {
		return fmt.Errorf("state topic configs: %w", rc.Err) // existence is judged by checkState
	}
	minISR, unclean := 1, false
	for _, c := range rc.Configs {
		if c.Value == nil {
			continue
		}
		switch c.Key {
		case "cleanup.policy":
			if *c.Value != "compact" {
				return &layoutError{fmt.Sprintf("%s has cleanup.policy=%s: time or size retention would delete dedup entries; it must be compact only", b.stateTopic, *c.Value)}
			}
		case "unclean.leader.election.enable":
			unclean = *c.Value == "true"
		case "min.insync.replicas":
			minISR, _ = strconv.Atoi(*c.Value)
		}
	}
	md, err := b.admin.BrokerMetadata(ctx)
	if err != nil {
		return err
	}
	td, err := b.admin.ListTopics(ctx, b.stateTopic)
	if err != nil {
		return err
	}
	rf := 0
	for _, p := range td[b.stateTopic].Partitions {
		if rf == 0 || len(p.Replicas) < rf {
			rf = len(p.Replicas)
		}
	}
	if problem := durabilityProblem(len(md.Brokers), rf, minISR, unclean); problem != "" {
		return &layoutError{b.stateTopic + ": " + problem}
	}
	if len(md.Brokers) == 1 && !b.warnedSingleBroker {
		b.warnedSingleBroker = true
		b.log.Warn("single-broker cluster: the dedup state lives on one disk; losing it can make B run twice. Acceptable for development only")
	}
	return nil
}

// durabilityProblem tells whether an acknowledged commit to the state topic
// survives the failure of any one broker. A one-broker cluster cannot
// replicate at all: that is a development setup, allowed with a warning,
// and distinguishable from a misconfigured production cluster by a fact the
// bridge can read (the broker count), not by a flag someone may forget.
func durabilityProblem(brokers, rf, minISR int, unclean bool) string {
	switch {
	case unclean:
		return "unclean leader election is enabled: a lagging replica may become leader and drop committed entries"
	case brokers <= 1:
		return ""
	case rf < 2:
		return fmt.Sprintf("replication factor %d on a %d-broker cluster: one broker failure loses committed entries", rf, brokers)
	case minISR < 2:
		return fmt.Sprintf("min.insync.replicas=%d: a commit acknowledged by a single replica is lost if that broker fails before followers copy it; set it to 2 or more", minISR)
	}
	return ""
}
func (b *Bridge) replyTopicExists(topic string) bool {
	b.replyMu.Lock()
	ok := b.replyTopics[topic]
	b.replyMu.Unlock()
	if ok {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	td, err := b.admin.ListTopics(ctx, topic)
	if err != nil {
		// Unsure: try to send. A real failure fails the transaction and
		// the record is retried after recovery.
		return true
	}
	d, found := td[topic]
	if !found || d.Err != nil {
		return false
	}
	b.replyMu.Lock()
	b.replyTopics[topic] = true
	b.replyMu.Unlock()
	return true
}

func (b *Bridge) assigned(_ context.Context, cl *kgo.Client, m map[string][]int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range m[b.cmdTopic] {
		if int(p) >= b.partitions {
			// Partitions were added while running: see LayoutTopic.
			err := &layoutError{fmt.Sprintf("command partitions grew beyond %d while running", b.partitions)}
			b.log.Error("partition beyond the recorded layout assigned; stopping", "partition", p, "err", err)
			b.abandon(err)
			continue
		}
		if old := b.workers[p]; old != nil {
			b.mu.Unlock()
			old.halt(true)
			b.mu.Lock()
		}
		w := newPartition(b, cl, p)
		b.workers[p] = w
		go w.run()
	}
	b.log.Info("partitions assigned", "partitions", m[b.cmdTopic])
	cl.ResumeFetchPartitions(m)
}

func (b *Bridge) revoked(ctx context.Context, cl *kgo.Client, m map[string][]int32) {
	b.stop(m[b.cmdTopic], true)
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		b.log.Warn("offset hint commit on revoke failed", "err", err)
	}
}

func (b *Bridge) lost(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	b.stop(m[b.cmdTopic], false)
}

func (b *Bridge) stop(ps []int32, graceful bool) {
	var ws []*partition
	b.mu.Lock()
	for _, p := range ps {
		if w := b.workers[p]; w != nil {
			ws = append(ws, w)
			delete(b.workers, p)
		}
	}
	b.mu.Unlock()
	for _, w := range ws {
		w.halt(graceful)
	}
	if len(ps) > 0 {
		b.log.Info("partitions released", "partitions", ps, "graceful", graceful)
	}
}

func (b *Bridge) stopAll(graceful bool) {
	b.mu.Lock()
	var ps []int32
	for p := range b.workers {
		ps = append(ps, p)
	}
	b.mu.Unlock()
	b.stop(ps, graceful)
}

// halt stops the worker. Graceful lets the record in progress finish (its
// call to B and the commit of its outcome) so a normal rebalance never turns
// into an unknown outcome; otherwise in-flight work is abandoned and left to
// the next owner.
func (w *partition) halt(graceful bool) {
	if !graceful {
		w.end()
	}
	close(w.stop)
	<-w.done
	w.end()
}
