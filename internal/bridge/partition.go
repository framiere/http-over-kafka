package bridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// partition owns one command partition: its transactional producer, its slice
// of the dedup store, and the processing of its records.
//
// Records are processed concurrently across dedup keys and strictly in order
// within one (a "lane" per key): only commands sharing a key can conflict,
// and a slow call to B holds back nothing but its own key. All writes go
// through one committer goroutine that groups concurrent requests into one
// transaction.
//
// The safety argument, in order:
//  1. Every commit goes through a producer whose transactional id is bound to
//     the partition. Opening one (open) bumps the epoch: every earlier
//     producer of that partition, in this process or a zombie elsewhere, is
//     fenced, and its open transaction is resolved before open returns.
//  2. open then commits a fence record and reads the state partition up to
//     it. Everything any previous owner ever committed is below the fence, so
//     the restored store is complete. Each successful open starts a new store
//     generation; a commit request decided against an older generation is
//     refused and its lane decides again against the restored truth.
//  3. B is called for a non-replayable command only after a "started" entry
//     for it has been committed by the current epoch. Any later owner sees
//     that entry and answers outcome_unknown instead of calling B again.
//  4. Response, Result, the entry's final state and the persisted offset
//     commit in one transaction: they exist together or not at all. The
//     persisted offset is a low watermark: every record below it is settled.
type partition struct {
	b    *Bridge
	cons *kgo.Client // the group consumer that assigned this partition
	id   int32
	ctx  context.Context // canceled when the partition is lost: abandon work
	end  context.CancelFunc
	// stop asks for a graceful exit: each lane finishes its current record.
	stop chan struct{}
	done chan struct{}

	reqs  chan *commitReq
	cgone chan struct{} // closed when the committer has exited
	slots chan struct{} // MaxInFlight

	mu       sync.Mutex
	st       store
	gen      uint64        // store generation, bumped by each successful open
	genCh    chan struct{} // closed when gen changes
	lanes    map[string]*lane
	lanesWG  sync.WaitGroup
	inflight map[int64]struct{} // dispatched, not settled
	maxSeen  int64
	stopping bool
	paused   bool

	// committer goroutine only
	prod      *kgo.Client
	purgeNext func() (string, bool)
	purgeStop func()
}

func newPartition(b *Bridge, cons *kgo.Client, id int32) *partition {
	ctx, cancel := context.WithCancel(context.Background())
	return &partition{
		b: b, cons: cons, id: id, ctx: ctx, end: cancel,
		stop: make(chan struct{}), done: make(chan struct{}),
		reqs: make(chan *commitReq, 1024), cgone: make(chan struct{}), slots: make(chan struct{}, b.cfg.MaxInFlight),
		genCh: make(chan struct{}), lanes: map[string]*lane{}, inflight: map[int64]struct{}{}, maxSeen: -1,
	}
}

// lane holds the pending records of one dedup key.
type lane struct {
	key  string
	recs []*kgo.Record
	cur  *attempt
}

// attempt is what this process knows about the record a lane is working on.
// It survives a new store generation, not a restart.
type attempt struct {
	offset    int64
	requestID string
	id        string // written into "started"; recognizes our own marker
	called    bool   // B may have received the request
	resp      *wire.Response
}

func newAttemptID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type commitReq struct {
	gen       uint64
	requestID string
	out       []*kgo.Record
	writes    map[string]*entry
	offset    int64 // the record this settles; -1 for a "started" marker
	err       chan error
}

var (
	errStaleGen = errors.New("store generation changed; decide again")
	errStopped  = errors.New("partition stopped")
)

func (w *partition) stopped() bool {
	select {
	case <-w.stop:
		return true
	case <-w.ctx.Done():
		return true
	default:
		return false
	}
}

func (w *partition) run() {
	defer close(w.done)
	cctx, ccancel := context.WithCancel(context.Background())
	defer ccancel()
	go w.commitLoop(cctx)
	select {
	case <-w.stop:
	case <-w.ctx.Done():
	}
	w.mu.Lock()
	w.stopping = true
	w.mu.Unlock()
	w.lanesWG.Wait()
	ccancel()
	<-w.cgone
}

// push dispatches fetched records to their lanes.
func (w *partition) push(recs []*kgo.Record) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping {
		return
	}
	for _, rec := range recs {
		w.inflight[rec.Offset] = struct{}{}
		w.maxSeen = max(w.maxSeen, rec.Offset)
		key := string(rec.Key)
		l := w.lanes[key]
		if l == nil {
			l = &lane{key: key}
			w.lanes[key] = l
			w.lanesWG.Add(1)
			go w.runLane(l)
		}
		l.recs = append(l.recs, rec)
	}
	if len(w.inflight) > highWater && !w.paused {
		w.paused = true
		w.cons.PauseFetchPartitions(map[string][]int32{w.b.cmdTopic: {w.id}})
	}
}

func (w *partition) runLane(l *lane) {
	defer w.lanesWG.Done()
	ready := w.waitGen(0)
	for {
		w.mu.Lock()
		if !ready || w.stopping || len(l.recs) == 0 {
			// Records left behind are not settled: the watermark stays
			// below them and the next owner reads them again.
			delete(w.lanes, l.key)
			w.mu.Unlock()
			return
		}
		rec := l.recs[0]
		l.recs[0] = nil
		l.recs = l.recs[1:]
		w.mu.Unlock()
		select {
		case w.slots <- struct{}{}:
		case <-w.stop:
			ready = false
			continue
		case <-w.ctx.Done():
			ready = false
			continue
		}
		if !w.handle(l, rec) {
			ready = false
		}
		<-w.slots
	}
}

// handle processes rec until its outcome is durably recorded. A refused or
// failed commit means the store may not be what we decided against: wait
// for the committer to reload it from Kafka, then decide again.
func (w *partition) handle(l *lane, rec *kgo.Record) bool {
	for {
		w.mu.Lock()
		g := w.gen
		w.mu.Unlock()
		err := w.process(l, rec, g)
		if err == nil {
			l.cur = nil
			w.settle(rec)
			return true
		}
		if errors.Is(err, errStopped) {
			return false
		}
		if !errors.Is(err, errStaleGen) {
			w.b.log.Error("processing failed; waiting for the partition to recover", "partition", w.id, "offset", rec.Offset, "err", err)
		}
		if !w.waitGen(g) {
			return false
		}
	}
}

func (w *partition) settle(rec *kgo.Record) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.inflight, rec.Offset)
	if w.paused && len(w.inflight) < lowWater {
		w.paused = false
		w.cons.ResumeFetchPartitions(map[string][]int32{w.b.cmdTopic: {w.id}})
	}
}

// waitGen blocks until the store generation is past g.
func (w *partition) waitGen(g uint64) bool {
	for {
		w.mu.Lock()
		if w.gen > g {
			w.mu.Unlock()
			return true
		}
		ch := w.genCh
		w.mu.Unlock()
		select {
		case <-ch:
		case <-w.stop:
			return false
		case <-w.ctx.Done():
			return false
		case <-w.cgone:
			return false
		}
	}
}

func (w *partition) entry(key string) *entry {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.entries[key]
}

func (w *partition) submit(r *commitReq) error {
	r.err = make(chan error, 1)
	select {
	case w.reqs <- r:
	case <-w.cgone:
		return errStopped
	}
	select {
	case err := <-r.err:
		return err
	case <-w.cgone:
		return errStopped
	}
}

// --- committer ---

func (w *partition) commitLoop(ctx context.Context) {
	defer close(w.cgone)
	defer w.closeProducer()
	defer w.stopPurge()
	if !w.reopen(ctx) {
		return
	}
	// Sweep restored state immediately. Between sweeps, wait the configured
	// interval; between bounded chunks, yield to commands without waiting.
	purgeTimer := time.NewTimer(0)
	defer purgeTimer.Stop()
	for {
		var batch []*commitReq
		select {
		case r := <-w.reqs:
			batch = append(batch, r)
		case <-purgeTimer.C:
			if w.purgeNext == nil {
				w.startPurge()
			}
		case <-ctx.Done():
			return
		}
		purging := w.purgeNext != nil
	drain:
		for len(batch) < 256 {
			select {
			case r := <-w.reqs:
				batch = append(batch, r)
			default:
				break drain
			}
		}
		w.mu.Lock()
		g := w.gen
		w.mu.Unlock()
		var valid []*commitReq
		for _, r := range batch {
			if r.gen != g {
				r.err <- errStaleGen
				continue
			}
			valid = append(valid, r)
		}
		if len(valid) == 0 && !purging {
			continue
		}
		if err := w.commitBatch(valid); err != nil {
			w.b.log.Error("commit failed; reopening partition", "partition", w.id, "requests", len(valid), "err", err)
			for _, r := range valid {
				r.err <- err
			}
			if !w.reopen(ctx) {
				return
			}
			purgeTimer.Reset(0) // retry against restored, committed state
			continue
		}
		if purging {
			if w.purgeNext != nil {
				purgeTimer.Reset(0)
			} else {
				purgeTimer.Reset(w.b.cfg.PurgeInterval)
			}
		}
		for _, r := range valid {
			r.err <- nil
		}
	}
}

func (w *partition) reopen(ctx context.Context) bool {
	for backoff := 100 * time.Millisecond; ; backoff = min(backoff*2, 5*time.Second) {
		if w.stopped() || ctx.Err() != nil {
			return false
		}
		w.closeProducer()
		st, err := w.open()
		if err == nil {
			w.stopPurge() // an iterator must never outlive its store generation
			w.mu.Lock()
			w.st = st
			w.gen++
			close(w.genCh)
			w.genCh = make(chan struct{})
			w.mu.Unlock()
			return true
		}
		w.b.log.Error("partition open failed", "partition", w.id, "err", err, "retryIn", backoff)
		select {
		case <-time.After(backoff):
		case <-w.stop:
			return false
		case <-w.ctx.Done():
			return false
		case <-ctx.Done():
			return false
		}
	}
}

func (w *partition) closeProducer() {
	if w.prod != nil {
		w.prod.Close()
		w.prod = nil
	}
}

func (w *partition) open() (store, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(w.ctx, w.b.cfg.RestoreTimeout)
	defer cancel()
	if err := w.b.verifyState(ctx); err != nil {
		return store{}, err
	}
	prod, err := kgo.NewClient(kafkaenv.ClientOpts(w.b.cfg.Brokers,
		kgo.TransactionalID(fmt.Sprintf("hok-bridge.%s.%d", w.b.cfg.Service, w.id)),
		kgo.TransactionTimeout(w.b.cfg.TxnTimeout),
		kgo.RecordPartitioner(partitioner{kgo.StickyKeyPartitioner(nil)}),
		kgo.ProducerLinger(0),
		// Back-to-back transactions hit CONCURRENT_TRANSACTIONS while the
		// previous markers are written (about a millisecond); the 20ms
		// default retry interval would be the whole latency tail.
		kgo.ConcurrentTransactionsBackoff(2*time.Millisecond),
	)...)
	if err != nil {
		return store{}, err
	}
	w.prod = prod

	fence, err := stateRecord(w.b.cfg.Signer, w.b.stateTopic, w.id, keyFence, fenceValue{Instance: w.b.cfg.Instance, At: time.Now().UTC()})
	if err != nil {
		return store{}, err
	}
	if err := prod.BeginTransaction(); err != nil {
		return store{}, err
	}
	if err := prod.ProduceSync(ctx, fence).FirstErr(); err != nil {
		w.abort()
		return store{}, fmt.Errorf("fence: %w", err)
	}
	if err := prod.EndTransaction(ctx, kgo.TryCommit); err != nil {
		w.abort()
		return store{}, fmt.Errorf("fence commit: %w", err)
	}

	rc, err := kgo.NewClient(kafkaenv.ClientOpts(w.b.cfg.Brokers,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{w.b.stateTopic: {w.id: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)...)
	if err != nil {
		return store{}, err
	}
	defer rc.Close()
	st := store{entries: map[string]*entry{}}
	for reached := false; !reached; {
		fs := rc.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return store{}, fmt.Errorf("restore: %w", err)
		}
		var ferr error
		fs.EachError(func(_ string, _ int32, err error) { ferr = errors.Join(ferr, err) })
		if ferr != nil {
			return store{}, fmt.Errorf("restore: %w", ferr)
		}
		for _, r := range fs.Records() {
			if r.Offset >= fence.Offset {
				reached = true
				break
			}
			if err := verifyStateRecord(w.b.cfg.StateKeys, r); err != nil {
				w.b.abandon(&layoutError{err.Error()})
				return store{}, err
			}
			if err := st.apply(r); err != nil {
				return store{}, err
			}
		}
	}
	if g := w.b.layout.Genesis; g != "" && st.genesis != g {
		if err := w.genesisClaims(ctx, &st); err != nil {
			return store{}, fmt.Errorf("genesis scan: %w", err)
		}
	}
	w.b.log.Info("partition ready", "partition", w.id, "entries", len(st.entries), "next", st.next, "fence", fence.Offset, "took", time.Since(start))
	return st, nil
}

// genesisClaims runs once per partition and dedup memory: it reads the
// commands of the idempotency window that predate the memory, on every
// command partition (they were hashed with whatever partition count held
// then), and claims as "outcome unknown" each idempotency scope that now
// belongs to this partition and has no entry. A retry of a key used before
// the memory began thus gets "unknown" instead of executing B.
func (w *partition) genesisClaims(ctx context.Context, st *store) error {
	l := w.b.layout
	offsets := map[int32]kgo.Offset{}
	pending := map[int32]int64{} // partition -> last offset to read
	for p := range int32(len(l.MemoryStart)) {
		if l.ScanFrom[p] < l.MemoryStart[p] {
			offsets[p] = kgo.NewOffset().At(l.ScanFrom[p])
			pending[p] = l.MemoryStart[p] - 1
		}
	}
	claims := map[string]*entry{}
	if len(offsets) > 0 {
		cl, err := kgo.NewClient(kafkaenv.ClientOpts(w.b.cfg.Brokers,
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{w.b.cmdTopic: offsets}),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		)...)
		if err != nil {
			return err
		}
		defer cl.Close()
		for len(pending) > 0 {
			fs := cl.PollFetches(ctx)
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fs.Err(); err != nil {
				return err
			}
			for _, r := range fs.Records() {
				last, ok := pending[r.Partition]
				if !ok || r.Offset > last {
					continue
				}
				if r.Offset == last {
					delete(pending, r.Partition)
				}
				// Claims are taken even from commands that no longer verify
				// (signed by a retired gateway key, or forged): a claim only
				// ever withholds execution, so a wrong one costs a refusal,
				// a missing one a double effect.
				cmd, err := w.b.verifier.Verify(r)
				if err != nil && !errors.Is(err, wire.ErrStale) {
					if cmd, err = wire.DecodeCommandUnverified(r); err != nil {
						continue
					}
				}
				key := cmd.DedupKey()
				if cmd.IdempotencyKey == "" || w.b.expectedPartition([]byte(key)) != w.id || st.entries[key] != nil || claims[key] != nil {
					continue
				}
				resp := wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown,
					"this Idempotency-Key was used before the bridge's dedup memory began; its outcome is unknown and it is not executed again")
				claims[key] = &entry{RequestID: cmd.RequestID, Fingerprint: cmd.Fingerprint(), Phase: done, Outcome: wire.OutcomeUnknown,
					Response: &resp, Unanswered: true, PurgeAfter: cmd.IssuedAt.Add(w.b.idemWindow()).UTC()}
			}
		}
	}
	// Claims and the marker commit together, in bounded transactions; the
	// marker goes last, so an interrupted scan is redone.
	keys := make([]string, 0, len(claims))
	for k := range claims {
		keys = append(keys, k)
	}
	for i := 0; ; i += 256 {
		var recs []*kgo.Record
		for _, k := range keys[i:min(i+256, len(keys))] {
			r, err := stateRecord(w.b.cfg.Signer, w.b.stateTopic, w.id, k, claims[k])
			if err != nil {
				return err
			}
			recs = append(recs, r)
		}
		last := i+256 >= len(keys)
		if last {
			gr, err := stateRecord(w.b.cfg.Signer, w.b.stateTopic, w.id, keyGenesis, l.Genesis)
			if err != nil {
				return err
			}
			recs = append(recs, gr)
		}
		if err := w.prod.BeginTransaction(); err != nil {
			return err
		}
		if err := w.prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			w.abort()
			return err
		}
		if err := w.prod.EndTransaction(ctx, kgo.TryCommit); err != nil {
			w.abort()
			return err
		}
		if last {
			break
		}
	}
	for k, e := range claims {
		st.entries[k] = e
	}
	st.genesis = l.Genesis
	w.b.log.Info("genesis claims written", "partition", w.id, "claims", len(claims), "genesis", l.Genesis)
	return nil
}

// abort is best effort: a failed abort leaves an open transaction that the
// next open (same transactional id) aborts, or the transaction timeout does.
func (w *partition) abort() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = w.prod.AbortBufferedRecords(ctx)
	_ = w.prod.EndTransaction(ctx, kgo.TryAbort)
}

// commitBatch writes the requests of one store generation in one
// transaction. The in-memory store changes only once the commit is
// confirmed; on error the committer reopens from Kafka.
func (w *partition) commitBatch(reqs []*commitReq) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.b.cfg.TxnTimeout)
	defer cancel()
	var recs []*kgo.Record
	writes := map[string]*entry{}
	settling := map[int64]bool{}
	for _, r := range reqs {
		recs = append(recs, r.out...)
		for k, e := range r.writes {
			writes[k] = e
			sr, err := stateRecord(w.b.cfg.Signer, w.b.stateTopic, w.id, k, e)
			if err != nil {
				return err
			}
			recs = append(recs, sr)
		}
		if r.offset >= 0 {
			settling[r.offset] = true
		}
	}
	w.mu.Lock()
	next := w.watermark(settling)
	purged := w.expired(writes)
	w.mu.Unlock()
	if next > w.st.next {
		sr, err := stateRecord(w.b.cfg.Signer, w.b.stateTopic, w.id, keyOffset, offsetValue{Next: next})
		if err != nil {
			return err
		}
		recs = append(recs, sr)
	}
	for _, k := range purged {
		sr, _ := stateRecord(w.b.cfg.Signer, w.b.stateTopic, w.id, k, nil)
		recs = append(recs, sr)
	}

	if len(recs) == 0 {
		return nil // a scan-only chunk needs no Kafka transaction
	}
	if err := w.prod.BeginTransaction(); err != nil {
		return err
	}
	if err := w.prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		w.abort()
		return err
	}
	for _, r := range reqs {
		if len(r.out) > 0 {
			w.b.cfg.Hooks.beforeCommit(r.requestID)
		} else if r.offset < 0 {
			w.b.cfg.Hooks.beforeStartedCommit(r.requestID)
		}
	}
	if err := w.prod.EndTransaction(ctx, kgo.TryCommit); err != nil {
		w.abort()
		return err
	}
	w.mu.Lock()
	for k, e := range writes {
		w.st.entries[k] = e
	}
	for _, k := range purged {
		delete(w.st.entries, k)
	}
	if next > w.st.next {
		w.st.next = next
		// Resume hint only; the authoritative position is st.next.
		w.cons.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{w.b.cmdTopic: {w.id: {Epoch: -1, Offset: next}}})
	}
	w.mu.Unlock()
	for _, r := range reqs {
		if len(r.out) > 0 {
			w.b.cfg.Hooks.afterCommit(r.requestID)
		}
	}
	return nil
}

// watermark is the lowest offset not settled once the records in settling
// are: everything below it is done. Caller holds mu.
func (w *partition) watermark(settling map[int64]bool) int64 {
	next := w.maxSeen + 1
	for o := range w.inflight {
		if !settling[o] && o < next {
			next = o
		}
	}
	return max(next, w.st.next)
}

// Bound examined keys as well as tombstones: a large store of live entries
// must not monopolize mu or the committer. A sweep continues in successive
// chunks, including when no commands arrive.
const purgeBatchSize = 512

func (w *partition) startPurge() {
	// Only the committer writes entries. The suspended map iterator tolerates
	// insertions/deletions between chunks; newly inserted or skipped keys are
	// reconsidered by the next sweep. Every next call holds mu.
	w.purgeNext, w.purgeStop = iter.Pull(maps.Keys(w.st.entries))
}

func (w *partition) stopPurge() {
	if w.purgeStop != nil {
		w.purgeStop()
	}
	w.purgeNext, w.purgeStop = nil, nil
}

// expired selects a bounded chunk without changing the store. Entries are
// deleted only after their signed tombstones commit; a failed transaction
// reopens the store and restarts the sweep from committed truth. An entry is
// purgeable only after its command can no longer pass verification, so a
// redelivery is answered "stale", never executed. Caller holds mu.
func (w *partition) expired(writing map[string]*entry) []string {
	if w.purgeNext == nil {
		return nil
	}
	now := w.b.now()
	var keys []string
	for range purgeBatchSize {
		k, ok := w.purgeNext()
		if !ok {
			w.stopPurge()
			break
		}
		e := w.st.entries[k]
		_, rewritten := writing[k]
		_, busy := w.lanes[k]
		if rewritten || busy || e.Phase != done || !now.After(e.PurgeAfter) || strings.HasPrefix(k, "#") {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

// --- decisions (lane goroutines) ---

// answeredKey marks a request answered without owning its dedup entry
// (replays, rejections): a redelivery must not answer or record it twice.
func answeredKey(requestID string) string { return "q:" + requestID }

func (w *partition) attemptFor(l *lane, rec *kgo.Record, cmd wire.Command) *attempt {
	if l.cur == nil || l.cur.offset != rec.Offset || l.cur.requestID != cmd.RequestID {
		l.cur = &attempt{offset: rec.Offset, requestID: cmd.RequestID, id: newAttemptID()}
	}
	return l.cur
}

func (w *partition) process(l *lane, rec *kgo.Record, g uint64) error {
	w.mu.Lock()
	below := rec.Offset < w.st.next
	w.mu.Unlock()
	if below {
		return nil // settled, by us or a previous owner
	}
	// Records of one dedup scope must meet in one partition, or two owners
	// would run the same command concurrently. The key is signed; the
	// partition is not, so it is checked.
	if want := w.b.expectedPartition(rec.Key); want != rec.Partition {
		w.b.log.Error("command on the wrong partition dropped: retries of its scope could run concurrently elsewhere",
			"partition", rec.Partition, "want", want, "offset", rec.Offset, "key", string(rec.Key))
		return nil
	}
	cmd, verr := w.b.verifier.Verify(rec)
	if verr != nil && !errors.Is(verr, wire.ErrStale) {
		// ReplyTo is untrusted: no reply, no result, B not called.
		w.b.log.Warn("command not authentic, dropped", "partition", rec.Partition, "offset", rec.Offset, "err", verr)
		w.b.dropped.Add(1)
		return nil
	}
	if w.entry(answeredKey(cmd.RequestID)) != nil {
		return nil
	}
	key := cmd.DedupKey()
	e := w.entry(key)
	cur := w.attemptFor(l, rec, cmd)

	// Older than the dedup memory: a bridge whose state is gone may have
	// executed it, and nothing here can prove otherwise. Never execute;
	// answer unknown, and claim the idempotency scope so that a retry with
	// the same key gets this answer instead of running B.
	if w.b.beforeMemory(rec) {
		if e != nil && e.RequestID == cmd.RequestID && e.Phase == done && !e.Unanswered {
			return nil
		}
		w.b.log.Warn("command predates the dedup memory, not executed", "requestId", cmd.RequestID, "partition", rec.Partition, "offset", rec.Offset)
		return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown,
			"the command predates the bridge's dedup memory; it may have been applied by a previous deployment and was not executed again"),
			e == nil || e.RequestID == cmd.RequestID || !e.claims(false), g)
	}

	if e != nil && e.RequestID == cmd.RequestID {
		if e.Phase == done {
			return nil // redelivery of a command whose outcome is already published
		}
		// A replayable operation can have completed after inheriting another
		// owner's started marker. Preserve that response if its outcome commit
		// failed; only a durable done entry above supersedes what we know.
		if cur.resp != nil {
			return w.complete(rec, cmd, *cur.resp, true, g)
		}
		// "started": B was, or may have been, called for this very command.
		if e.Attempt == cur.id {
			if !cur.called {
				if op, err := w.b.spec.Check(cmd); err == nil {
					return w.execute(rec, cmd, op, cur, g)
				}
			}
		}
		if op, err := w.b.spec.Check(cmd); err == nil && op.Replayable() && verr == nil {
			w.b.log.Warn("resuming a replayable operation whose outcome is unknown", "requestId", cmd.RequestID, "operation", op.ID)
			cur.called = true // a restored marker means an earlier attempt may have run
			return w.execute(rec, cmd, op, cur, g)
		}
		w.b.log.Warn("outcome unknown: B may have executed before a crash or takeover", "requestId", cmd.RequestID, "operation", cmd.OperationID)
		return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown,
			"the bridge stopped after the request may have reached the service; it was not retried"), true, g)
	}

	// An outcome commit may have failed after B answered. In particular,
	// replayable operations have no "started" entry to find above. Expiry
	// forbids another call, but cannot change the outcome we already know.
	if cur.resp != nil {
		ownEntry := e == nil
		if e != nil {
			op, err := w.b.spec.Check(cmd)
			ownEntry = !e.claims(err == nil && op.Replayable())
		}
		return w.complete(rec, cmd, *cur.resp, ownEntry, g)
	}
	if verr != nil {
		if cur.called {
			return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown,
				"the command is no longer valid; an earlier attempt may have reached the service and was not retried"), e == nil, g)
		}
		return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultCommandStale, verr.Error()), e == nil, g)
	}
	if redacted, found := w.b.redactSecrets(cmd); len(found) > 0 {
		w.b.log.Error("command carries a credential declared by the contract; rejected without calling B",
			"requestId", cmd.RequestID, "locations", found)
		return w.complete(rec, redacted, wire.FaultResponse(cmd.RequestID, wire.FaultSecretInCommand,
			"the request carried "+strings.Join(found, ", ")), e == nil, g)
	}
	op, err := w.b.spec.Check(cmd)
	if err != nil {
		return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultOperationMismatch, err.Error()), e == nil, g)
	}
	if e != nil {
		// Another request of the same idempotency scope came first.
		if e.Fingerprint != cmd.Fingerprint() {
			return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultIdempotencyKeyReused,
				"this Idempotency-Key was first used with a different request (method, path, query, content-type or body)"), false, g)
		}
		if e.claims(op.Replayable()) {
			if e.Phase == done && e.Response != nil {
				return w.replay(rec, cmd, e, g)
			}
			// Unreachable: the lane is sequential, the original's record
			// precedes this one and is settled first.
			return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultIdempotencyInFlight, ""), false, g)
		}
	}
	if cmd.IdempotencyKey != "" && w.b.now().Before(w.b.layout.BlindUntil) {
		// The memory began after commands of the idempotency window were
		// deleted: this key may have been used then. Not executed, not
		// claimed: once the window has passed, it may run.
		return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown,
			"the bridge's idempotency history is incomplete until "+w.b.layout.BlindUntil.Format(time.RFC3339)+
				"; a request with an Idempotency-Key it has not seen is not executed before then"), false, g)
	}
	if cur.called && !op.Replayable() {
		return w.complete(rec, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown, "dedup entry lost after the call"), true, g)
	}
	return w.execute(rec, cmd, op, cur, g)
}

func (w *partition) execute(rec *kgo.Record, cmd wire.Command, op *apispec.Operation, cur *attempt, g uint64) error {
	t := Timing{RequestID: cmd.RequestID, OperationID: op.ID}
	t0 := time.Now()
	key := cmd.DedupKey()
	if e := w.entry(key); !op.Replayable() && (e == nil || e.Attempt != cur.id) {
		e := &entry{RequestID: cmd.RequestID, Fingerprint: cmd.Fingerprint(), Phase: started, Attempt: cur.id, PurgeAfter: w.purgeAfter(cmd)}
		if err := w.submit(&commitReq{gen: g, requestID: cmd.RequestID, writes: map[string]*entry{key: e}, offset: -1}); err != nil {
			return fmt.Errorf("commit started: %w", err)
		}
		w.b.cfg.Hooks.afterStarted(cmd.RequestID)
	}
	t.Started = time.Since(t0)

	t1 := time.Now()
	var resp wire.Response
	didCall := false
	for try := 1; ; try++ {
		// Check at the point of use, after the started transaction and before
		// every retry. Expiration stops new attempts; it cannot undo an earlier
		// call or turn an uncertain outcome into proof of non-execution.
		if err := w.b.verifier.CheckTime(cmd); err != nil {
			if cur.called {
				resp = wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown,
					"the command is no longer valid; an earlier attempt may have reached the service and was not retried")
			} else {
				resp = wire.FaultResponse(cmd.RequestID, wire.FaultCommandStale, err.Error())
			}
			break
		}
		previouslyCalled := cur.called
		cur.called = true
		didCall = true
		resp = w.b.up.call(w.ctx, cmd, op)
		if try > 1 {
			w.b.log.Warn("retried replayable operation after unknown outcome", "requestId", cmd.RequestID, "try", try)
		}
		if previouslyCalled && resp.Fault != "" && resp.Fault.Execution() == wire.ExecutionNone {
			resp = wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown,
				"the retry did not reach the service, but an earlier attempt may have; its outcome remains unknown")
			break
		}
		if resp.Fault != wire.FaultOutcomeUnknown || !op.Replayable() || try >= w.b.cfg.ReplayAttempts || w.ctx.Err() != nil {
			break
		}
	}
	cur.resp = &resp
	t.Upstream = time.Since(t1)
	if didCall {
		w.b.cfg.Hooks.afterUpstream(cmd.RequestID)
	}

	t2 := time.Now()
	if err := w.complete(rec, cmd, resp, true, g); err != nil {
		return err
	}
	t.Complete = time.Since(t2)
	t.Total = time.Since(t0)
	w.b.cfg.Hooks.onTiming(t)
	return nil
}

// complete publishes the outcome of cmd: Response, Result, and either its
// dedup entry (ownEntry) or an "answered" marker, in one transaction.
func (w *partition) complete(rec *kgo.Record, cmd wire.Command, resp wire.Response, ownEntry bool, g uint64) error {
	res, err := wire.NewResult(cmd, resp, w.b.now())
	if err != nil {
		return fmt.Errorf("result: %w", err)
	}
	resultRec, err := wire.EncodeResult(res, w.b.cfg.Signer)
	if err != nil {
		return err
	}
	resultRec.Partition = w.id
	out := []*kgo.Record{resultRec}
	if r, ok := w.responseRecord(resp, cmd.ReplyTo); ok {
		out = append(out, r)
	}
	e := &entry{RequestID: cmd.RequestID, Fingerprint: cmd.Fingerprint(), Phase: done, Outcome: res.Outcome, PurgeAfter: w.purgeAfter(cmd)}
	key := answeredKey(cmd.RequestID)
	if ownEntry {
		key = cmd.DedupKey()
		if cmd.IdempotencyKey != "" {
			e.Response = &resp
		}
	} else {
		e.PurgeAfter = cmd.ExpiresAt.Add(2 * w.b.cfg.ClockSkew).UTC()
	}
	return w.submit(&commitReq{gen: g, requestID: cmd.RequestID, out: out, writes: map[string]*entry{key: e}, offset: rec.Offset})
}

// replay answers an idempotent retry with the stored outcome. B is not called
// and no Result is produced: the original already has one.
func (w *partition) replay(rec *kgo.Record, cmd wire.Command, e *entry, g uint64) error {
	resp := *e.Response
	resp.RequestID = cmd.RequestID
	resp.ReplayOf = e.RequestID
	var out []*kgo.Record
	if r, ok := w.responseRecord(resp, cmd.ReplyTo); ok {
		out = append(out, r)
	}
	w.b.replays.Add(1)
	marker := &entry{RequestID: cmd.RequestID, Fingerprint: cmd.Fingerprint(), Phase: done, Outcome: e.Outcome,
		PurgeAfter: cmd.ExpiresAt.Add(2 * w.b.cfg.ClockSkew).UTC()}
	return w.submit(&commitReq{gen: g, requestID: cmd.RequestID, out: out, writes: map[string]*entry{answeredKey(cmd.RequestID): marker}, offset: rec.Offset})
}

func (w *partition) responseRecord(resp wire.Response, replyTo string) (*kgo.Record, bool) {
	if !w.b.replyTopicExists(replyTo) {
		// The gateway instance is gone: nobody waits on this connection.
		// The Result still records the outcome; a retry gets it replayed.
		w.b.log.Warn("reply topic missing, response not sent", "replyTo", replyTo, "requestId", resp.RequestID)
		return nil, false
	}
	r, err := wire.EncodeResponse(resp, replyTo, w.b.cfg.Signer)
	if err != nil {
		w.b.log.Error("response not encodable, not sent", "requestId", resp.RequestID, "err", err)
		return nil, false
	}
	return r, true
}

func (w *partition) purgeAfter(cmd wire.Command) time.Time {
	t := cmd.ExpiresAt.Add(2 * w.b.cfg.ClockSkew)
	if cmd.IdempotencyKey != "" {
		if r := w.b.now().Add(w.b.cfg.IdempotencyRetention); r.After(t) {
			t = r
		}
	}
	return t.UTC()
}

// partitioner places state and result records on the command's partition
// number (topics are checked at startup to have equal partition counts) and
// hashes everything else (reply topics) the default way.
type partitioner struct{ hashed kgo.Partitioner }

func (p partitioner) ForTopic(t string) kgo.TopicPartitioner {
	if strings.HasPrefix(t, "http.responses.") {
		return p.hashed.ForTopic(t)
	}
	return kgo.ManualPartitioner().ForTopic(t)
}
