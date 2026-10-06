package events

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// One fact, at most one event, whoever wrote the copy (D10 threat model:
// anyone can write to the result topic, and a byte-identical copy of a
// genuine Result verifies).
//
// Two rules make that hold with bounded memory:
//
//   - Routing: a Result is only accepted on the partition the bridge puts it
//     on, the hash of its command's DedupKey (the command's partition number).
//     Every copy of a requestId therefore lands on one partition, owned by one
//     instance at a time: a per-owner set of derived requestIds sees them all.
//   - Window: a Result whose CompletedAt (signed) is older than the window
//     yields no event. Entries can then be forgotten after the window, since
//     any later copy of them is itself too old.
//
// The set is the dedup topic: one record per derived event, written in the
// event's transaction. An instance reloads it whenever its assignment
// changes, before deriving anything, so a crash or a rebalance between the
// original and a copy cannot lose the entry.

// DedupTopic is the changelog of requestIds the deriver has emitted an event
// for. One partition, compacted, kept for the window.
func DedupTopic(service string) string { return "http.event-dedup." + service }

const TypeDedup = "event-dedup.v1"

// DefaultDedupWindow matches Kafka's default retention: a Result older than
// that may be gone from the result topic anyway.
const DefaultDedupWindow = 7 * 24 * time.Hour

func dedupTopic(service string, window time.Duration) kafkaenv.Topic {
	t := kafkaenv.EventTopic(DedupTopic(service), 1)
	policy, retention := "compact,delete", strconv.FormatInt((window+24*time.Hour).Milliseconds(), 10)
	t.Configs["cleanup.policy"] = &policy
	t.Configs["retention.ms"] = &retention
	return t
}

type dedupEntry struct {
	V           int       `json:"v"`
	RequestID   string    `json:"requestId"`
	CompletedAt time.Time `json:"completedAt"`
	Event       string    `json:"event"`  // event topic it went to
	Result      Position  `json:"result"` // the Result it was derived from
}

type dedupSet map[string]dedupEntry

func (s dedupSet) prune(cutoff time.Time) {
	for k, e := range s {
		if e.CompletedAt.Before(cutoff) {
			delete(s, k)
		}
	}
}

func dedupRecord(service string, e dedupEntry) *kgo.Record {
	v, err := json.Marshal(e)
	if err != nil {
		panic(err) // strings, ints, a time
	}
	return &kgo.Record{
		Topic:   DedupTopic(service),
		Key:     []byte(e.RequestID),
		Value:   v,
		Headers: []kgo.RecordHeader{{Key: wire.HeaderType, Value: []byte(TypeDedup)}},
	}
}

// loadDedup reads the committed changelog up to the end offset it has now.
// Reading read_committed up to the high watermark (not the last stable
// offset) waits for transactions open at that moment to finish, so the
// entries of a previous owner whose transaction already committed are seen
// even when a later transaction is still open.
func loadDedup(ctx context.Context, brokers []string, service string, cutoff time.Time) (dedupSet, error) {
	topic := DedupTopic(service)
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(brokers,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.KeepControlRecords(),
	)...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("dedup end offset: %w", err)
	}
	end, ok := ends.Lookup(topic, 0)
	if !ok || end.Err != nil {
		return nil, fmt.Errorf("dedup end offset of %s: %v", topic, end.Err)
	}
	set := dedupSet{}
	for pos := int64(0); pos < end.Offset; {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("dedup load stopped at %d/%d: %w", pos, end.Offset, err)
		}
		var ferr error
		fs.EachError(func(_ string, _ int32, err error) { ferr = err })
		if ferr != nil {
			return nil, ferr
		}
		fs.EachRecord(func(r *kgo.Record) {
			pos = r.Offset + 1
			if r.Attrs.IsControl() {
				return
			}
			var e dedupEntry
			if err := json.Unmarshal(r.Value, &e); err != nil || e.RequestID == "" {
				ferr = fmt.Errorf("dedup record at %d unreadable: %v", r.Offset, err)
				return
			}
			if !e.CompletedAt.Before(cutoff) {
				set[e.RequestID] = e
			}
		})
		if ferr != nil {
			return nil, ferr
		}
	}
	return set, nil
}
