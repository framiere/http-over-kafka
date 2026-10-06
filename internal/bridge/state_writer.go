package bridge

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// stableStateEnd is read only after InitProducerID has fenced the previous
// owner. Never infer a new append position from an unresolved transaction.
func (w *partition) stableStateEnd(ctx context.Context) (int64, error) {
	a := kadm.NewClient(w.prod)
	for {
		ends, err := a.ListEndOffsets(ctx, w.b.stateTopic)
		if err != nil {
			return 0, err
		}
		stable, err := a.ListCommittedOffsets(ctx, w.b.stateTopic)
		if err != nil {
			return 0, err
		}
		e, eok := ends.Lookup(w.b.stateTopic, w.id)
		s, sok := stable.Lookup(w.b.stateTopic, w.id)
		if !eok || !sok || e.Err != nil || s.Err != nil {
			return 0, fmt.Errorf("state append position unavailable: end=%v stable=%v", e.Err, s.Err)
		}
		if e.Offset == s.Offset {
			return e.Offset, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// produceState binds each state record to its physical Kafka position. The
// sole state writer predicts consecutive offsets and one transaction marker.
// This is only an availability assumption: Kafka's returned offsets are
// checked before committing or allowing a non-replayable upstream call.
// Concurrent writes, extra markers or an uncertain commit force a reopen.
func (w *partition) produceState(ctx context.Context, recs []*kgo.Record) error {
	next := w.nextStateOffset
	positions := make(map[*kgo.Record]int64)
	for _, r := range recs {
		if r.Topic == w.b.stateTopic {
			if _, duplicate := positions[r]; duplicate {
				return fmt.Errorf("state record pointer repeated in one transaction")
			}
			if r.Partition != w.id {
				return fmt.Errorf("state write to unexpected partition %d", r.Partition)
			}
			bindStateRecord(w.b.cfg.Signer, r, next)
			positions[r] = next
			next++
		}
	}
	if err := w.prod.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return err
	}
	for r, expected := range positions {
		if r.Offset != expected || r.Partition != w.id {
			return fmt.Errorf("state append position changed: expected %d, received %d; transaction refused", expected, r.Offset)
		}
	}
	if len(positions) > 0 {
		w.nextStateOffset = next + 1 // Kafka's commit control record.
	}
	return nil
}
