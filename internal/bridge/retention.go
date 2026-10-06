package bridge

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// A slow owner accepts through ExpiresAt + S on its own clock. Another
// owner's clock can be 2S ahead, so it must retain the entry through
// ExpiresAt + 3S. expired uses a strict comparison: equality is still live.
const replaySkewFactor = 3

const retentionPolicy = 1

func (b *Bridge) replayMargin() time.Duration { return replaySkewFactor * b.cfg.ClockSkew }

func (b *Bridge) commandPurgeAfter(cmd wire.Command) time.Time {
	return cmd.ExpiresAt.Add(b.replayMargin()).UTC()
}

// Old entries persisted only PurgeAfter, without their command's ExpiresAt.
// Adding S conservatively upgrades every old 2S deadline, including genesis
// claims and answered markers. Mark the in-memory entry so a later rewrite
// persists the upgrade, and every restore from the old bytes adds S only once.
func (b *Bridge) restoreRetention(st *store) error {
	for key, e := range st.entries {
		switch e.RetentionPolicy {
		case 0:
			e.PurgeAfter = e.PurgeAfter.Add(b.cfg.ClockSkew).UTC()
			e.RetentionPolicy = retentionPolicy
		case retentionPolicy:
		default:
			return fmt.Errorf("state %q has unsupported retention policy %d", key, e.RetentionPolicy)
		}
	}
	return nil
}

// Validate after defaults, before any duration arithmetic. Overflow could
// turn a retention window negative and discard the very evidence it protects.
func validateDedupWindow(cfg Config) error {
	if cfg.ClockSkew < 0 || cfg.MaxTTL < 0 || cfg.IdempotencyRetention < 0 {
		return errors.New("bridge: clock skew, maximum TTL and idempotency retention must be positive")
	}
	remaining := time.Duration(math.MaxInt64) - cfg.IdempotencyRetention
	if cfg.MaxTTL > remaining {
		return errors.New("bridge: dedup retention window overflows time.Duration")
	}
	remaining -= cfg.MaxTTL
	if cfg.ClockSkew > remaining/replaySkewFactor {
		return errors.New("bridge: dedup retention window overflows time.Duration")
	}
	return nil
}
