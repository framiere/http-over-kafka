package bridge

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestPurgeRetainsThroughSlowOwnerAcceptanceBoundary(t *testing.T) {
	const skew = 30 * time.Second
	cmd := command("/charges", nil, nil)
	cmd.IdempotencyKey = ""
	gw, err := identity.NewSigner(identity.RoleGateway, "gw", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := wire.EncodeCommand(cmd, gw)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		after  time.Duration
		purged bool
	}{
		{"old boundary crossed", 2*skew + time.Nanosecond, false},
		{"slow owner still accepts at equality", 3 * skew, false},
		{"all owners reject", 3*skew + time.Nanosecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fast := cmd.ExpiresAt.Add(tc.after)
			slow := fast.Add(-2 * skew)
			v := wire.CommandVerifier{Keys: gw.Self(), Service: cmd.Service, ClockSkew: skew,
				Now: func() time.Time { return slow }}
			_, err := v.Verify(rec)
			if (!tc.purged && err != nil) || (tc.purged && !errors.Is(err, wire.ErrStale)) {
				t.Fatalf("slow owner's verification: %v, purged=%v", err, tc.purged)
			}
			w := &partition{b: &Bridge{cfg: Config{ClockSkew: skew, Now: func() time.Time { return fast }}},
				st: store{entries: map[string]*entry{}}, lanes: map[string]*lane{}}
			w.st.entries[cmd.DedupKey()] = &entry{Phase: done, PurgeAfter: w.purgeAfter(cmd)}
			w.startPurge()
			defer w.stopPurge()
			if keys := w.expired(nil); (len(keys) == 1) != tc.purged {
				t.Fatalf("fast owner's purge: %v, want purged=%v", keys, tc.purged)
			}
		})
	}
}

func TestDedupWindowCoversClockMarginAndKeyedRetention(t *testing.T) {
	cmd := command("/charges", nil, nil)
	now := cmd.IssuedAt
	w := &partition{b: &Bridge{cfg: Config{ClockSkew: 30 * time.Second, MaxTTL: time.Hour,
		IdempotencyRetention: 24 * time.Hour, Now: func() time.Time { return now }}}}
	if got, want := w.b.idemWindow(), 25*time.Hour+90*time.Second; got != want {
		t.Fatalf("genesis window %s, want %s", got, want)
	}
	if got, want := w.purgeAfter(cmd), now.Add(24*time.Hour); !got.Equal(want) {
		t.Fatalf("keyed retention %s, want %s", got, want)
	}
	w.b.cfg.IdempotencyRetention = time.Second
	if got, want := w.purgeAfter(cmd), cmd.ExpiresAt.Add(90*time.Second); !got.Equal(want) {
		t.Fatalf("short keyed retention %s, want %s", got, want)
	}
}

func TestNewRejectsUnsafeDedupWindows(t *testing.T) {
	spec, err := apispec.Load("payments", api.Payments)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := identity.NewSigner(identity.RoleGateway, "gw", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	signer, err := identity.NewSigner(identity.RoleBridge, "bridge", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		skew, ttl, r time.Duration
		valid        bool
	}{
		{"defaults", 0, 0, 0, true},
		{"negative skew", -1, 0, 0, false},
		{"negative TTL", 0, -1, 0, false},
		{"negative retention", 0, 0, -1, false},
		{"skew multiplication overflow", time.Duration(math.MaxInt64/3 + 1), 1, 1, false},
		{"retention plus TTL overflow", 1, 1, math.MaxInt64, false},
		{"combined window overflow", 1, 1, math.MaxInt64 - 3, false},
		{"largest supported window", 1, 1, math.MaxInt64 - 4, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New(Config{Service: "payments", Brokers: []string{"127.0.0.1:1"}, Spec: spec,
				Keys: gw.Self(), Signer: signer, Upstream: "http://127.0.0.1:1",
				ClockSkew: tc.skew, MaxTTL: tc.ttl, IdempotencyRetention: tc.r})
			if (err == nil) != tc.valid {
				t.Fatalf("New: %v, valid=%v", err, tc.valid)
			}
			if b != nil && b.idemWindow() <= 0 {
				t.Fatalf("accepted nonpositive dedup window: %s", b.idemWindow())
			}
		})
	}
}

func TestRestoreUpgradesLegacyRetentionOnceAndPurgesDurably(t *testing.T) {
	w := purgePartition(t, 0)
	const skew = wire.DefaultClockSkew
	expires := time.Now().UTC()
	now := expires.Add(2*skew + time.Nanosecond)
	w.b.cfg.Now = func() time.Time { return now }
	want := expires.Add(3 * skew)
	var records []*kgo.Record
	for _, key := range []string{"r:legacy", "i:legacy", "q:legacy", "r:current"} {
		e := &entry{Phase: done, PurgeAfter: expires.Add(2 * skew)}
		if key == "r:current" {
			e.PurgeAfter, e.RetentionPolicy = want, retentionPolicy
		}
		r, err := stateRecord(w.b.stateTopic, w.id, key, e)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
	}
	commitPurgeFixture(t, w, records, len(records))
	for restore := range 3 {
		if !w.reopen(t.Context()) {
			t.Fatal("restore failed")
		}
		if len(w.st.entries) != len(records) {
			t.Fatalf("restore %d: lost entries", restore)
		}
		for key, e := range w.st.entries {
			if !e.PurgeAfter.Equal(want) || e.RetentionPolicy != retentionPolicy {
				t.Fatalf("restore %d: %s deadline=%s policy=%d", restore, key, e.PurgeAfter, e.RetentionPolicy)
			}
		}
		// One upgraded entry is rewritten between restores; the other legacy
		// entries still come from their original bytes. Neither may gain S twice.
		if restore == 0 {
			r, err := stateRecord(w.b.stateTopic, w.id, "r:legacy", w.st.entries["r:legacy"])
			if err != nil {
				t.Fatal(err)
			}
			commitPurgeFixture(t, w, []*kgo.Record{r}, len(records))
		}
	}
	for _, boundary := range []time.Time{now, want} {
		now = boundary
		w.startPurge()
		if err := w.commitBatch(nil); err != nil {
			t.Fatal(err)
		}
		if len(w.st.entries) != len(records) {
			t.Fatalf("purged while a slow owner can accept: %s", now)
		}
	}
	now = want.Add(time.Nanosecond)
	w.startPurge()
	if err := w.commitBatch(nil); err != nil {
		t.Fatal(err)
	}
	if !w.reopen(t.Context()) || len(w.st.entries) != 0 {
		t.Fatal("safe tombstones did not survive restoration")
	}
}

func TestRestoreRejectsUnknownRetentionPolicy(t *testing.T) {
	b := &Bridge{cfg: Config{ClockSkew: time.Second}}
	st := store{entries: map[string]*entry{"r:future": {RetentionPolicy: retentionPolicy + 1}}}
	if err := b.restoreRetention(&st); err == nil {
		t.Fatal("accepted an unknown retention policy")
	}
}
