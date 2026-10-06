package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func erroredFetch(err error) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "results", Partitions: []kgo.FetchPartition{{Err: err}}}}}}
}

func TestConsumerResumesAfterTransientGroupError(t *testing.T) {
	for _, transient := range []error{
		&kgo.ErrGroupSession{Err: kerr.NotCoordinator},
		fmt.Errorf("session wrapper: %w", &kgo.ErrGroupSession{Err: kerr.CoordinatorLoadInProgress}),
		&kgo.ErrGroupSession{Err: kerr.UnknownMemberID},
		fmt.Errorf("session wrapper: %w", &kgo.ErrGroupSession{Err: fmt.Errorf("heartbeat: %w", kerr.IllegalGeneration)}),
		kerr.UnknownTopicOrPartition,
	} {
		t.Run(transient.Error(), func(t *testing.T) {
			record := &kgo.Record{Topic: "results", Partition: 1, Offset: 42, Value: []byte("available result")}
			mixed := erroredFetch(transient)
			mixed[0].Topics[0].Partitions = append(mixed[0].Topics[0].Partitions,
				kgo.FetchPartition{Partition: 1, Records: []*kgo.Record{record}})
			permanent := &kgo.ErrGroupSession{Err: kerr.GroupAuthorizationFailed}
			steps := []kgo.Fetches{erroredFetch(transient), mixed, erroredFetch(permanent)}
			polls, batches := 0, 0
			d := &Deriver{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			err := d.consume(t.Context(), func(context.Context) kgo.Fetches {
				if polls >= len(steps) {
					t.Fatal("consumer polled again after a permanent error")
				}
				fs := steps[polls]
				polls++
				return fs
			}, func(fs kgo.Fetches) error {
				batches++
				records := fs.Records()
				if len(records) != 1 || records[0] != record {
					t.Fatalf("available records lost or replaced: %v", records)
				}
				return nil
			})
			if !errors.Is(err, kerr.GroupAuthorizationFailed) || polls != 3 || batches != 1 {
				t.Fatalf("polls=%d batches=%d error=%v; want recovery then explicit permanent failure", polls, batches, err)
			}
		})
	}
}

func TestConsumerPreservesFatalFetchAndBatchFailures(t *testing.T) {
	for _, permanent := range []error{
		kerr.TopicAuthorizationFailed, kgo.ErrClientClosed, errors.New("unclassified failure"),
		kerr.UnknownMemberID, kerr.IllegalGeneration, // only group-session notifications recover
		&kgo.ErrGroupSession{Err: kerr.GroupAuthorizationFailed},
		&kgo.ErrGroupSession{Err: kerr.SaslAuthenticationFailed},
		&kgo.ErrGroupSession{Err: kerr.FencedInstanceID},
		&kgo.ErrGroupSession{Err: kerr.ProducerFenced},
		&kgo.ErrGroupSession{Err: kgo.ErrClientClosed},
		&kgo.ErrGroupSession{Err: context.Canceled}, // the Run context is still live
		&kgo.ErrGroupSession{Err: errors.New("unknown group error")},
		errors.Join(kerr.NotCoordinator, kerr.TopicAuthorizationFailed),
		&kgo.ErrGroupSession{Err: errors.Join(kerr.UnknownMemberID, kerr.GroupAuthorizationFailed)},
		errors.Join(&kgo.ErrGroupSession{Err: kerr.UnknownMemberID}, kerr.ProducerFenced),
	} {
		t.Run(permanent.Error(), func(t *testing.T) {
			d := &Deriver{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			fs := append(erroredFetch(kerr.NotCoordinator), erroredFetch(permanent)...)
			polls := 0
			err := d.consume(t.Context(), func(context.Context) kgo.Fetches {
				polls++
				if polls > 1 {
					t.Fatal("permanent error was retried")
				}
				return fs
			}, func(kgo.Fetches) error {
				t.Fatal("batch processed despite permanent fetch error")
				return nil
			})
			if !errors.Is(err, permanent) {
				t.Fatalf("permanent error lost: %v", err)
			}
		})
	}
	for _, want := range []error{kerr.NotCoordinator, kerr.UnknownMemberID, kerr.IllegalGeneration, kerr.ProducerFenced} {
		t.Run("transaction failure/"+want.Error(), func(t *testing.T) {
			d := &Deriver{}
			err := d.consume(t.Context(), func(context.Context) kgo.Fetches {
				return kgo.Fetches{{Topics: []kgo.FetchTopic{{Partitions: []kgo.FetchPartition{{Records: []*kgo.Record{{}}}}}}}}
			}, func(kgo.Fetches) error { return want })
			if !errors.Is(err, want) {
				t.Fatalf("transaction failure lost: %v", err)
			}
		})
	}
}

func TestConsumerCancellationStopsRecovery(t *testing.T) {
	d := &Deriver{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	polls := 0
	err := d.consume(ctx, func(context.Context) kgo.Fetches {
		polls++
		if polls > 1 {
			t.Fatal("continued polling after cancellation")
		}
		cancel()
		return erroredFetch(context.Canceled)
	}, func(kgo.Fetches) error {
		t.Fatal("processed a batch after cancellation")
		return nil
	})
	if err != nil || polls != 1 {
		t.Fatalf("cancellation did not stop cleanly: polls=%d error=%v", polls, err)
	}
}
