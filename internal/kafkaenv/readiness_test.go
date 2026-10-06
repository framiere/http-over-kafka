package kafkaenv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTopicMetadataReady(t *testing.T) {
	for _, tt := range []struct {
		name    string
		details kadm.TopicDetails
		want    error
	}{
		{name: "missing topic", want: kerr.UnknownTopicOrPartition},
		{name: "topic not propagated", details: kadm.TopicDetails{"orders": {Err: kerr.UnknownTopicOrPartition}}, want: kerr.UnknownTopicOrPartition},
		{name: "empty partitions", details: kadm.TopicDetails{"orders": {}}, want: kerr.LeaderNotAvailable},
		{name: "leader pending", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: -1}}}}, want: kerr.LeaderNotAvailable},
		{name: "partition error", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 1, Err: kerr.LeaderNotAvailable}}}}, want: kerr.LeaderNotAvailable},
		{name: "authorization refused", details: kadm.TopicDetails{"orders": {Err: kerr.TopicAuthorizationFailed}}, want: kerr.TopicAuthorizationFailed},
		{name: "permanent partition error", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 1, Err: kerr.InvalidTopicException}}}}, want: kerr.InvalidTopicException},
		{name: "leader zero is valid", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 0}}}}},
		{name: "all existing partitions ready", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 0}, 1: {Leader: 1}}}}},
		{name: "one existing partition unavailable", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 0}, 1: {Leader: -1}}}}, want: kerr.LeaderNotAvailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := topicMetadataReady(tt.details, []string{"orders"}); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	// Check every requested topic before deciding to retry.
	details := kadm.TopicDetails{"private": {Err: kerr.TopicAuthorizationFailed}}
	if err := topicMetadataReady(details, []string{"missing", "private"}); !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("transient failure hid an authorization error: %v", err)
	}
}

func TestWaitForTopicsRetriesOnlyTransientMetadata(t *testing.T) {
	calls := 0
	list := func(context.Context, ...string) (kadm.TopicDetails, error) {
		calls++
		switch calls {
		case 1:
			return nil, nil // controller accepted the topic, metadata has not arrived
		case 2:
			return kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: -1}}}}, nil
		default:
			return kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 1}}}}, nil
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := waitForTopics(ctx, list, []string{"orders"}); err != nil || calls != 3 {
		t.Fatalf("calls=%d, error=%v", calls, err)
	}
	for _, permanent := range []error{kerr.TopicAuthorizationFailed, kerr.InvalidTopicException} {
		calls = 0
		list = func(context.Context, ...string) (kadm.TopicDetails, error) {
			calls++
			return nil, permanent
		}
		if err := waitForTopics(ctx, list, []string{"orders"}); !errors.Is(err, permanent) || calls != 1 {
			t.Fatalf("permanent error retried or hidden: calls=%d, error=%v", calls, err)
		}
	}
}

func TestWaitForTopicsHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	list := func(context.Context, ...string) (kadm.TopicDetails, error) {
		calls++
		cancel() // cancel during the first pending metadata response
		return nil, nil
	}
	if err := waitForTopics(ctx, list, []string{"orders"}); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancellation lost: calls=%d, error=%v", calls, err)
	}
	if err := waitForTopics(ctx, list, []string{"orders"}); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("already canceled context made a request: calls=%d, error=%v", calls, err)
	}
}

func TestEnsureTopicsHonorsCanceledContext(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := EnsureTopics(ctx, cl, Topic{Name: "orders", Partitions: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("provisioning ignored canceled context: %v", err)
	}
}
