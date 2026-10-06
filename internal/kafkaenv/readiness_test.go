package kafkaenv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
		{name: "missing zero", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{1: {Leader: 1}}}}, want: errInvalidTopicMetadata},
		{name: "hole", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 1}, 2: {Leader: 1}}}}, want: errInvalidTopicMetadata},
		{name: "negative partition", details: kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{-1: {Leader: 1}}}}, want: errInvalidTopicMetadata},
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

func TestDecodeTopicMetadataRejectsInconsistentResponse(t *testing.T) {
	valid := kmsg.MetadataResponseTopic{Topic: kmsg.StringPtr("orders"), Partitions: []kmsg.MetadataResponseTopicPartition{
		{Partition: 1, Leader: 1}, {Partition: 0, Leader: 1}, // order is immaterial
	}}
	if details, err := decodeTopicMetadata(&kmsg.MetadataResponse{Topics: []kmsg.MetadataResponseTopic{valid}}); err != nil {
		t.Fatal(err)
	} else if err := topicMetadataReady(details, []string{"orders"}); err != nil {
		t.Fatalf("valid unsorted metadata rejected: %v", err)
	}
	for _, tt := range []struct {
		name   string
		topics []kmsg.MetadataResponseTopic
	}{
		{"unnamed topic", []kmsg.MetadataResponseTopic{{}}},
		{"empty topic", []kmsg.MetadataResponseTopic{{Topic: kmsg.StringPtr("")}}},
		{"duplicate topic", []kmsg.MetadataResponseTopic{valid, valid}},
		{"error overwritten by duplicate topic", []kmsg.MetadataResponseTopic{{Topic: valid.Topic, ErrorCode: kerr.TopicAuthorizationFailed.Code}, valid}},
		{"duplicate partition", []kmsg.MetadataResponseTopic{{Topic: valid.Topic, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1}, {Partition: 0, Leader: 1}}}}},
		{"error overwritten by duplicate partition", []kmsg.MetadataResponseTopic{{Topic: valid.Topic, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, ErrorCode: kerr.TopicAuthorizationFailed.Code}, {Partition: 0, Leader: 1}}}}},
		{"missing zero", []kmsg.MetadataResponseTopic{{Topic: valid.Topic, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 1, Leader: 1}}}}},
		{"hole", []kmsg.MetadataResponseTopic{{Topic: valid.Topic, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1}, {Partition: 2, Leader: 1}}}}},
		{"negative partition", []kmsg.MetadataResponseTopic{{Topic: valid.Topic, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: -1, Leader: 1}}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			details, err := decodeTopicMetadata(&kmsg.MetadataResponse{Topics: tt.topics})
			if !errors.Is(err, errInvalidTopicMetadata) || details != nil || kerr.IsRetriable(err) {
				t.Fatalf("inconsistent metadata not refused permanently: details=%+v err=%v", details, err)
			}
		})
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
	for _, permanent := range []error{kerr.TopicAuthorizationFailed, kerr.InvalidTopicException, errInvalidTopicMetadata} {
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

func TestWaitForTopicsHonorsCancellationWithReadyMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := waitForTopics(ctx, func(context.Context, ...string) (kadm.TopicDetails, error) {
		cancel()
		return kadm.TopicDetails{"orders": {Partitions: kadm.PartitionDetails{0: {Leader: 1}}}}, nil
	}, []string{"orders"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ready response hid cancellation: %v", err)
	}
}

func TestWaitForTopicsHonorsDeadlineDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	calls := 0
	err := waitForTopics(ctx, func(context.Context, ...string) (kadm.TopicDetails, error) {
		calls++
		return nil, kerr.LeaderNotAvailable
	}, []string{"orders"})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 || time.Since(start) > time.Second {
		t.Fatalf("deadline ignored: calls=%d elapsed=%s error=%v", calls, time.Since(start), err)
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
