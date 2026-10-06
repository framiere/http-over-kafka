// Package kafkaenv holds the Kafka settings every binary must agree on.
// Topics are provisioned explicitly: the compose broker has auto-creation
// off, so a forgotten topic fails loudly instead of appearing with defaults.
package kafkaenv

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const BrokersEnv = "KAFKA_BROKERS"

// MaxMessageBytes fits a Result (command + response, each up to
// wire.MaxBodyBytes, base64 in the worst case) with headroom. Topics and
// producers both need it; Kafka's 1 MiB default would reject large results.
const MaxMessageBytes = 8 << 20

func BrokersFromEnv() ([]string, error) {
	v := os.Getenv(BrokersEnv)
	if v == "" {
		return nil, fmt.Errorf("%s is not set (e.g. localhost:9092)", BrokersEnv)
	}
	return strings.Split(v, ","), nil
}

// ClientOpts are the base options for every client. Producers are idempotent
// with acks=all (franz-go defaults, restated so nobody weakens them by
// accident); extra options are appended and may override.
func ClientOpts(brokers []string, extra ...kgo.Opt) []kgo.Opt {
	return append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(MaxMessageBytes),
	}, extra...)
}

type Topic struct {
	Name       string
	Partitions int32
	Configs    map[string]*string
	// ReplicationFactor 0 means the broker default.
	ReplicationFactor int16
}

func ptr(s string) *string { return &s }

func baseConfigs() map[string]*string {
	return map[string]*string{"max.message.bytes": ptr(strconv.Itoa(MaxMessageBytes))}
}

// ServiceTopics are the command and result topics of a service, with the same
// partition count so a result shares its command's partition number.
func ServiceTopics(service string, partitions int32) []Topic {
	return []Topic{CommandTopic(service, partitions), ResultTopic(service, partitions)}
}

// CommandTopic is created by its producer, the gateway.
func CommandTopic(service string, partitions int32) Topic {
	return Topic{Name: wire.CommandTopic(service), Partitions: partitions, Configs: baseConfigs()}
}

// ResultTopic is created by its producer, the bridge, with the command
// topic's actual partition count.
func ResultTopic(service string, partitions int32) Topic {
	return Topic{Name: wire.ResultTopic(service), Partitions: partitions, Configs: baseConfigs()}
}

// ReplyTopic is a gateway instance's private reply topic. One partition: one
// reader. Replies are only useful while a caller waits, hence short retention.
func ReplyTopic(gatewayInstance string) Topic {
	c := baseConfigs()
	c["retention.ms"] = ptr("3600000")
	return Topic{Name: wire.ReplyTopic(gatewayInstance), Partitions: 1, Configs: c}
}

// EventTopic is a domain event topic declared by an x-conduktor-event mapping.
func EventTopic(name string, partitions int32) Topic {
	return Topic{Name: name, Partitions: partitions, Configs: baseConfigs()}
}

// EnsureTopics creates missing topics and waits until metadata describes a
// leader for every partition. The wait does not establish replica health or
// reconcile partition counts or configs of existing topics. The entire call
// is bounded by 30 seconds, or by ctx's deadline when earlier.
// Readiness polls fresh metadata rather than a cached pre-creation lookup.
func EnsureTopics(ctx context.Context, cl *kgo.Client, topics ...Topic) error {
	if len(topics) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	adm := kadm.NewClient(cl)
	var errs []error
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		names = append(names, t.Name)
		// kadm.CreateTopic returns the per-topic error as err too.
		_, err := adm.CreateTopic(ctx, t.Partitions, cmp.Or(t.ReplicationFactor, -1), t.Configs, t.Name)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			errs = append(errs, fmt.Errorf("create topic %s: %w", t.Name, err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	// CreateTopics acknowledges the controller's change before every broker
	// can describe it. Consumers must not start from missing or leaderless
	// metadata and mistake that transient state for lost durable data.
	return waitForTopics(ctx, func(ctx context.Context, names ...string) (kadm.TopicDetails, error) {
		return freshTopicMetadata(ctx, cl, names)
	}, names)
}

// A direct Metadata request refreshes the client's admin metadata cache.
// kadm.ListTopics may reuse a cached absence longer than a startup attempt
// lasts. Refreshing only metadata preserves producer and consumer state.
func freshTopicMetadata(ctx context.Context, cl *kgo.Client, names []string) (kadm.TopicDetails, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = false
	for _, name := range names {
		req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{Topic: kmsg.StringPtr(name)})
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	details := make(kadm.TopicDetails, len(resp.Topics))
	for _, topic := range resp.Topics {
		if topic.Topic == nil {
			continue
		}
		detail := kadm.TopicDetail{Topic: *topic.Topic, Err: kerr.ErrorForCode(topic.ErrorCode), Partitions: kadm.PartitionDetails{}}
		for _, p := range topic.Partitions {
			detail.Partitions[p.Partition] = kadm.PartitionDetail{Leader: p.Leader, Err: kerr.ErrorForCode(p.ErrorCode)}
		}
		details[detail.Topic] = detail
	}
	return details, nil
}

func waitForTopics(ctx context.Context, list func(context.Context, ...string) (kadm.TopicDetails, error), names []string) error {
	for backoff := 50 * time.Millisecond; ; backoff = min(2*backoff, time.Second) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("waiting for topic metadata: %w", err)
		}
		details, err := list(ctx, names...)
		if err == nil {
			err = topicMetadataReady(details, names)
		}
		if err == nil {
			return nil
		}
		if !kerr.IsRetriable(err) {
			return fmt.Errorf("waiting for topic metadata: %w", err)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("waiting for topic metadata: %w", errors.Join(ctx.Err(), err))
		case <-timer.C:
		}
	}
}

// Permanent errors take precedence over transient ones so a missing topic
// cannot hide an authorization error on another topic until the timeout.
func topicMetadataReady(details kadm.TopicDetails, names []string) error {
	var pending error
	for _, name := range names {
		topic, ok := details[name]
		if !ok {
			pending = fmt.Errorf("topic %s: %w", name, kerr.UnknownTopicOrPartition)
			continue
		}
		if topic.Err != nil {
			err := fmt.Errorf("topic %s: %w", name, topic.Err)
			if !kerr.IsRetriable(err) {
				return err
			}
			pending = err
			continue
		}
		if len(topic.Partitions) == 0 {
			pending = fmt.Errorf("topic %s has no partitions: %w", name, kerr.LeaderNotAvailable)
		}
		for id, partition := range topic.Partitions {
			err := partition.Err
			if err == nil && partition.Leader < 0 {
				err = kerr.LeaderNotAvailable
			}
			if err != nil {
				err = fmt.Errorf("topic %s partition %d: %w", name, id, err)
				if !kerr.IsRetriable(err) {
					return err
				}
				pending = err
			}
		}
	}
	return pending
}
