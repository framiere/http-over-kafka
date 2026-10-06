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

	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
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

// EnsureTopics creates missing topics and leaves existing ones untouched. It
// does not reconcile partition counts or configs of existing topics.
func EnsureTopics(ctx context.Context, adm *kadm.Client, topics ...Topic) error {
	var errs []error
	for _, t := range topics {
		// kadm.CreateTopic returns the per-topic error as err too.
		_, err := adm.CreateTopic(ctx, t.Partitions, cmp.Or(t.ReplicationFactor, -1), t.Configs, t.Name)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			errs = append(errs, fmt.Errorf("create topic %s: %w", t.Name, err))
		}
	}
	return errors.Join(errs...)
}
