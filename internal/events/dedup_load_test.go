package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestLoadDedupPreservesTopicError(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err := loadDedup(ctx, brokers, kafkatest.Service(t, "missing"), time.Now())
	if !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("missing dedup topic must preserve its broker error: %v", err)
	}
}
