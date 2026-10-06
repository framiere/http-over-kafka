// Command deriver turns one service's Results into its domain events.
//
//	KAFKA_BROKERS           required
//	HOK_SERVICE              required, e.g. orders
//	HOK_TRUSTED_BRIDGE_KEYS  required: only Results signed by the bridge are
//	                        derived from (D10); no keys, no start
//	HOK_DERIVER_INSTANCE     required, stable across restarts (it names the
//	                        transactional id: see events.TransactionalID)
//	HOK_SPEC_DIR             optional, reads <dir>/<service>.openapi.yaml;
//	                        defaults to the embedded spec, as the gateway
//	HOK_PARTITIONS           partitions of the topics it creates (default 6)
//	HOK_SESSION_TIMEOUT      how long a dead instance's partitions, and its
//	                        open transaction, stay frozen (default 10s)
//	HOK_DEDUP_WINDOW         Results completed longer ago yield no event, only a
//	                        beyond_dedup_window failure (default 168h)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/events"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		slog.Error("deriver stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	brokers, err := kafkaenv.BrokersFromEnv()
	if err != nil {
		return err
	}
	bridgeKeys, err := identity.TrustedKeysFromEnv(identity.RoleBridge)
	if err != nil {
		return err
	}
	svc, err := loadSpec(os.Getenv("HOK_SERVICE"), os.Getenv("HOK_SPEC_DIR"))
	if err != nil {
		return err
	}
	partitions := int32(6)
	if v := os.Getenv("HOK_PARTITIONS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 1 {
			return fmt.Errorf("HOK_PARTITIONS %q: want a positive integer", v)
		}
		partitions = int32(n)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	window := events.DefaultDedupWindow
	if v := os.Getenv("HOK_DEDUP_WINDOW"); v != "" {
		if window, err = time.ParseDuration(v); err != nil || window <= 0 {
			return fmt.Errorf("HOK_DEDUP_WINDOW %q: want a positive duration", v)
		}
	}
	if err := provision(ctx, brokers, events.Topics(svc, partitions, window)); err != nil {
		return err
	}
	session := 10 * time.Second
	if v := os.Getenv("HOK_SESSION_TIMEOUT"); v != "" {
		if session, err = time.ParseDuration(v); err != nil || session < 6*time.Second {
			return fmt.Errorf("HOK_SESSION_TIMEOUT %q: want a duration >= 6s (broker minimum)", v)
		}
	}
	d, err := events.New(events.Config{Brokers: brokers, Service: svc, Instance: os.Getenv("HOK_DERIVER_INSTANCE"),
		BridgeKeys: bridgeKeys, SessionTimeout: session, DedupWindow: window})
	if err != nil {
		return err
	}
	return d.Run(ctx)
}

func loadSpec(service, dir string) (*apispec.Service, error) {
	if dir != "" {
		return apispec.LoadFile(service, filepath.Join(dir, service+".openapi.yaml"))
	}
	embedded := map[string][]byte{"orders": api.Orders, "payments": api.Payments}
	data, ok := embedded[service]
	if !ok {
		return nil, fmt.Errorf("HOK_SERVICE %q: no embedded spec, set HOK_SPEC_DIR", service)
	}
	return apispec.Load(service, data)
}

func provision(ctx context.Context, brokers []string, topics []kafkaenv.Topic) error {
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(brokers)...)
	if err != nil {
		return err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return kafkaenv.EnsureTopics(ctx, cl, topics...)
}
