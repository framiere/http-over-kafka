// Command bridge is the provider side of one service: it consumes the
// service's commands from Kafka and calls the real service over plain HTTP.
// Run several for availability; they share the work through a consumer group.
//
// Environment:
//
//	KAFKA_BROKERS             broker list
//	KB_TRUSTED_GATEWAY_KEYS   gateway public keys: verify commands
//	KB_BRIDGE_SIGNING_KEY     bridge private key: signs responses, results and its dedup state (D10)
//	KB_SERVICE                service name, e.g. payments
//	KB_UPSTREAM               the service's base URL, e.g. http://payments:8082
//	KB_UPSTREAM_CREDENTIALS   service.scheme=value,...: the service's own credential when its
//	                          contract requires one for mutations (D12); checked at startup
//	KB_SPEC_DIR               optional: <dir>/<name>.openapi.yaml; else the embedded demo specs
//	KB_BRIDGE_INSTANCE        instance id for logs (default: hostname)
//	KB_UPSTREAM_TIMEOUT       one call to the service; past it the outcome is unknown (default 15s)
//	KB_IDEMPOTENCY_RETENTION  how long a keyed outcome stays replayable (default 24h)
//	KB_SESSION_TIMEOUT        how long a dead bridge's partitions stay frozen (default 10s)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/api"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/bridge"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
)

var embeddedSpecs = map[string][]byte{"orders": api.Orders, "payments": api.Payments}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg, err := config(logger)
	if err != nil {
		return err
	}
	b, err := bridge.New(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return b.Run(ctx)
}

func config(logger *slog.Logger) (bridge.Config, error) {
	var errs []error
	need := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is not set", k))
		}
		return v
	}
	dur := func(k string, def time.Duration) time.Duration {
		v := os.Getenv(k)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", k, err))
		}
		return d
	}
	brokers, err := kafkaenv.BrokersFromEnv()
	errs = append(errs, err)
	keys, err := identity.TrustedKeysFromEnv(identity.RoleGateway)
	errs = append(errs, err)
	signer, err := identity.SignerFromEnv(identity.RoleBridge)
	errs = append(errs, err)
	instance := os.Getenv("KB_BRIDGE_INSTANCE")
	if instance == "" {
		instance, _ = os.Hostname()
	}
	name := need("KB_SERVICE")
	var spec *apispec.Service
	if name != "" {
		if dir := os.Getenv("KB_SPEC_DIR"); dir != "" {
			spec, err = apispec.LoadFile(name, filepath.Join(dir, name+".openapi.yaml"))
		} else if data, ok := embeddedSpecs[name]; ok {
			spec, err = apispec.Load(name, data)
		} else {
			err = fmt.Errorf("no embedded spec for %q; set KB_SPEC_DIR", name)
		}
		errs = append(errs, err)
	}
	var creds apispec.Credentials
	if name != "" {
		creds, err = apispec.ParseCredentials(name, os.Getenv("KB_UPSTREAM_CREDENTIALS"))
		errs = append(errs, err)
	}
	return bridge.Config{
		Service:              name,
		Brokers:              brokers,
		Spec:                 spec,
		Keys:                 keys,
		Signer:               signer,
		Credentials:          creds,
		Upstream:             need("KB_UPSTREAM"),
		Instance:             instance,
		UpstreamTimeout:      dur("KB_UPSTREAM_TIMEOUT", 15*time.Second),
		IdempotencyRetention: dur("KB_IDEMPOTENCY_RETENTION", 24*time.Hour),
		SessionTimeout:       dur("KB_SESSION_TIMEOUT", 10*time.Second),
		Log:                  logger,
	}, errors.Join(errs...)
}
