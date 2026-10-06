// Command bridge is the provider side of one service: it consumes the
// service's commands from Kafka and calls the real service over plain HTTP.
// Run several for availability; they share the work through a consumer group.
//
// Environment:
//
//	KAFKA_BROKERS             broker list
//	HOK_TRUSTED_GATEWAY_KEYS   gateway public keys: verify commands
//	HOK_BRIDGE_SIGNING_KEY     bridge private key: signs responses, results and its dedup state (D10)
//	HOK_TRUSTED_BRIDGE_KEYS    optional bridge public keys: verify persisted state; defaults to
//	                          the signing key's public key. Must include the active signing key.
//	                          Deploy old + new trust everywhere before rotating signers. Retain
//	                          old public keys while state signed by them remains in Kafka,
//	                          including compacted metadata; idempotency expiry is not sufficient.
//	HOK_SERVICE                service name, e.g. payments
//	HOK_UPSTREAM               the service's base URL, e.g. http://payments:8082
//	HOK_UPSTREAM_CREDENTIALS   service.scheme=value,...: the service's own credential when its
//	                          contract requires one for mutations (D12); checked at startup
//	HOK_SPEC_DIR               optional: <dir>/<name>.openapi.yaml; else the embedded demo specs
//	HOK_BRIDGE_INSTANCE        instance id for logs (default: hostname)
//	HOK_UPSTREAM_TIMEOUT       one call to the service; past it the outcome is unknown (default 15s)
//	HOK_IDEMPOTENCY_RETENTION  how long a keyed outcome stays replayable (default 24h)
//	HOK_SESSION_TIMEOUT        how long a dead bridge's partitions stay frozen (default 10s)
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

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
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
	var stateKeys identity.TrustedKeys
	if os.Getenv(identity.TrustedKeysEnv(identity.RoleBridge)) != "" {
		stateKeys, err = identity.TrustedKeysFromEnv(identity.RoleBridge)
		errs = append(errs, err)
	}
	instance := os.Getenv("HOK_BRIDGE_INSTANCE")
	if instance == "" {
		instance, _ = os.Hostname()
	}
	name := need("HOK_SERVICE")
	var spec *apispec.Service
	if name != "" {
		if dir := os.Getenv("HOK_SPEC_DIR"); dir != "" {
			spec, err = apispec.LoadFile(name, filepath.Join(dir, name+".openapi.yaml"))
		} else if data, ok := embeddedSpecs[name]; ok {
			spec, err = apispec.Load(name, data)
		} else {
			err = fmt.Errorf("no embedded spec for %q; set HOK_SPEC_DIR", name)
		}
		errs = append(errs, err)
	}
	var creds apispec.Credentials
	if name != "" {
		creds, err = apispec.ParseCredentials(name, os.Getenv("HOK_UPSTREAM_CREDENTIALS"))
		errs = append(errs, err)
	}
	return bridge.Config{
		Service:              name,
		Brokers:              brokers,
		Spec:                 spec,
		Keys:                 keys,
		Signer:               signer,
		StateKeys:            stateKeys,
		Credentials:          creds,
		Upstream:             need("HOK_UPSTREAM"),
		Instance:             instance,
		UpstreamTimeout:      dur("HOK_UPSTREAM_TIMEOUT", 15*time.Second),
		IdempotencyRetention: dur("HOK_IDEMPOTENCY_RETENTION", 24*time.Hour),
		SessionTimeout:       dur("HOK_SESSION_TIMEOUT", 10*time.Second),
		Log:                  logger,
	}, errors.Join(errs...)
}
