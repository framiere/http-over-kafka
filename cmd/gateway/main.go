// Command gateway is the HTTP entry point application A calls instead of B.
//
// Environment:
//
//	KAFKA_BROKERS        broker list
//	HOK_GATEWAY_INSTANCE  stable instance id, names the reply topic (D11)
//	HOK_GATEWAY_SIGNING_KEY  command signing key (cmd/keygen)
//	HOK_TRUSTED_BRIDGE_KEYS  bridge public keys, to authenticate responses (D10)
//	HOK_SERVICES          name=upstreamURL,...  e.g. orders=http://orders:8081
//	HOK_UPSTREAM_CREDENTIALS  service.scheme=value,...  B's own credential for
//	                     passthrough operations whose contract requires one (D12)
//	HOK_SPEC_DIR          optional: <dir>/<name>.openapi.yaml; else the embedded demo specs
//	HOK_JWT_ISSUER, HOK_JWT_AUDIENCE, HOK_JWT_KEYS (kid:base64 Ed25519 public key,...)
//	HOK_REQUEST_TIMEOUT   caller wait before 504 (default 10s)
//	HOK_COMMAND_TTL       how long a command may still run after issue (default 5m)
//	HOK_PARTITIONS        partitions of service topics it creates (default 6)
//	ADDR                 HTTP listen address (default :8080)
//	HOK_ADMIN_ADDR        /healthz and /readyz (default :9080)
//
// A request is routed by Host: "orders", "orders:8080" and "orders.internal"
// all reach service orders.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/gateway"
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
	g, err := gateway.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	kafkaCtx, stopKafka := context.WithCancel(context.Background())
	defer stopKafka()
	runErr := make(chan error, 1)
	go func() { runErr <- g.Run(kafkaCtx) }()

	srv := &http.Server{
		Addr:              envOr("ADDR", ":8080"),
		Handler:           g,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      cfg.Timeout + 30*time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	admin := &http.Server{Addr: envOr("HOK_ADMIN_ADDR", ":9080"), Handler: adminHandler(g), ReadHeaderTimeout: 5 * time.Second}
	srvErr := make(chan error, 2)
	go func() { srvErr <- srv.ListenAndServe() }()
	go func() { srvErr <- admin.ListenAndServe() }()
	logger.Info("gateway listening", "addr", srv.Addr, "admin", admin.Addr, "instance", cfg.Instance)

	select {
	case <-ctx.Done():
	case err := <-srvErr:
		return err
	case err := <-runErr:
		return fmt.Errorf("kafka loop stopped: %w", err)
	}
	// Drain in-flight requests first (they may still be waiting for their
	// Response), then stop consuming replies.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout+5*time.Second)
	defer cancel()
	err = errors.Join(srv.Shutdown(shutdownCtx), admin.Shutdown(shutdownCtx))
	stopKafka()
	<-runErr
	return err
}

func adminHandler(g *gateway.Gateway) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !g.Ready() {
			http.Error(w, "kafka not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "ready, %d pending, %d replies dropped\n", g.Pending(), g.DroppedReplies())
	})
	return mux
}

func config(logger *slog.Logger) (gateway.Config, error) {
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
	signer, err := identity.SignerFromEnv(identity.RoleGateway)
	errs = append(errs, err)
	bridgeKeys, err := identity.TrustedKeysFromEnv(identity.RoleBridge)
	errs = append(errs, err)
	var keys identity.Keyring
	if v := need("HOK_JWT_KEYS"); v != "" {
		keys, err = identity.ParseKeyring(v)
		errs = append(errs, err)
	}
	partitions, err := strconv.ParseInt(envOr("HOK_PARTITIONS", "6"), 10, 32)
	errs = append(errs, err)
	cfg := gateway.Config{
		Instance: need("HOK_GATEWAY_INSTANCE"),
		Auth: &gateway.Authenticator{
			Keys: keys, Issuer: need("HOK_JWT_ISSUER"), Audience: need("HOK_JWT_AUDIENCE"), Leeway: 30 * time.Second,
		},
		Signer:     signer,
		BridgeKeys: bridgeKeys,
		Brokers:    brokers,
		Timeout:    dur("HOK_REQUEST_TIMEOUT", 10*time.Second),
		CommandTTL: dur("HOK_COMMAND_TTL", 5*time.Minute),
		Partitions: int32(partitions),
		Logger:     logger,
	}
	for entry := range strings.SplitSeq(need("HOK_SERVICES"), ",") {
		if entry == "" {
			continue
		}
		svc, err := service(strings.TrimSpace(entry))
		if err != nil {
			errs = append(errs, err)
			continue
		}
		svc.Credentials, err = apispec.ParseCredentials(svc.Spec.Name(), os.Getenv("HOK_UPSTREAM_CREDENTIALS"))
		errs = append(errs, err)
		cfg.Services = append(cfg.Services, svc)
	}
	return cfg, errors.Join(errs...)
}

func service(entry string) (gateway.Service, error) {
	name, upstream, ok := strings.Cut(entry, "=")
	if !ok {
		return gateway.Service{}, fmt.Errorf("HOK_SERVICES entry %q: want name=url", entry)
	}
	u, err := url.Parse(upstream)
	if err != nil {
		return gateway.Service{}, fmt.Errorf("HOK_SERVICES %s: %w", name, err)
	}
	var spec *apispec.Service
	if dir := os.Getenv("HOK_SPEC_DIR"); dir != "" {
		spec, err = apispec.LoadFile(name, filepath.Join(dir, name+".openapi.yaml"))
	} else if data, ok := embeddedSpecs[name]; ok {
		spec, err = apispec.Load(name, data)
	} else {
		err = fmt.Errorf("no embedded spec for %q; set HOK_SPEC_DIR", name)
	}
	if err != nil {
		return gateway.Service{}, err
	}
	return gateway.Service{Spec: spec, Upstream: u}, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
