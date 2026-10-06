// Command playground serves a page that calls the real gateway as an
// application would and shows, live, what Kafka committed: commands,
// replies, results and business events. Open http://localhost:8090.
//
// Environment:
//
//	KAFKA_BROKERS                 broker list
//	HOK_PLAYGROUND_GATEWAY        where requests are sent (default http://localhost:8080)
//	HOK_PLAYGROUND_GATEWAY_ADMIN  gateway /readyz (default http://localhost:9080)
//	HOK_PLAYGROUND_PUBLIC_GATEWAY gateway URL written in curl commands, as seen
//	                              from the visitor's terminal (default HOK_PLAYGROUND_GATEWAY)
//	HOK_PLAYGROUND_APP            caller application in minted tokens (default checkout)
//	HOK_PLAYGROUND_CONSOLE_URL    Conduktor Console, linked from the page
//	                              (default http://localhost:8088; "none" hides the link)
//	HOK_DEV_IDP_KEY, HOK_JWT_ISSUER, HOK_JWT_AUDIENCE
//	                              the INSECURE dev IdP (deploy/dev-idp.env), as cmd/devtoken
//	HOK_TRUSTED_GATEWAY_KEYS, HOK_TRUSTED_BRIDGE_KEYS
//	                              optional public keys; without them records are
//	                              shown "unverified", never "authentic"
//	HOK_SPEC_DIR                  optional: <dir>/orders.openapi.yaml and
//	                              payments.openapi.yaml; else the embedded specs
//	ADDR                          listen address (default :8090)
//
// GET /token returns a caller token minted with the dev IdP key, for
// terminals: TOKEN=$(curl -s localhost:8090/token).
//
// It holds no gateway or bridge signing key: it can call the gateway and
// read Kafka, nothing else.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/playground"
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
	pg, err := playground.New(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runErr := make(chan error, 1)
	go func() { runErr <- pg.Run(ctx) }()

	// No WriteTimeout: /api/events is a long-lived stream. BaseContext ends
	// those streams on SIGTERM, so Shutdown does not wait for them.
	srv := &http.Server{
		Addr: envOr("ADDR", ":8090"), Handler: pg.Handler(), ReadHeaderTimeout: 5 * time.Second,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.ListenAndServe() }()
	logger.Info("playground listening", "addr", srv.Addr, "gateway", cfg.Gateway.String(), "since", cfg.Since)

	select {
	case <-ctx.Done():
	case err := <-srvErr:
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	return <-runErr
}

func config(logger *slog.Logger) (playground.Config, error) {
	var errs []error
	need := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is not set", k))
		}
		return v
	}
	parseURL := func(k, def string) *url.URL {
		u, err := url.Parse(envOr(k, def))
		if err != nil || u.Scheme == "" || u.Host == "" {
			errs = append(errs, fmt.Errorf("%s: want an absolute URL, got %q", k, envOr(k, def)))
			return nil
		}
		return u
	}
	brokers, err := kafkaenv.BrokersFromEnv()
	errs = append(errs, err)
	cfg := playground.Config{
		Brokers:      brokers,
		Gateway:      parseURL("HOK_PLAYGROUND_GATEWAY", "http://localhost:8080"),
		GatewayAdmin: parseURL("HOK_PLAYGROUND_GATEWAY_ADMIN", "http://localhost:9080"),
		Application:  envOr("HOK_PLAYGROUND_APP", "checkout"),
		Logger:       logger,
		IdP:          playground.IdP{Issuer: need("HOK_JWT_ISSUER"), Audience: need("HOK_JWT_AUDIENCE")},
	}
	cfg.PublicGateway = os.Getenv("HOK_PLAYGROUND_PUBLIC_GATEWAY")
	cfg.Since = time.Now() // records committed from now on are shown
	if cfg.Console = envOr("HOK_PLAYGROUND_CONSOLE_URL", "http://localhost:8088"); cfg.Console == "none" {
		cfg.Console = ""
	}

	kid, b64, ok := strings.Cut(need("HOK_DEV_IDP_KEY"), ":")
	seed, err := base64.StdEncoding.DecodeString(b64)
	if !ok || err != nil || len(seed) != ed25519.SeedSize {
		errs = append(errs, errors.New("HOK_DEV_IDP_KEY must be <kid>:<base64 32-byte seed>"))
	} else {
		cfg.IdP.KeyID, cfg.IdP.Key = kid, ed25519.NewKeyFromSeed(seed)
	}

	for role, dst := range map[identity.Role]*identity.TrustedKeys{
		identity.RoleGateway: &cfg.GatewayKeys,
		identity.RoleBridge:  &cfg.BridgeKeys,
	} {
		if os.Getenv(identity.TrustedKeysEnv(role)) == "" {
			logger.Warn("no trusted keys: records will be shown unverified", "role", role, "env", identity.TrustedKeysEnv(role))
			continue
		}
		keys, err := identity.TrustedKeysFromEnv(role)
		errs = append(errs, err)
		*dst = keys
	}

	for name, dst := range map[string]**apispec.Service{"orders": &cfg.Orders, "payments": &cfg.Payments} {
		var svc *apispec.Service
		var err error
		if dir := os.Getenv("HOK_SPEC_DIR"); dir != "" {
			svc, err = apispec.LoadFile(name, filepath.Join(dir, name+".openapi.yaml"))
		} else {
			svc, err = apispec.Load(name, embeddedSpecs[name])
		}
		errs = append(errs, err)
		*dst = svc
	}
	return cfg, errors.Join(errs...)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
