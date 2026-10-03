// Command audit prints the mutation stream of every service as JSON lines on
// stdout: who (caller, and whether its signature holds), what (operation,
// method, path) and the outcome. It needs no change to A, B, gateway or bridge.
//
//	KAFKA_BROKERS    required
//	KB_AUDIT_GROUP   consumer group (default kb-audit); a new group replays
//	                 the stream from the beginning
//	KB_TRUSTED_GATEWAY_KEYS  public keys checking commands
//	KB_TRUSTED_BRIDGE_KEYS   public keys checking results
//	                         either may be absent: that role's entries are then
//	                         marked "unverified", never "authentic"
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/audit"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/identity"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
)

func main() {
	if err := run(); err != nil {
		slog.Error("audit stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// Logs go to stderr: stdout is the audit stream.
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	brokers, err := kafkaenv.BrokersFromEnv()
	if err != nil {
		return err
	}
	var cfg audit.Config
	for role, dst := range map[identity.Role]*identity.TrustedKeys{
		identity.RoleGateway: &cfg.GatewayKeys,
		identity.RoleBridge:  &cfg.BridgeKeys,
	} {
		if os.Getenv(identity.TrustedKeysEnv(role)) == "" {
			slog.Warn("no trusted keys: entries will be marked unverified", "role", role, "env", identity.TrustedKeysEnv(role))
			continue
		}
		if *dst, err = identity.TrustedKeysFromEnv(role); err != nil {
			return err
		}
	}
	group := os.Getenv("KB_AUDIT_GROUP")
	if group == "" {
		group = "kb-audit"
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg.Brokers, cfg.Group, cfg.Out = brokers, group, os.Stdout
	return audit.Run(ctx, cfg)
}
