package bridge_test

import (
	"bytes"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/api"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
)

func TestBridgeStartupOmitsUpstreamUserinfo(t *testing.T) {
	_, srv := paymentsB(t)
	e := newEnv(t, "startup", api.Payments, srv.URL)
	cfg := e.config()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("startup-user", "startup-secret")
	cfg.Upstream = u.String()
	var logs bytes.Buffer
	cfg.Log = slog.New(slog.NewJSONHandler(&logs, nil))
	stop := e.start(cfg)
	cmd := e.charge("account", "startup-key")
	e.send(cmd)
	if got := e.await(cmd.RequestID, 30*time.Second); got.Status != 201 {
		t.Fatalf("valid userinfo configuration failed: %+v", got)
	}
	stop() // all bridge log writes have completed before reading the buffer
	text := logs.String()
	for _, secret := range []string{"startup-user", "startup-secret"} {
		if strings.Contains(text, secret) {
			t.Fatalf("successful startup exposes userinfo: %s", text)
		}
	}
	if !strings.Contains(text, `"service":"`+e.service+`"`) || !strings.Contains(text, `"upstream":"`+srv.URL+`"`) {
		t.Fatalf("startup lost service or safe endpoint context: %s", text)
	}
	for _, suffix := range []string{"?access_token=query-secret", "/\n"} {
		cfg.Upstream = u.String() + suffix
		_, err := bridge.New(cfg)
		if err == nil || !strings.Contains(err.Error(), e.service) {
			t.Fatalf("configuration error lost service context: %v", err)
		}
		for _, secret := range []string{"startup-user", "startup-secret", "query-secret"} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("constructor exposes userinfo: %v", err)
			}
		}
	}
}
