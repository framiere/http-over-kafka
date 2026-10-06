package bridge_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/bridge"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProviderQuerySecretNeverPersistsAfterTransportFailure(t *testing.T) {
	const secret = "provider-secret+/=&value"
	const spec = `openapi: 3.0.3
info: {title: secured, version: '1'}
components:
  securitySchemes:
    key: {type: apiKey, in: query, name: access_token}
security: [{key: []}]
paths:
  /charges:
    post: {operationId: createCharge, responses: {'201': {description: ok}}}
`
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		calls.Add(1)
		if r.URL.Query().Get("access_token") != secret {
			t.Error("provider credential was not injected")
		}
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	}))
	defer srv.Close()
	e := newEnv(t, "secret", []byte(spec), srv.URL)
	cfg := e.config()
	cfg.Credentials = apispec.Credentials{"key": secret}
	e.start(cfg)
	cmd := e.charge("account", "retry-key")
	e.send(cmd)
	if resp := e.await(cmd.RequestID, 30*time.Second); resp.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("fault %s, want unknown", resp.Fault)
	}
	retry := e.charge("account", "retry-key")
	e.send(retry)
	if resp := e.await(retry.RequestID, 30*time.Second); resp.ReplayOf != cmd.RequestID || resp.Fault != wire.FaultOutcomeUnknown {
		t.Fatalf("retry did not preserve original uncertainty: %+v", resp)
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream was called %d times", calls.Load())
	}
	// Check the actual Kafka bytes, including the cached response used for retries.
	for _, topic := range []string{wire.ResultTopic(e.service), wire.ReplyTopic(e.gw), bridge.StateTopic(e.service)} {
		records := kafkatest.Consume(t, topic, 30*time.Second, func(rs []*kgo.Record) bool {
			for _, r := range rs {
				if bytes.Contains(r.Value, []byte(cmd.RequestID)) && (topic != bridge.StateTopic(e.service) || bytes.Contains(r.Value, []byte(`"phase":"done"`))) {
					return true
				}
			}
			return false
		})
		for _, r := range records {
			raw := append(bytes.Clone(r.Key), r.Value...)
			for _, h := range r.Headers {
				raw = append(raw, h.Value...)
			}
			if bytes.Contains(raw, []byte("provider-secret")) || bytes.Contains(raw, []byte("access_token")) {
				t.Fatalf("credential leaked into %s[%d]@%d", topic, r.Partition, r.Offset)
			}
		}
	}
}
