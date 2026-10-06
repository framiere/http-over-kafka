// Package gateway is the only component application A sees. It authenticates
// the caller, resolves the operation from the service's OpenAPI document,
// sends mutations through Kafka and waits for B's response on this instance's
// reply topic, and proxies GETs straight to B (D8).
//
// What A observes must match a direct call to B: same status, same body bytes,
// same significant headers. Everything the gateway answers on its own (auth,
// routing, timeout, transport) is an RFC 9457 problem carrying X-Request-Id.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Service is one upstream B: its routing table and, for GET passthrough, its
// base URL.
type Service struct {
	Spec     *apispec.Service
	Upstream *url.URL
	// Credentials are the service's own credentials, by securityScheme name,
	// injected on passthrough calls whose operation requires them (D12).
	// Never the caller's.
	Credentials apispec.Credentials
}

type Config struct {
	// Instance must be stable across restarts (D11): it names the reply topic.
	Instance string
	Services []Service
	Auth     *Authenticator
	// Signer signs commands (gateway role).
	Signer *identity.Signer
	// BridgeKeys authenticate Responses (D10): anything else on the reply
	// topic is dropped and never reaches a caller.
	BridgeKeys identity.TrustedKeys
	Brokers    []string
	// Timeout is how long the caller waits before a 504. The command keeps
	// living after it (D4).
	Timeout time.Duration
	// CommandTTL is how long after issue the bridge may still call B. It must
	// exceed Timeout and stay within wire.DefaultMaxTTL.
	CommandTTL time.Duration
	// Partitions of a service command topic when this gateway creates it.
	// Existing topics are never altered.
	Partitions int32
	// Transport is used for GET passthrough; nil means a pooled default.
	Transport http.RoundTripper
	Logger    *slog.Logger
	// OnTiming, when set, receives the gateway-side timing of every mutation
	// that got a response. For latency analysis.
	OnTiming func(Timing)
}

// Timing splits a mutation's time in the gateway.
type Timing struct {
	// Produce is from handler start to the broker's ack of the command
	// (body read, signing, produce with acks=all).
	Produce time.Duration
	// Total is from handler start to the Response being received. Total minus
	// Produce is the bridge, B, the reply produce and this instance's fetch.
	Total time.Duration
}

type Gateway struct {
	cfg      Config
	log      *slog.Logger
	services map[string]*service
	replies  *replies
	producer atomic.Pointer[kgo.Client]
	dropped  atomic.Int64
}

type service struct {
	Service
	secrets secretFilter
	proxy   http.Handler
}

func New(cfg Config) (*Gateway, error) {
	var errs []error
	if err := wire.ValidateName("gateway instance", cfg.Instance); err != nil {
		errs = append(errs, err)
	}
	if cfg.Auth == nil || cfg.Signer == nil || len(cfg.Brokers) == 0 || len(cfg.Services) == 0 {
		errs = append(errs, errors.New("auth, signer, brokers and at least one service are required"))
	}
	if cfg.Signer != nil && cfg.Signer.Role() != identity.RoleGateway {
		errs = append(errs, fmt.Errorf("signer has role %q, commands need %q", cfg.Signer.Role(), identity.RoleGateway))
	}
	if cfg.BridgeKeys.Role() != identity.RoleBridge {
		errs = append(errs, fmt.Errorf("bridge keys required to authenticate responses (have role %q)", cfg.BridgeKeys.Role()))
	}
	if cfg.Timeout <= 0 || cfg.CommandTTL <= cfg.Timeout || cfg.CommandTTL > wire.DefaultMaxTTL {
		errs = append(errs, fmt.Errorf("need 0 < timeout (%s) < command TTL (%s) <= %s", cfg.Timeout, cfg.CommandTTL, wire.DefaultMaxTTL))
	}
	if cfg.Partitions <= 0 {
		errs = append(errs, errors.New("partitions must be positive"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("gateway config: %w", err)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.MaxIdleConnsPerHost = 256
		// As the bridge: the caller's Accept-Encoding is forwarded as is; Go
		// adding its own would decompress and drop Content-Encoding.
		t.DisableCompression = true
		cfg.Transport = t
	}
	g := &Gateway{cfg: cfg, log: cfg.Logger, services: map[string]*service{}, replies: newReplies()}
	for _, s := range cfg.Services {
		name := s.Spec.Name()
		if _, dup := g.services[name]; dup {
			return nil, fmt.Errorf("gateway config: service %q configured twice", name)
		}
		if s.Upstream == nil || s.Upstream.Scheme == "" || s.Upstream.Host == "" {
			return nil, fmt.Errorf("gateway config: service %q needs an absolute upstream URL", name)
		}
		// The gateway calls B only for passthrough; the bridge checks mutations.
		passthrough := func(op *apispec.Operation) bool { return !op.Transported() }
		if err := s.Credentials.Check(s.Spec, passthrough); err != nil {
			return nil, fmt.Errorf("gateway config: %w", err)
		}
		svc := &service{Service: s, secrets: newSecretFilter(s.Spec.Secrets())}
		svc.proxy = g.newPassthrough(svc)
		g.services[name] = svc
	}
	return g, nil
}

// Ready reports whether mutations can be accepted: topics exist and the reply
// consumer is positioned.
func (g *Gateway) Ready() bool { return g.producer.Load() != nil }

// Pending is the number of requests waiting for a Response.
func (g *Gateway) Pending() int { return g.replies.len() }

// DroppedReplies counts records read from the reply topic that reached no
// caller: no waiting connection (caller timed out or left, or a previous
// process sent the command), or not an authentic, decodable Response.
func (g *Gateway) DroppedReplies() int64 { return g.dropped.Load() }

// Run connects to Kafka, retrying until it succeeds or ctx ends, then consumes
// this instance's reply topic until ctx ends. Until connected, mutations get
// 503; GETs never depend on Kafka.
func (g *Gateway) Run(ctx context.Context) error {
	clientOpts := kafkaenv.ClientOpts(g.cfg.Brokers,
		// franz-go lingers 10ms by default: pure latency for a request/reply
		// path. Batching still happens under load while requests are in flight.
		kgo.ProducerLinger(0),
		// Stated, not inherited: the bridge rejects a command whose partition
		// is not murmur2(key) % N (a signed command copied to another
		// partition would otherwise reach a second consumer and run twice).
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	producer, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return err
	}
	defer producer.Close()
	replyTopic := wire.ReplyTopic(g.cfg.Instance)

	var start int64
	for backoff := 250 * time.Millisecond; ; backoff = min(backoff*2, maxPrepareBackoff) {
		if start, err = g.prepare(ctx, kadm.NewClient(producer), replyTopic); err == nil {
			break
		}
		g.log.Warn("kafka not ready, retrying", "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	// Start at the end offset read above, not "at end" resolved lazily: any
	// Response to a command produced after Ready is at or past it. Older
	// Responses belong to connections that died with the previous process.
	consumer, err := kgo.NewClient(kafkaenv.ClientOpts(g.cfg.Brokers,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{replyTopic: {0: kgo.NewOffset().At(start)}}),
		// The bridge writes Response, Result and dedup state in one
		// transaction. An aborted one (fenced zombie) is an outcome that never
		// happened: franz-go reads uncommitted by default, which would hand it
		// to A while the system records another.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)...)
	if err != nil {
		return err
	}
	defer consumer.Close()
	g.producer.Store(producer)
	defer g.producer.Store(nil)
	g.log.Info("gateway ready", "instance", g.cfg.Instance, "replyTopic", replyTopic, "startOffset", start)

	for {
		fs := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(t string, p int32, err error) {
			g.log.Error("reply fetch", "topic", t, "partition", p, "err", err)
		})
		fs.EachRecord(func(rec *kgo.Record) {
			resp, err := wire.DecodeResponse(rec, g.cfg.BridgeKeys)
			if err != nil {
				// Forged, altered or undecodable: never delivered, and the
				// waiter keeps waiting for the genuine Response (504 if none
				// comes, which is exactly what the caller should be told).
				g.dropped.Add(1)
				msg := "reply undecodable, dropped"
				if errors.Is(err, wire.ErrNotAuthentic) {
					msg = "response not authentic, dropped"
				}
				// The record key is the requestId it claims to answer.
				g.log.Error(msg, "requestId", string(rec.Key), "offset", rec.Offset, "err", err)
				return
			}
			if !g.replies.deliver(resp) {
				g.dropped.Add(1)
				g.log.Info("reply for no waiting connection dropped (caller timed out, left, or previous process)",
					"requestId", resp.RequestID, "status", resp.Status, "fault", resp.Fault)
			}
		})
	}
}

// Startup retries. A paused broker resumes the hung attempt itself; a dead
// one fails attempts fast, so the gap between Kafka returning and readiness
// is bounded by maxPrepareBackoff (+ prepareTimeout if a connection hangs).
// Topic creation is idempotent: an attempt timing out after the broker
// created the topics leaves the next one with nothing slow to do.
const (
	prepareTimeout    = 3 * time.Second
	maxPrepareBackoff = 2 * time.Second
)

func (g *Gateway) prepare(ctx context.Context, adm *kadm.Client, replyTopic string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, prepareTimeout)
	defer cancel()
	// Each topic has one creator, its producer: the gateway creates the
	// command topics and its reply topic, never http.results.* or the
	// bridge's state topics, whose partitioning the bridge derives.
	topics := []kafkaenv.Topic{kafkaenv.ReplyTopic(g.cfg.Instance)}
	for name := range g.services {
		topics = append(topics, kafkaenv.CommandTopic(name, g.cfg.Partitions))
	}
	if err := kafkaenv.EnsureTopics(ctx, adm, topics...); err != nil {
		return 0, err
	}
	ends, err := adm.ListEndOffsets(ctx, replyTopic)
	if err != nil {
		return 0, err
	}
	o, ok := ends.Lookup(replyTopic, 0)
	if !ok {
		return 0, fmt.Errorf("no end offset for %s", replyTopic)
	}
	return o.Offset, o.Err
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Never derived from the request: a caller choosing ids could collide with
	// another caller's request and receive its response.
	requestID := wire.NewRequestID()

	svc, ok := g.services[serviceName(r.Host)]
	if !ok {
		writeProblem(w, requestID, http.StatusNotFound, ProblemUnknownService, "Unknown service",
			fmt.Sprintf("no service is routed for host %q", r.Host))
		return
	}
	caller, err := g.cfg.Auth.Authenticate(r)
	if err != nil {
		var ae *authError
		errors.As(err, &ae)
		w.Header().Set("WWW-Authenticate", ae.challenge())
		writeProblem(w, requestID, http.StatusUnauthorized, ProblemUnauthorized, "Unauthorized", ae.detail)
		return
	}
	match, err := svc.Spec.Resolve(r.Method, r.URL.EscapedPath())
	if r.Method == http.MethodHead && errors.Is(err, apispec.ErrMethodNotAllowed) {
		// An explicit HEAD has its own security requirements. Only inherit
		// GET when the contract does not declare HEAD on this path.
		match, err = svc.Spec.Resolve(http.MethodGet, r.URL.EscapedPath())
	}
	if err != nil {
		var mna *apispec.MethodNotAllowedError
		if errors.As(err, &mna) {
			allow := mna.Allow
			if slices.Contains(allow, http.MethodGet) && !slices.Contains(allow, http.MethodHead) {
				allow = append(slices.Clone(allow), http.MethodHead)
				slices.Sort(allow)
			}
			w.Header().Set("Allow", strings.Join(allow, ", "))
			writeProblem(w, requestID, http.StatusMethodNotAllowed, ProblemMethodNotAllowed, "Method not allowed",
				fmt.Sprintf("%s is not declared on this path", r.Method))
			return
		}
		writeProblem(w, requestID, http.StatusNotFound, ProblemNoOperation, "No such operation",
			fmt.Sprintf("%s declares no operation for this path", svc.Spec.Name()))
		return
	}
	if !match.Operation.Transported() {
		g.passthrough(w, r, requestID, svc, match.Operation, caller)
		return
	}
	g.mutate(w, r, requestID, svc, match, caller)
}

// serviceName maps "orders", "orders:8080" or "orders.internal" to "orders":
// A keeps calling its usual hostname, resolved to the gateway.
func serviceName(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	name, _, _ := strings.Cut(strings.ToLower(host), ".")
	return name
}

func (g *Gateway) mutate(w http.ResponseWriter, r *http.Request, requestID string, svc *service, match apispec.Match, caller wire.Caller) {
	start := time.Now()
	log := g.log.With("requestId", requestID, "operation", match.Operation.ID)
	producer := g.producer.Load()
	if producer == nil {
		w.Header().Set("Retry-After", "1")
		writeProblem(w, requestID, http.StatusServiceUnavailable, ProblemTransportUnavailable,
			"Transport unavailable: the operation was not applied", "gateway is not connected to Kafka yet")
		return
	}
	keys := r.Header.Values(wire.IdempotencyKeyHeader)
	if len(keys) > 1 {
		writeProblem(w, requestID, http.StatusBadRequest, ProblemInvalidRequest, "Invalid request", "multiple Idempotency-Key headers")
		return
	}
	var idemKey string
	if len(keys) == 1 {
		idemKey = keys[0]
		if err := wire.ValidateIdempotencyKey(idemKey); err != nil || idemKey == "" {
			writeProblem(w, requestID, http.StatusBadRequest, ProblemInvalidRequest, "Invalid request",
				"Idempotency-Key must be 1 to 255 printable ASCII characters")
			return
		}
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, wire.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeProblem(w, requestID, http.StatusRequestEntityTooLarge, ProblemPayloadTooLarge, "Payload too large",
				fmt.Sprintf("request bodies are limited to %d bytes", wire.MaxBodyBytes))
			return
		}
		log.Info("request body read failed", "err", err)
		writeProblem(w, requestID, http.StatusBadRequest, ProblemInvalidRequest, "Invalid request", "request body could not be read")
		return
	}

	now := start.UTC()
	deadline := now.Add(g.cfg.Timeout)
	cmd := wire.Command{
		V:              wire.Version,
		RequestID:      requestID,
		Service:        svc.Spec.Name(),
		OperationID:    match.Operation.ID,
		Method:         r.Method,
		PathTemplate:   match.Operation.PathTemplate,
		Path:           r.URL.EscapedPath(),
		RawQuery:       svc.secrets.rawQuery(r.URL.RawQuery),
		PathParams:     match.PathParams,
		Caller:         caller,
		Headers:        svc.secrets.requestHeaders(r.Header),
		Body:           wire.NewBody(body),
		IdempotencyKey: idemKey,
		TraceParent:    r.Header.Get("traceparent"),
		ReplyTo:        wire.ReplyTopic(g.cfg.Instance),
		Deadline:       deadline,
		IssuedAt:       now,
		ExpiresAt:      now.Add(g.cfg.CommandTTL),
	}
	rec, err := wire.EncodeCommand(cmd, g.cfg.Signer)
	if err != nil {
		// Only reachable with input the HTTP server accepted but the wire
		// contract refuses (e.g. a header name that is not a token).
		log.Info("request refused by wire contract", "err", err)
		writeProblem(w, requestID, http.StatusBadRequest, ProblemInvalidRequest, "Invalid request", err.Error())
		return
	}

	// Registered before producing: the Response cannot overtake its waiter.
	reply := g.replies.register(requestID)
	defer g.replies.forget(requestID)

	// The command outlives the connection (D4): a caller hanging up must not
	// abort a produce, only stop the wait. Bounded by the deadline: a record
	// not yet sent by then is failed, never written.
	produced := make(chan error, 1)
	go func() {
		pctx, cancel := context.WithDeadline(context.WithoutCancel(r.Context()), deadline)
		defer cancel()
		produced <- producer.ProduceSync(pctx, rec).FirstErr()
	}()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	var ackedAt time.Time
	for {
		select {
		case resp := <-reply:
			if g.cfg.OnTiming != nil && !ackedAt.IsZero() {
				g.cfg.OnTiming(Timing{Produce: ackedAt.Sub(start), Total: time.Since(start)})
			}
			writeResponse(w, requestID, resp)
			return
		case err := <-produced:
			if err == nil {
				ackedAt = time.Now()
				produced = nil
				continue
			}
			// The producer is idempotent and never cancels an in-flight
			// record: an error means the command was not written.
			log.Warn("command produce failed", "err", err)
			writeProblem(w, requestID, http.StatusServiceUnavailable, ProblemTransportUnavailable,
				"Transport unavailable: the operation was not applied", "the command could not be published; it is safe to retry")
			return
		case <-timer.C:
			detail := fmt.Sprintf("no response within %s; the operation may still be applied until %s", g.cfg.Timeout, cmd.ExpiresAt.Format(time.RFC3339))
			if idemKey != "" {
				detail += "; retry with the same Idempotency-Key to get its outcome"
			}
			writeProblem(w, requestID, http.StatusGatewayTimeout, wire.ProblemTypeTimeout,
				"Gateway timeout: the operation may still be applied", detail)
			return
		case <-r.Context().Done():
			log.Info("caller left before the response; the command keeps running")
			return
		}
	}
}

// writeResponse restitutes B's response: B's headers as transported, the
// body bytes untouched. X-Request-Id is the gateway's and replaces any B set.
func writeResponse(w http.ResponseWriter, requestID string, resp wire.Response) {
	h := w.Header()
	for name, values := range resp.Headers.HTTP() {
		h[name] = values
	}
	h.Set(wire.RequestIDHeader, requestID)
	if resp.ReplayOf != "" {
		// Stripe's convention: this is the stored outcome of an earlier
		// request with the same Idempotency-Key; B was not called again.
		h.Set("Idempotent-Replayed", "true")
	}
	body := resp.Body.Bytes()
	if bodyAllowed(resp.Status) {
		h.Set("Content-Length", strconv.Itoa(len(body)))
	}
	w.WriteHeader(resp.Status)
	if bodyAllowed(resp.Status) && len(body) > 0 {
		_, _ = w.Write(body)
	}
}

func bodyAllowed(status int) bool {
	return status != http.StatusNoContent && status != http.StatusNotModified && status >= 200
}
