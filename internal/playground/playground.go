// Package playground serves the page a visitor opens after starting the
// stack: buttons that call the real gateway as an application would, and the
// records Kafka committed, pushed live.
//
// It is a client and a reader, nothing more. It holds the dev IdP key (to
// mint caller tokens, as cmd/devtoken does) and public keys (to report
// authenticity), never a gateway or bridge signing key. Everything it shows
// comes from the gateway's responses, the service's own reads through the
// gateway, and Kafka records.
package playground

import (
	"context"
	"crypto/ed25519"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/audit"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

//go:embed web
var web embed.FS

type IdP struct {
	KeyID    string
	Key      ed25519.PrivateKey
	Issuer   string
	Audience string
}

type Config struct {
	Brokers []string
	// Gateway is where the playground sends requests; GatewayAdmin serves
	// /readyz. PublicGateway is the base URL written in curl commands, as
	// reachable from the visitor's terminal.
	Gateway       *url.URL
	GatewayAdmin  *url.URL
	PublicGateway string
	IdP           IdP
	// Application and Instance are the caller identity in minted tokens.
	Application string
	Instance    string
	// Public keys only. Zero: that role's records are shown "unverified".
	GatewayKeys identity.TrustedKeys
	BridgeKeys  identity.TrustedKeys
	// The demo services the actions call. Their OpenAPI also names the
	// event topics to read and the mappings results are explained with.
	Orders   *apispec.Service
	Payments *apispec.Service
	// Since is the first instant shown (zero: when New runs), compared with
	// record timestamps. Earlier records are read once and skipped.
	Since        time.Time
	PollInterval time.Duration // status probes, default 2s
	// Console is Conduktor Console's URL, linked from the page; "" hides it.
	Console string
	Logger  *slog.Logger
}

type Server struct {
	cfg    Config
	log    *slog.Logger
	kafka  *kgo.Client
	client *http.Client
	readyz string
	desc   describer
	hub    *hub

	mu    sync.Mutex
	calls map[string]call
	order []string
}

// BufferSize is how many records a newly opened page receives: those
// committed since the playground started, up to this many.
const BufferSize = 500

func New(cfg Config) (*Server, error) {
	if len(cfg.Brokers) == 0 || cfg.Gateway == nil || cfg.GatewayAdmin == nil {
		return nil, errors.New("playground: brokers, gateway and gateway admin URLs are required")
	}
	if cfg.Orders == nil || cfg.Payments == nil {
		return nil, errors.New("playground: the orders and payments specs are required")
	}
	if len(cfg.IdP.Key) != ed25519.PrivateKeySize || cfg.IdP.KeyID == "" {
		return nil, errors.New("playground: an IdP signing key is required to mint caller tokens")
	}
	if cfg.Application == "" {
		return nil, errors.New("playground: caller application is required")
	}
	if cfg.Instance == "" {
		cfg.Instance = cfg.Application + "-playground"
	}
	if cfg.PublicGateway == "" {
		cfg.PublicGateway = cfg.Gateway.String()
	}
	if cfg.Since.IsZero() {
		cfg.Since = time.Now()
	}
	cfg.Since = cfg.Since.UTC().Truncate(time.Millisecond)
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 2 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	specs := map[string]*apispec.Service{cfg.Orders.Name(): cfg.Orders, cfg.Payments.Name(): cfg.Payments}
	events := map[string]bool{}
	var eventTopics []string
	for _, svc := range specs {
		for _, op := range svc.Operations() {
			if op.Event != nil && !events[op.Event.Topic] {
				events[op.Event.Topic] = true
				eventTopics = append(eventTopics, op.Event.Topic)
			}
		}
	}
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(cfg.Brokers,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(topicPattern(eventTopics)),
		// From the start, filtered on Since in Run. AfterMilli(Since) would
		// avoid reading history but loses records: on a partition with no
		// record after Since yet, it resolves to the end offset, and a
		// record committed between that lookup and the first fetch is
		// skipped (reproduced in tests on the gateway's first command).
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Only what Kafka committed: records of aborted transactions are
		// never shown.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.MetadataMaxAge(5*time.Second),
	)...)
	if err != nil {
		return nil, err
	}
	readyz := cfg.GatewayAdmin.JoinPath("readyz").String()
	initial := Status{
		Gateway: Probe{State: Checking, Target: readyz},
		Kafka:   Probe{State: Checking, Target: fmt.Sprint(cfg.Brokers)},
		Since:   cfg.Since,
		Topics:  []string{},
		Caller:  cfg.Application,
		Console: cfg.Console,
		Watch:   watch(cfg.Orders),
	}
	return &Server{
		cfg:    cfg,
		log:    log,
		kafka:  cl,
		client: &http.Client{Timeout: 30 * time.Second},
		readyz: readyz,
		desc: describer{
			auditor:    audit.Auditor{GatewayKeys: cfg.GatewayKeys, BridgeKeys: cfg.BridgeKeys},
			bridgeKeys: cfg.BridgeKeys,
			specs:      specs,
			events:     events,
		},
		hub:   newHub(BufferSize, initial),
		calls: map[string]call{},
	}, nil
}

// Run reads Kafka and probes the gateway until ctx ends. Fetch errors are
// reported on the page, not returned: the page must say what is wrong.
func (s *Server) Run(ctx context.Context) error {
	defer s.kafka.Close()
	go s.watch(ctx)
	for {
		fs := s.kafka.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(t string, p int32, err error) {
			s.log.Warn("fetch", "topic", t, "partition", p, "err", err)
		})
		fs.EachRecord(func(r *kgo.Record) {
			if !r.Timestamp.Before(s.cfg.Since) {
				s.hub.publish(s.desc.item(r))
			}
		})
	}
}

func (s *Server) Handler() http.Handler {
	static, err := fs.Sub(web, "web")
	if err != nil {
		panic(err) // embedded at build time
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", noCache(http.FileServerFS(static)))
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.hub.currentStatus())
	})
	mux.HandleFunc("POST /api/actions/{name}", s.handleAction)
	// The same dev caller token the actions use, as plain text, so a
	// terminal needs neither Go nor cmd/devtoken:
	//	TOKEN=$(curl -s localhost:8090/token)
	// Not a new exposure: it is signed with the INSECURE dev IdP key
	// committed in deploy/dev-idp.env, which anyone can already use. Never
	// give the playground a real identity provider's key.
	mux.HandleFunc("GET /token", s.handleToken)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}

// handleEvents streams Server-Sent Events: "status" whenever a dependency
// changes state, "record" for every Kafka record, with its sequence number
// as id so a reconnecting EventSource resumes where it stopped.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	rc := http.NewResponseController(w)
	after, _ := strconv.ParseInt(r.Header.Get("Last-Event-ID"), 10, 64)
	status, backlog, ch, cancel := s.hub.subscribe(after)
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_ = rc.SetWriteDeadline(time.Time{})

	send := func(m message) error {
		data, err := json.Marshal(m.data)
		if err != nil {
			return err
		}
		if m.id != 0 {
			if _, err := fmt.Fprintf(w, "id: %d\n", m.id); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", m.event, data); err != nil {
			return err
		}
		return nil
	}
	if _, err := fmt.Fprint(w, "retry: 2000\n\n"); err != nil {
		return
	}
	if send(message{event: "status", data: status}) != nil {
		return
	}
	for _, it := range backlog {
		if send(message{event: "record", id: it.Seq, data: it}) != nil {
			return
		}
	}
	if rc.Flush() != nil {
		return
	}
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-ch:
			if !ok {
				return // too slow: the browser reconnects with Last-Event-ID
			}
			if send(m) != nil {
				return
			}
		case <-ping.C:
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
		}
		if rc.Flush() != nil {
			return
		}
	}
}

func (s *Server) handleToken(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	tok, err := s.token()
	if err != nil {
		http.Error(w, "cannot mint a dev token: "+err.Error()+"; check HOK_DEV_IDP_KEY, HOK_JWT_ISSUER, HOK_JWT_AUDIENCE", http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, tok)
}

// watch names the orders topics a newcomer should open in Console: the
// commands, the results and the declared business events.
func watch(orders *apispec.Service) []string {
	out := []string{wire.CommandTopic(orders.Name()), wire.ResultTopic(orders.Name())}
	for _, op := range orders.Operations() {
		if op.Event != nil && !slices.Contains(out, op.Event.Topic) {
			out = append(out, op.Event.Topic)
		}
	}
	return out
}

func noCache(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		h.ServeHTTP(w, r)
	})
}
