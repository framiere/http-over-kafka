package playground

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

// Status is what the page shows in its header, and what it says when a
// dependency is down.
type Status struct {
	Gateway Probe     `json:"gateway"`
	Kafka   Probe     `json:"kafka"`
	Since   time.Time `json:"since"`  // records are shown from this instant on
	Topics  []string  `json:"topics"` // topics currently read
	Caller  string    `json:"caller"` // application named in minted tokens
	// Console is Conduktor Console's URL ("" hides the link), Watch the
	// topics worth opening there.
	Console string   `json:"console,omitempty"`
	Watch   []string `json:"watch"`
}

// Probe states.
const (
	Checking = "checking"
	Up       = "up"
	Degraded = "degraded" // answering, not ready
	Down     = "down"
)

type Probe struct {
	State  string `json:"state"`
	Detail string `json:"detail,omitempty"`
	Target string `json:"target"`
}

func (s Status) same(o Status) bool {
	return s.Gateway == o.Gateway && s.Kafka == o.Kafka && s.Since.Equal(o.Since) && s.Caller == o.Caller &&
		s.Console == o.Console && slices.Equal(s.Watch, o.Watch) && slices.Equal(s.Topics, o.Topics)
}

// probeGateway reads the gateway's /readyz: up when it can accept mutations.
func probeGateway(ctx context.Context, client *http.Client, readyz string) Probe {
	p := Probe{Target: readyz}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, readyz, nil)
	if err != nil {
		p.State, p.Detail = Down, err.Error()
		return p
	}
	resp, err := client.Do(req)
	if err != nil {
		p.State, p.Detail = Down, err.Error()
		return p
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	p.Detail = strings.TrimSpace(string(body))
	if resp.StatusCode == http.StatusOK {
		p.State = Up
	} else {
		p.State = Degraded
	}
	return p
}

func (s *Server) probeKafka(ctx context.Context) (Probe, []string) {
	p := Probe{Target: strings.Join(s.cfg.Brokers, ",")}
	if err := s.kafka.Ping(ctx); err != nil {
		p.State, p.Detail = Down, err.Error()
		return p, nil
	}
	topics := s.kafka.GetConsumeTopics()
	slices.Sort(topics)
	p.State = Up
	if len(topics) == 0 {
		p.Detail = "connected, but none of the topics exist yet: they are created by the gateway, the bridges and the deriver at startup"
	}
	return p, topics
}

func (s *Server) watch(ctx context.Context) {
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()
	for {
		pctx, cancel := context.WithTimeout(ctx, s.cfg.PollInterval)
		gw := probeGateway(pctx, s.client, s.readyz)
		k, topics := s.probeKafka(pctx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		st := s.hub.currentStatus()
		st.Gateway, st.Kafka, st.Topics = gw, k, topics
		s.hub.setStatus(st)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
