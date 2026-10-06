package playground

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/audit"
	"github.com/sderosiaux/http-over-kafka/internal/events"
	"github.com/sderosiaux/http-over-kafka/internal/identity"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Unsigned marks event records: the deriver writes CloudEvents without a
// signature of their own. It only derives from results whose bridge
// signature holds, which the trace shows next to the event.
const Unsigned = "unsigned"

// Item is one Kafka record as the page shows it. Everything in it comes from
// the record: nothing is inferred except Expect, which reads the same
// OpenAPI mapping the deriver applies.
type Item struct {
	Seq       int64     `json:"seq"`
	Kind      string    `json:"kind"` // command, response, result, event, unreadable
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Key       string    `json:"key"`
	Timestamp time.Time `json:"timestamp"`
	RequestID string    `json:"requestId,omitempty"`
	// Authenticity is audit.Authentic, audit.NotAuthentic, audit.Unverified
	// (no public keys for that role) or Unsigned (events).
	Authenticity string `json:"authenticity"`
	AuthError    string `json:"authError,omitempty"`

	Command  *CommandInfo  `json:"command,omitempty"`
	Response *ResponseInfo `json:"response,omitempty"`
	Result   *ResultInfo   `json:"result,omitempty"`
	Event    *EventInfo    `json:"event,omitempty"`
	Error    string        `json:"error,omitempty"`

	Headers []Header `json:"headers"`
	// Value is the record value as stored, cut at MaxValueBytes.
	Value     string `json:"value"`
	Truncated bool   `json:"truncated,omitempty"`
}

type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type CommandInfo struct {
	Service     string      `json:"service"`
	OperationID string      `json:"operationId"`
	Method      string      `json:"method"`
	Path        string      `json:"path"`
	Caller      wire.Caller `json:"caller"`
	Idempotent  bool        `json:"idempotent"`
	// CallerSecrets lists caller credential headers found in the record.
	// The gateway strips them (D5): anything here is a finding.
	CallerSecrets []string `json:"callerSecrets"`
}

type ResponseInfo struct {
	Status   int        `json:"status"`
	Fault    wire.Fault `json:"fault,omitempty"`
	ReplayOf string     `json:"replayOf,omitempty"`
}

type ResultInfo struct {
	Service     string       `json:"service"`
	OperationID string       `json:"operationId"`
	Method      string       `json:"method"`
	Path        string       `json:"path"`
	Type        string       `json:"type"`
	Outcome     wire.Outcome `json:"outcome"`
	Status      int          `json:"status"`
	Fault       wire.Fault   `json:"fault,omitempty"`
	Expect      *Expect      `json:"expect"`
}

// Expect is what the service's OpenAPI says this result yields.
type Expect struct {
	Declared bool   `json:"declared"`         // the operation has an x-conduktor-event mapping
	Fires    bool   `json:"fires"`            // and this result matches its condition
	Topic    string `json:"topic,omitempty"`  // mapping target
	Type     string `json:"type,omitempty"`   // e.g. OrderCreated
	On       int    `json:"on,omitempty"`     // status the mapping requires
	Known    bool   `json:"known"`            // the playground has this service's spec
	Spec     string `json:"spec,omitempty"`   // service whose spec was read
	Reason   string `json:"reason,omitempty"` // why no event, in words
}

type EventInfo struct {
	Type   string `json:"type"`
	Source string `json:"source"`
	Time   string `json:"time"`
}

// MaxValueBytes caps the record value sent to the browser.
const MaxValueBytes = 16 << 10

// describer turns records into items.
type describer struct {
	auditor    audit.Auditor
	bridgeKeys identity.TrustedKeys
	specs      map[string]*apispec.Service
	events     map[string]bool // event topics
}

func (d describer) item(r *kgo.Record) Item {
	it := Item{
		Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
		Key: printable(r.Key), Timestamp: r.Timestamp.UTC(),
	}
	for _, h := range r.Headers {
		it.Headers = append(it.Headers, Header{Key: h.Key, Value: printable(h.Value)})
	}
	it.Value, it.Truncated = printableValue(r.Value)

	switch {
	case d.events[r.Topic]:
		it.Kind, it.Authenticity = "event", Unsigned
		it.RequestID = header(r, "ce_id")
		it.Event = &EventInfo{Type: header(r, "ce_type"), Source: header(r, "ce_source"), Time: header(r, "ce_time")}
	case header(r, wire.HeaderType) == wire.TypeCommand:
		e := d.auditor.Entry(r)
		it.Authenticity, it.AuthError = e.Authenticity, e.AuthError
		cmd, err := wire.DecodeCommandUnverified(r)
		if err != nil {
			return unreadable(it, err)
		}
		it.Kind, it.RequestID = "command", cmd.RequestID
		it.Command = &CommandInfo{
			Service: cmd.Service, OperationID: cmd.OperationID, Method: cmd.Method, Path: cmd.Path,
			Caller: cmd.Caller, Idempotent: cmd.IdempotencyKey != "", CallerSecrets: callerSecrets(cmd.Headers),
		}
	case header(r, wire.HeaderType) == wire.TypeResult:
		e := d.auditor.Entry(r)
		it.Authenticity, it.AuthError = e.Authenticity, e.AuthError
		res, err := wire.DecodeResultUnverified(r)
		if err != nil {
			return unreadable(it, err)
		}
		status := res.Response.Status
		if res.Response.UpstreamStatus != 0 {
			status = res.Response.UpstreamStatus
		}
		it.Kind, it.RequestID = "result", res.Command.RequestID
		it.Result = &ResultInfo{
			Service: res.Command.Service, OperationID: res.Command.OperationID,
			Method: res.Command.Method, Path: res.Command.Path,
			Type: res.Type, Outcome: res.Outcome, Status: status, Fault: res.Response.Fault,
			Expect: d.expect(res),
		}
	case header(r, wire.HeaderType) == wire.TypeResponse:
		it.Authenticity, it.AuthError = audit.Unverified, ""
		if !d.bridgeKeys.IsZero() {
			if _, err := wire.DecodeResponse(r, d.bridgeKeys); err != nil {
				it.Authenticity, it.AuthError = audit.NotAuthentic, truncate(err.Error())
			} else {
				it.Authenticity = audit.Authentic
			}
		}
		var resp wire.Response
		if err := json.Unmarshal(r.Value, &resp); err != nil {
			return unreadable(it, err)
		}
		if err := resp.Validate(); err != nil {
			return unreadable(it, err)
		}
		it.Kind, it.RequestID = "response", resp.RequestID
		it.Response = &ResponseInfo{Status: resp.Status, Fault: resp.Fault, ReplayOf: resp.ReplayOf}
	default:
		it.Authenticity, it.AuthError = audit.NotAuthentic, "not a record of this contract"
		return unreadable(it, errors.New("record type "+strconv.Quote(header(r, wire.HeaderType))))
	}
	return it
}

// expect applies the operation's x-conduktor-event condition the way
// events.Derive does: a fault or a non-2xx never yields an event.
func (d describer) expect(res wire.Result) *Expect {
	svc, ok := d.specs[res.Command.Service]
	if !ok {
		return &Expect{Reason: "the playground does not have the OpenAPI of " + res.Command.Service}
	}
	x := &Expect{Known: true, Spec: svc.Name()}
	op, ok := svc.Operation(res.Command.OperationID)
	if !ok || op.Event == nil {
		x.Reason = res.Command.OperationID + " declares no event in the OpenAPI of " + svc.Name()
		return x
	}
	m := op.Event
	x.Declared, x.Topic, x.Type, x.On = true, m.Topic, m.Type, m.On
	resp := res.Response
	status := resp.Status
	if resp.Fault != "" {
		switch resp.Fault.Execution() {
		case wire.ExecutionNone:
			x.Reason = "the service was not called (" + string(resp.Fault) + ")"
			return x
		case wire.ExecutionUnknown:
			x.Reason = "the outcome is unknown (" + string(resp.Fault) + "): an unknown outcome never produces an event"
			return x
		}
		status = resp.UpstreamStatus
		if status == m.On {
			x.Reason = "the service answered " + strconv.Itoa(m.On) + " but its body was not relayed: the deriver reports it in " + events.FailureTopic(svc.Name())
			return x
		}
	}
	if status != m.On || res.Outcome != wire.OutcomeSucceeded {
		x.Reason = m.Type + " is published on " + strconv.Itoa(m.On) + " only; the service answered " + strconv.Itoa(status)
		return x
	}
	x.Fires = true
	return x
}

func unreadable(it Item, err error) Item {
	it.Kind, it.Error = "unreadable", truncate(err.Error())
	return it
}

// Caller secrets as wire defines them (D5); the record must carry none.
var secretHeaders = []string{"authorization", "proxy-authorization", "cookie", "x-api-key"}

func callerSecrets(h wire.Headers) []string {
	found := []string{}
	for _, n := range secretHeaders {
		if _, ok := h[n]; ok {
			found = append(found, n)
		}
	}
	return found
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func printable(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(b)
}

func printableValue(b []byte) (string, bool) {
	cut := len(b) > MaxValueBytes
	if cut {
		b = b[:MaxValueBytes]
		for len(b) > 0 && !utf8.Valid(b) {
			b = b[:len(b)-1]
		}
	}
	return printable(b), cut
}

func truncate(s string) string {
	if len(s) > 300 {
		return s[:300]
	}
	return s
}

// topicPattern matches every service's commands, replies and results, plus
// the declared event topics.
func topicPattern(eventTopics []string) string {
	alts := []string{`http\.(requests|responses|results)\..+`}
	for _, t := range eventTopics {
		alts = append(alts, regexp.QuoteMeta(t))
	}
	return "^(" + strings.Join(alts, "|") + ")$"
}

// hub keeps the items seen since start and fans them out to SSE clients.
// A client that falls behind is dropped; its EventSource reconnects with
// Last-Event-ID and catches up from the buffer.
type hub struct {
	mu     sync.Mutex
	seq    int64
	buf    []Item
	max    int
	subs   map[chan message]struct{}
	status Status
}

type message struct {
	event string // "record" or "status"
	id    int64  // records only
	data  any
}

func newHub(max int, initial Status) *hub {
	return &hub{max: max, subs: map[chan message]struct{}{}, status: initial}
}

func (h *hub) publish(it Item) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	it.Seq = h.seq
	h.buf = append(h.buf, it)
	if len(h.buf) > h.max {
		h.buf = slices.Delete(h.buf, 0, len(h.buf)-h.max)
	}
	h.fanout(message{event: "record", id: it.Seq, data: it})
}

// setStatus broadcasts s when it differs from the last one.
func (h *hub) setStatus(s Status) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.same(h.status) {
		return
	}
	h.status = s
	h.fanout(message{event: "status", data: s})
}

func (h *hub) currentStatus() Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status
}

func (h *hub) fanout(m message) {
	for ch := range h.subs {
		select {
		case ch <- m:
		default:
			delete(h.subs, ch)
			close(ch)
		}
	}
}

// subscribe returns the current status, the buffered items after seq, and a
// channel of what follows. cancel is idempotent.
func (h *hub) subscribe(after int64) (Status, []Item, <-chan message, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var backlog []Item
	for _, it := range h.buf {
		if it.Seq > after {
			backlog = append(backlog, it)
		}
	}
	ch := make(chan message, 256)
	h.subs[ch] = struct{}{}
	return h.status, backlog, ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
}
