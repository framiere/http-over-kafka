// Package apispec turns a service's OpenAPI document into the routing model
// shared by gateway and bridge: given (method, escaped path), both must land on
// the same operation, or the bridge refuses the command.
//
// Matching ignores `servers`: the gateway decides which service a request is
// for, this package only maps the path inside that service.
package apispec

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

const (
	ExtEvent     = "x-conduktor-event"
	ExtRetrySafe = "x-conduktor-retry-safe"
	extPrefix    = "x-conduktor-"
)

var (
	ErrNoOperation      = errors.New("no operation matches path")
	ErrMethodNotAllowed = errors.New("path matches but method is not declared")
)

// Operation is one (method, path template) of a service.
type Operation struct {
	Service      string
	ID           string
	Method       string
	PathTemplate string
	// RetrySafe is x-conduktor-retry-safe: the owner of B declares that
	// executing this operation twice has the effect of executing it once.
	RetrySafe bool
	// Event is the x-conduktor-event mapping, nil when none is declared.
	Event *EventMapping
	// Security lists alternative sets of SecurityScheme names B requires for
	// this operation (operation-level, else document-level). Nil: none. An
	// empty set among them: anonymous calls are accepted.
	Security [][]string
}

// Transported reports whether the operation goes through Kafka.
func (o *Operation) Transported() bool { return wire.IsMutation(o.Method) }

// Replayable reports whether B may be called again for the same command when
// the first attempt's outcome is unknown (D3): PUT and DELETE are idempotent
// by HTTP contract, anything else only when declared retry-safe.
func (o *Operation) Replayable() bool {
	return o.Method == http.MethodPut || o.Method == http.MethodDelete || o.RetrySafe
}

// EventMapping is a parsed x-conduktor-event. An event is derived only from a
// Result whose response came from B (Response.Fault == "") with Status == On.
type EventMapping struct {
	On    int
	Topic string
	Type  string
	Key   Expr
	Value Template
}

// Match is a resolved request.
type Match struct {
	Operation  *Operation
	PathParams map[string]string // unescaped values
}

type segment struct {
	literal string // empty for a parameter
	param   string
}

type route struct {
	template string
	segs     []segment
	ops      map[string]*Operation
}

// Service is an immutable, validated routing table for one service.
type Service struct {
	name    string
	routes  []route
	byID    map[string]*Operation
	schemes map[string]SecurityScheme
	secrets Secrets
}

func (s *Service) Name() string { return s.name }

// Operation returns the operation with the given operationId.
func (s *Service) Operation(id string) (*Operation, bool) {
	op, ok := s.byID[id]
	return op, ok
}

// Operations returns all operations, ordered by path template then method.
func (s *Service) Operations() []*Operation {
	out := make([]*Operation, 0, len(s.byID))
	// operationId is optional on reads. Enumerate the routing table so
	// startup checks also see unnamed operations and their security needs.
	for _, r := range s.routes {
		for _, op := range r.ops {
			out = append(out, op)
		}
	}
	slices.SortFunc(out, func(a, b *Operation) int {
		return strings.Compare(a.PathTemplate+" "+a.Method, b.PathTemplate+" "+b.Method)
	})
	return out
}

func LoadFile(service, path string) (*Service, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Load(service, data)
}

// Load parses and validates an OpenAPI 3.0 document. Anything ambiguous or
// unsupported fails here, at startup, rather than per request.
func Load(service string, data []byte) (*Service, error) {
	if err := wire.ValidateName("service", service); err != nil {
		return nil, err
	}
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	doc, err := loader.LoadFromData(data)
	if err != nil {
		return nil, fmt.Errorf("%s: load: %w", service, err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		return nil, fmt.Errorf("%s: validate: %w", service, err)
	}
	s := &Service{name: service, byID: map[string]*Operation{}}
	if s.schemes, s.secrets, err = loadSecurity(doc); err != nil {
		return nil, fmt.Errorf("%s: %w", service, err)
	}
	shapes := map[string]string{}
	for _, tmpl := range doc.Paths.InMatchingOrder() {
		item := doc.Paths.Value(tmpl)
		for name := range item.Extensions {
			if strings.HasPrefix(name, extPrefix) {
				return nil, fmt.Errorf("%s: path %q: %s is only read on operations", service, tmpl, name)
			}
		}
		segs, err := parsePathTemplate(tmpl)
		if err != nil {
			return nil, fmt.Errorf("%s: path %q: %w", service, tmpl, err)
		}
		shape := shapeOf(segs)
		if other, dup := shapes[shape]; dup {
			return nil, fmt.Errorf("%s: paths %q and %q are ambiguous", service, other, tmpl)
		}
		shapes[shape] = tmpl
		r := route{template: tmpl, segs: segs, ops: map[string]*Operation{}}
		for method, o := range item.Operations() {
			op, err := buildOperation(service, method, tmpl, segs, o)
			if err != nil {
				return nil, fmt.Errorf("%s: %s %s: %w", service, method, tmpl, err)
			}
			if op.ID != "" {
				if _, dup := s.byID[op.ID]; dup {
					return nil, fmt.Errorf("%s: duplicate operationId %q", service, op.ID)
				}
				s.byID[op.ID] = op
			}
			if op.Security, err = effectiveSecurity(doc, o, s.schemes); err != nil {
				return nil, fmt.Errorf("%s: %s %s: %w", service, method, tmpl, err)
			}
			r.ops[method] = op
		}
		s.routes = append(s.routes, r)
	}
	return s, nil
}

var paramSeg = regexp.MustCompile(`^\{([A-Za-z_][A-Za-z0-9_.-]*)\}$`)

func parsePathTemplate(t string) ([]segment, error) {
	if !strings.HasPrefix(t, "/") {
		return nil, errors.New("must start with /")
	}
	if t == "/" {
		return nil, nil
	}
	seen := map[string]bool{}
	var segs []segment
	for raw := range strings.SplitSeq(t[1:], "/") {
		switch {
		case raw == "":
			return nil, errors.New("empty segment (trailing or double slash)")
		case paramSeg.MatchString(raw):
			name := paramSeg.FindStringSubmatch(raw)[1]
			if seen[name] {
				return nil, fmt.Errorf("parameter %q repeated", name)
			}
			seen[name] = true
			segs = append(segs, segment{param: name})
		case strings.ContainsAny(raw, "{}"):
			// e.g. /files/{name}.json: partial-segment params are unsupported.
			return nil, fmt.Errorf("segment %q mixes literal and parameter", raw)
		default:
			segs = append(segs, segment{literal: raw})
		}
	}
	return segs, nil
}

func shapeOf(segs []segment) string {
	var b strings.Builder
	for _, s := range segs {
		b.WriteByte('/')
		if s.param != "" {
			b.WriteString("{}")
		} else {
			b.WriteString(s.literal)
		}
	}
	return b.String()
}

func buildOperation(service, method, tmpl string, segs []segment, o *openapi3.Operation) (*Operation, error) {
	op := &Operation{Service: service, ID: o.OperationID, Method: method, PathTemplate: tmpl}
	if op.Transported() && op.ID == "" {
		return nil, errors.New("operationId is required on mutations: it names commands and results")
	}
	for name := range o.Extensions {
		if strings.HasPrefix(name, extPrefix) && name != ExtEvent && name != ExtRetrySafe {
			return nil, fmt.Errorf("unknown extension %q", name)
		}
	}
	if v, ok := o.Extensions[ExtRetrySafe]; ok {
		b, isBool := v.(bool)
		if !isBool {
			return nil, fmt.Errorf("%s must be a boolean", ExtRetrySafe)
		}
		op.RetrySafe = b
	}
	if v, ok := o.Extensions[ExtEvent]; ok {
		if !op.Transported() {
			return nil, fmt.Errorf("%s on %s: only mutations produce results to derive from", ExtEvent, method)
		}
		ev, err := parseEvent(v, segs, o)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ExtEvent, err)
		}
		op.Event = ev
	}
	return op, nil
}

var (
	topicRe     = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,249}$`)
	eventTypeRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.]*$`)
)

func parseEvent(v any, segs []segment, o *openapi3.Operation) (*EventMapping, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("must be an object")
	}
	for k := range m {
		if !slices.Contains([]string{"on", "topic", "type", "key", "value"}, k) {
			return nil, fmt.Errorf("unknown field %q", k)
		}
	}
	on, ok := m["on"].(float64)
	if !ok || on != float64(int(on)) {
		return nil, errors.New("on must be an integer status code")
	}
	ev := &EventMapping{On: int(on)}
	if o.Responses == nil || o.Responses.Value(strconv.Itoa(ev.On)) == nil {
		return nil, fmt.Errorf("on: %d is not a declared response of the operation", ev.On)
	}
	if ev.On < 200 || ev.On > 299 {
		return nil, fmt.Errorf("on: %d is not a success status; events record facts, failures are results", ev.On)
	}
	if ev.Topic, ok = m["topic"].(string); !ok || !topicRe.MatchString(ev.Topic) || ev.Topic == "." || ev.Topic == ".." {
		return nil, errors.New("topic must be a valid Kafka topic name")
	}
	if strings.HasPrefix(ev.Topic, "http.") {
		return nil, fmt.Errorf("topic %q: the http. prefix is reserved for commands, responses and results", ev.Topic)
	}
	if ev.Type, ok = m["type"].(string); !ok || !eventTypeRe.MatchString(ev.Type) {
		return nil, errors.New("type must be an identifier such as OrderCreated")
	}
	keyStr, ok := m["key"].(string)
	if !ok {
		return nil, errors.New("key must be an expression string")
	}
	key, err := ParseExpr(keyStr)
	if err != nil {
		return nil, fmt.Errorf("key: %w", err)
	}
	ev.Key = key
	rawValue, ok := m["value"].(map[string]any)
	if !ok {
		return nil, errors.New("value must be an object")
	}
	if ev.Value, err = parseTemplate(rawValue, "value"); err != nil {
		return nil, err
	}
	params := map[string]bool{}
	for _, s := range segs {
		if s.param != "" {
			params[s.param] = true
		}
	}
	var exprErr error
	check := func(e Expr) {
		if exprErr != nil {
			return
		}
		name := e.Steps[0].Field
		switch {
		case e.Source == RequestPath && !params[name]:
			exprErr = fmt.Errorf("%s: no path parameter %q", e, name)
		case e.Source == RequestHeaders && len(wire.RequestHeaders(http.Header{name: {"x"}})) == 0:
			// Caller secrets and hop-by-hop headers never reach a command:
			// the expression could only ever fail.
			exprErr = fmt.Errorf("%s: header %q is never transported", e, name)
		case e.Source == ResponseHeaders && name == "set-cookie":
			// B hands the cookie to one caller; an event topic has many readers.
			exprErr = fmt.Errorf("%s: set-cookie is a session secret, not event data", e)
		case e.Source == ResponseHeaders && len(wire.ResponseHeaders(http.Header{name: {"x"}})) == 0:
			exprErr = fmt.Errorf("%s: header %q is never transported", e, name)
		}
	}
	check(ev.Key)
	ev.Value.exprs(check)
	return ev, exprErr
}

// Resolve maps a method and an escaped path (URL.EscapedPath) to an
// operation. Literal segments win over parameters, left to right, so
// /orders/search beats /orders/{id}. Paths with empty, "." or ".." segments
// never match: they are normalization traps, not resources.
func (s *Service) Resolve(method, escapedPath string) (Match, error) {
	segs, err := splitPath(escapedPath)
	if err != nil {
		return Match{}, fmt.Errorf("%w: %v", ErrNoOperation, err)
	}
	var best *route
	for i := range s.routes {
		r := &s.routes[i]
		if matches(r.segs, segs) && (best == nil || moreSpecific(r.segs, best.segs)) {
			best = r
		}
	}
	if best == nil {
		return Match{}, ErrNoOperation
	}
	op, ok := best.ops[method]
	if !ok {
		return Match{}, &MethodNotAllowedError{Allow: allowed(best)}
	}
	params := map[string]string{}
	for i, sg := range best.segs {
		if sg.param != "" {
			params[sg.param] = segs[i]
		}
	}
	return Match{Operation: op, PathParams: params}, nil
}

// MethodNotAllowedError carries the Allow header value for a 405.
type MethodNotAllowedError struct{ Allow []string }

func (e *MethodNotAllowedError) Error() string {
	return fmt.Sprintf("%v (allow: %s)", ErrMethodNotAllowed, strings.Join(e.Allow, ", "))
}
func (e *MethodNotAllowedError) Unwrap() error { return ErrMethodNotAllowed }

func allowed(r *route) []string {
	out := make([]string, 0, len(r.ops))
	for m := range r.ops {
		out = append(out, m)
	}
	slices.Sort(out)
	return out
}

func splitPath(p string) ([]string, error) {
	if !strings.HasPrefix(p, "/") {
		return nil, errors.New("path must start with /")
	}
	if p == "/" {
		return nil, nil
	}
	var out []string
	for raw := range strings.SplitSeq(p[1:], "/") {
		seg, err := url.PathUnescape(raw)
		if err != nil {
			return nil, err
		}
		if seg == "" || seg == "." || seg == ".." {
			return nil, fmt.Errorf("segment %q not allowed", raw)
		}
		out = append(out, seg)
	}
	return out, nil
}

func matches(tmpl []segment, segs []string) bool {
	if len(tmpl) != len(segs) {
		return false
	}
	for i, t := range tmpl {
		if t.param == "" && t.literal != segs[i] {
			return false
		}
	}
	return true
}

func moreSpecific(a, b []segment) bool {
	for i := range a {
		if (a[i].param == "") != (b[i].param == "") {
			return a[i].param == ""
		}
	}
	return false
}

// Check re-resolves a command the way the bridge must before calling B, and
// fails if the gateway resolved it differently (spec drift between the two).
func (s *Service) Check(cmd wire.Command) (*Operation, error) {
	m, err := s.Resolve(cmd.Method, cmd.Path)
	if err != nil {
		return nil, err
	}
	if m.Operation.ID != cmd.OperationID || m.Operation.PathTemplate != cmd.PathTemplate || !mapsEqual(m.PathParams, cmd.PathParams) {
		return nil, fmt.Errorf("gateway resolved %s %s to %q %s, bridge resolves %q %s",
			cmd.Method, cmd.Path, cmd.OperationID, cmd.PathTemplate, m.Operation.ID, m.Operation.PathTemplate)
	}
	return m.Operation, nil
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
