package apispec

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Source is the part of the exchange an expression reads from.
type Source string

const (
	RequestBody     Source = "request.body"
	RequestPath     Source = "request.pathParams"
	RequestQuery    Source = "request.query"
	RequestHeaders  Source = "request.headers"
	ResponseBody    Source = "response.body"
	ResponseHeaders Source = "response.headers"
)

var sources = []Source{RequestBody, RequestPath, RequestQuery, RequestHeaders, ResponseBody, ResponseHeaders}

// Step is one navigation step: a field name, or an array index when IsIndex.
type Step struct {
	Field   string
	Index   int
	IsIndex bool
}

// Expr is a parsed "$.<source>.<path>" expression, e.g. $.response.body.id or
// $.request.body.items[0].sku. Body expressions navigate JSON; pathParams,
// query and headers take exactly one step: the name (headers lowercase, first
// value). Evaluation semantics (missing field, non-JSON body) belong to the
// consumer and must be decided there explicitly.
type Expr struct {
	Source Source
	Steps  []Step
	raw    string
}

func (e Expr) String() string { return e.raw }

var (
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*`)
	indexRe = regexp.MustCompile(`^\[(0|[1-9][0-9]{0,5})\]`)
)

func ParseExpr(s string) (Expr, error) {
	rest, ok := strings.CutPrefix(s, "$.")
	if !ok {
		return Expr{}, fmt.Errorf("expression %q must start with $.", s)
	}
	var src Source
	for _, cand := range sources {
		if r, ok := strings.CutPrefix(rest, string(cand)); ok && (r == "" || r[0] == '.' || r[0] == '[') {
			src, rest = cand, r
			break
		}
	}
	if src == "" {
		return Expr{}, fmt.Errorf("expression %q: unknown source, want one of %v", s, sources)
	}
	var steps []Step
	for rest != "" {
		if m := indexRe.FindStringSubmatch(rest); m != nil {
			i, _ := strconv.Atoi(m[1])
			steps = append(steps, Step{Index: i, IsIndex: true})
			rest = rest[len(m[0]):]
			continue
		}
		if rest[0] != '.' {
			return Expr{}, fmt.Errorf("expression %q: unexpected %q", s, rest)
		}
		id := identRe.FindString(rest[1:])
		if id == "" {
			return Expr{}, fmt.Errorf("expression %q: empty or invalid field after '.'", s)
		}
		steps = append(steps, Step{Field: id})
		rest = rest[1+len(id):]
	}
	isBody := src == RequestBody || src == ResponseBody
	if !isBody && (len(steps) != 1 || steps[0].IsIndex) {
		return Expr{}, fmt.Errorf("expression %q: %s takes exactly one name", s, src)
	}
	if src == RequestHeaders || src == ResponseHeaders {
		steps[0].Field = strings.ToLower(steps[0].Field)
	}
	return Expr{Source: src, Steps: steps, raw: s}, nil
}

// Template is the value shape of an event: exactly one of Expr, Literal
// (a JSON scalar) or Fields (a nested object) is set.
type Template struct {
	Expr    *Expr
	Literal json.RawMessage
	Fields  map[string]Template
}

// FieldNames returns object keys in sorted order, for deterministic output.
func (t Template) FieldNames() []string {
	names := make([]string, 0, len(t.Fields))
	for k := range t.Fields {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// Strings starting with "$" are expressions and must parse; anything else
// scalar is a literal. Arrays are rejected: positional templates are ambiguous.
func parseTemplate(v any, at string) (Template, error) {
	switch x := v.(type) {
	case string:
		if strings.HasPrefix(x, "$") {
			e, err := ParseExpr(x)
			if err != nil {
				return Template{}, fmt.Errorf("%s: %w", at, err)
			}
			return Template{Expr: &e}, nil
		}
		b, _ := json.Marshal(x)
		return Template{Literal: b}, nil
	case bool, float64, int, int64, uint64, nil:
		b, err := json.Marshal(x)
		if err != nil {
			return Template{}, fmt.Errorf("%s: %w", at, err)
		}
		return Template{Literal: b}, nil
	case map[string]any:
		t := Template{Fields: make(map[string]Template, len(x))}
		for k, sub := range x {
			child, err := parseTemplate(sub, at+"."+k)
			if err != nil {
				return Template{}, err
			}
			t.Fields[k] = child
		}
		return t, nil
	default:
		return Template{}, fmt.Errorf("%s: unsupported template value %T", at, v)
	}
}

func (t Template) exprs(visit func(Expr)) {
	if t.Expr != nil {
		visit(*t.Expr)
	}
	for _, c := range t.Fields {
		c.exprs(visit)
	}
}
