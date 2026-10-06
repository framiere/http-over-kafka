package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// Evaluation is strict. An expression that does not resolve to a value fails
// the whole event: no field is ever dropped, defaulted or nulled, because a
// consumer cannot tell a guessed value from a real one. The only null an
// event carries is a null B (or the caller) actually sent.
//
//   - body not JSON, or empty           → error
//   - field absent, index out of range  → error (absent ≠ null)
//   - step into a scalar / wrong type   → error
//   - header, query or path param absent → error
//   - query parameter repeated          → error (which one would be the fact?)
//   - header repeated                   → first value (apispec contract)
type exchange struct {
	cmd  wire.Command
	resp wire.Response

	parsed map[apispec.Source]parsedBody
}

type parsedBody struct {
	v   any
	err error
}

func newExchange(cmd wire.Command, resp wire.Response) *exchange {
	return &exchange{cmd: cmd, resp: resp, parsed: map[apispec.Source]parsedBody{}}
}

func (x *exchange) body(src apispec.Source) (any, error) {
	if p, ok := x.parsed[src]; ok {
		return p.v, p.err
	}
	b := x.cmd.Body
	if src == apispec.ResponseBody {
		b = x.resp.Body
	}
	var p parsedBody
	raw, ok := b.JSON()
	switch {
	case b.IsEmpty():
		p.err = fmt.Errorf("%s is empty", src)
	case !ok:
		p.err = fmt.Errorf("%s is not JSON", src)
	default:
		// UseNumber: ids like 9007199254740993 must survive the round trip.
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		p.err = dec.Decode(&p.v)
	}
	x.parsed[src] = p
	return p.v, p.err
}

// eval resolves e to a JSON value.
func (x *exchange) eval(e apispec.Expr) (any, error) {
	name := ""
	if len(e.Steps) > 0 {
		name = e.Steps[0].Field
	}
	switch e.Source {
	case apispec.RequestBody, apispec.ResponseBody:
		v, err := x.body(e.Source)
		if err != nil {
			return nil, err
		}
		return walk(v, e)
	case apispec.RequestPath:
		if v, ok := x.cmd.PathParams[name]; ok {
			return v, nil
		}
		return nil, fmt.Errorf("path parameter %q absent", name)
	case apispec.RequestQuery:
		q, err := url.ParseQuery(x.cmd.RawQuery)
		if err != nil {
			return nil, fmt.Errorf("query is malformed: %v", err)
		}
		switch vs := q[name]; len(vs) {
		case 0:
			return nil, fmt.Errorf("query parameter %q absent", name)
		case 1:
			return vs[0], nil
		default:
			return nil, fmt.Errorf("query parameter %q repeated %d times", name, len(vs))
		}
	case apispec.RequestHeaders, apispec.ResponseHeaders:
		h := x.cmd.Headers
		if e.Source == apispec.ResponseHeaders {
			h = x.resp.Headers
		}
		if vs := h[name]; len(vs) > 0 {
			return vs[0], nil
		}
		return nil, fmt.Errorf("header %q absent", name)
	}
	return nil, fmt.Errorf("unsupported source %q", e.Source)
}

func walk(v any, e apispec.Expr) (any, error) {
	at := "$." + string(e.Source)
	for _, s := range e.Steps {
		if s.IsIndex {
			arr, ok := v.([]any)
			if !ok {
				return nil, fmt.Errorf("%s is %s, not an array", at, kind(v))
			}
			at += fmt.Sprintf("[%d]", s.Index)
			if s.Index >= len(arr) {
				return nil, fmt.Errorf("%s absent (array length %d)", at, len(arr))
			}
			v = arr[s.Index]
			continue
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s is %s, not an object", at, kind(v))
		}
		at += "." + s.Field
		if v, ok = obj[s.Field]; !ok {
			return nil, fmt.Errorf("%s absent", at)
		}
	}
	return v, nil
}

func kind(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "an object"
	case []any:
		return "an array"
	case string:
		return "a string"
	case json.Number:
		return "a number"
	case bool:
		return "a boolean"
	}
	return fmt.Sprintf("%T", v)
}

// renderKey turns the key expression into record key bytes. Only non-empty
// strings and numbers are keys: null, booleans and structures would put
// unrelated entities on one key or make the key an encoding accident.
func (x *exchange) renderKey(e apispec.Expr) ([]byte, error) {
	v, err := x.eval(e)
	if err != nil {
		return nil, err
	}
	switch k := v.(type) {
	case string:
		if k == "" {
			return nil, fmt.Errorf("key is an empty string")
		}
		return []byte(k), nil
	case json.Number:
		return []byte(k.String()), nil
	}
	return nil, fmt.Errorf("key is %s, want a string or a number", kind(v))
}

// renderValue builds the event value, collecting every failing expression so
// one failure record explains all of them.
func (x *exchange) renderValue(t apispec.Template, at string, errs *[]string) any {
	switch {
	case t.Expr != nil:
		v, err := x.eval(*t.Expr)
		if err != nil {
			*errs = append(*errs, at+" ("+t.Expr.String()+"): "+err.Error())
		}
		return v
	case t.Literal != nil:
		return t.Literal
	}
	obj := make(map[string]any, len(t.Fields))
	for _, name := range t.FieldNames() {
		obj[name] = x.renderValue(t.Fields[name], at+"."+name, errs)
	}
	return obj
}

func marshalCompact(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimSuffix(buf.String(), "\n")), nil
}
