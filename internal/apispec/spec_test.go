package apispec_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/sderosiaux/kafka-backbone-for-http/api"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

func mustLoad(t *testing.T, name string, data []byte) *apispec.Service {
	t.Helper()
	s, err := apispec.Load(name, data)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestDemoSpecsLoad(t *testing.T) {
	orders := mustLoad(t, "orders", api.Orders)
	mustLoad(t, "payments", api.Payments)

	op, ok := orders.Operation("createOrder")
	if !ok || op.Event == nil {
		t.Fatal("createOrder must declare an event")
	}
	ev := op.Event
	if ev.On != 201 || ev.Topic != "orders.events" || ev.Type != "OrderCreated" {
		t.Fatalf("event header: %+v", ev)
	}
	if ev.Key.Source != apispec.ResponseBody || len(ev.Key.Steps) != 1 || ev.Key.Steps[0].Field != "id" {
		t.Fatalf("key: %+v", ev.Key)
	}
	if got := ev.Value.FieldNames(); !slices.Equal(got, []string{"customerId", "id", "items"}) {
		t.Fatalf("value fields: %v", got)
	}
	if e := ev.Value.Fields["customerId"].Expr; e == nil || e.Source != apispec.RequestBody {
		t.Fatalf("customerId: %+v", e)
	}
}

func TestResolve(t *testing.T) {
	orders := mustLoad(t, "orders", api.Orders)
	cases := []struct {
		method, path string
		op           string
		params       map[string]string
		err          error
	}{
		{"POST", "/orders", "createOrder", map[string]string{}, nil},
		{"GET", "/orders", "listOrders", map[string]string{}, nil},
		{"PUT", "/orders/ord_1", "replaceOrder", map[string]string{"orderId": "ord_1"}, nil},
		{"DELETE", "/orders/ord_1", "deleteOrder", map[string]string{"orderId": "ord_1"}, nil},
		// An encoded slash stays inside one segment and is unescaped in params.
		{"GET", "/orders/a%2Fb", "getOrder", map[string]string{"orderId": "a/b"}, nil},
		{"GET", "/orders/a/b", "", nil, apispec.ErrNoOperation},
		{"POST", "/orders/", "", nil, apispec.ErrNoOperation},
		{"POST", "//orders", "", nil, apispec.ErrNoOperation},
		{"GET", "/orders/..", "", nil, apispec.ErrNoOperation},
		{"GET", "/orders/%2e%2e", "", nil, apispec.ErrNoOperation},
		{"GET", "/orders/%zz", "", nil, apispec.ErrNoOperation},
		{"GET", "/ORDERS", "", nil, apispec.ErrNoOperation},
		{"PATCH", "/orders/ord_1", "", nil, apispec.ErrMethodNotAllowed},
		{"post", "/orders", "", nil, apispec.ErrMethodNotAllowed},
	}
	for _, c := range cases {
		m, err := orders.Resolve(c.method, c.path)
		if c.err != nil {
			if !errors.Is(err, c.err) {
				t.Errorf("%s %s: want %v, got %v", c.method, c.path, c.err, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s %s: %v", c.method, c.path, err)
			continue
		}
		if m.Operation.ID != c.op || !mapsEq(m.PathParams, c.params) {
			t.Errorf("%s %s: got %s %v", c.method, c.path, m.Operation.ID, m.PathParams)
		}
	}

	_, err := orders.Resolve("PATCH", "/orders/x")
	var mna *apispec.MethodNotAllowedError
	if !errors.As(err, &mna) || !slices.Equal(mna.Allow, []string{"DELETE", "GET", "PUT"}) {
		t.Fatalf("405 must carry Allow: %v", err)
	}
}

func mapsEq(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

const header = "openapi: 3.0.3\ninfo: {title: t, version: '1'}\npaths:\n"

func TestLiteralSegmentsWin(t *testing.T) {
	s := mustLoad(t, "svc", []byte(header+`
  /a/{x}/c:
    get: {operationId: axc, parameters: [{name: x, in: path, required: true, schema: {type: string}}], responses: {'200': {description: ok}}}
  /a/b/{y}:
    get: {operationId: aby, parameters: [{name: y, in: path, required: true, schema: {type: string}}], responses: {'200': {description: ok}}}
  /a/search/c:
    get: {operationId: search, responses: {'200': {description: ok}}}
`))
	for path, want := range map[string]string{"/a/b/c": "aby", "/a/z/c": "axc", "/a/search/c": "search"} {
		m, err := s.Resolve("GET", path)
		if err != nil || m.Operation.ID != want {
			t.Errorf("%s: got %v %v, want %s", path, m.Operation, err, want)
		}
	}
}

func TestLoadRejects(t *testing.T) {
	ok := `{'201': {description: ok}}`
	cases := map[string]string{
		"ambiguous templates": `
  /o/{a}:
    post: {operationId: a, responses: ` + ok + `, parameters: [{name: a, in: path, required: true, schema: {type: string}}]}
  /o/{b}:
    put: {operationId: b, responses: ` + ok + `, parameters: [{name: b, in: path, required: true, schema: {type: string}}]}`,
		"mutation without operationId": `
  /o:
    post: {responses: ` + ok + `}`,
		"partial segment param": `
  /f/{name}.json:
    post: {operationId: f, responses: ` + ok + `, parameters: [{name: name, in: path, required: true, schema: {type: string}}]}`,
		"typo in extension": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-retrysafe: true}`,
		"retry-safe not bool": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-retry-safe: "yes"}`,
		"event on undeclared status": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 200, topic: o.events, type: OCreated, key: $.response.body.id, value: {id: $.response.body.id}}}`,
		"event on failure status": `
  /o:
    post: {operationId: o, responses: {'400': {description: bad}}, x-conduktor-event: {on: 400, topic: o.events, type: ORejected, key: $.response.body.id, value: {id: $.response.body.id}}}`,
		"event on GET": `
  /o:
    get: {operationId: o, responses: {'200': {description: ok}}, x-conduktor-event: {on: 200, topic: o.events, type: OSeen, key: $.response.body.id, value: {id: $.response.body.id}}}`,
		"event to reserved topic": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 201, topic: http.results.o, type: OCreated, key: $.response.body.id, value: {id: $.response.body.id}}}`,
		"bad expression": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 201, topic: o.events, type: OCreated, key: $.response.bdy.id, value: {id: $.response.body.id}}}`,
		"unknown path param in expr": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 201, topic: o.events, type: OCreated, key: $.request.pathParams.id, value: {id: $.response.body.id}}}`,
		"caller secret in expr": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 201, topic: o.events, type: OCreated, key: $.response.body.id, value: {t: $.request.headers.Authorization}}}`,
		"set-cookie in expr": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 201, topic: o.events, type: OCreated, key: $.response.body.id, value: {c: $.response.headers.set-cookie}}}`,
		"hop-by-hop response header in expr": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 201, topic: o.events, type: OCreated, key: $.response.body.id, value: {l: $.response.headers.content-length}}}`,
		"unknown event field": `
  /o:
    post: {operationId: o, responses: ` + ok + `, x-conduktor-event: {on: 201, topic: o.events, type: OCreated, key: $.response.body.id, value: {id: $.response.body.id}, when: always}}`,
		"extension at path level": `
  /o:
    x-conduktor-retry-safe: true
    post: {operationId: o, responses: ` + ok + `}`,
	}
	for name, paths := range cases {
		if _, err := apispec.Load("svc", []byte(header+paths)); err == nil {
			t.Errorf("%s: accepted", name)
		} else {
			t.Logf("%s: %v", name, err) // -v shows each case fails for its own reason
		}
	}
}

func TestReplayable(t *testing.T) {
	s := mustLoad(t, "svc", []byte(header+`
  /a:
    post: {operationId: plain, responses: {'201': {description: ok}}}
    put: {operationId: put, responses: {'200': {description: ok}}}
    patch: {operationId: patch, responses: {'200': {description: ok}}}
    delete: {operationId: del, responses: {'204': {description: ok}}}
  /b:
    post: {operationId: safe, x-conduktor-retry-safe: true, responses: {'201': {description: ok}}}
`))
	for id, want := range map[string]bool{"plain": false, "put": true, "patch": false, "del": true, "safe": true} {
		op, _ := s.Operation(id)
		if op.Replayable() != want {
			t.Errorf("%s: replayable=%v", id, op.Replayable())
		}
	}
}

func TestParseExpr(t *testing.T) {
	good := map[string]apispec.Expr{}
	for _, s := range []string{
		"$.request.body", "$.request.body.items[0].sku", "$.response.body.id",
		"$.request.pathParams.orderId", "$.request.headers.X-Tenant", "$.request.query.page",
	} {
		e, err := apispec.ParseExpr(s)
		if err != nil {
			t.Errorf("%s: %v", s, err)
		}
		good[s] = e
	}
	if e := good["$.request.body.items[0].sku"]; len(e.Steps) != 3 || !e.Steps[1].IsIndex || e.Steps[2].Field != "sku" {
		t.Errorf("steps: %+v", e.Steps)
	}
	if good["$.request.headers.X-Tenant"].Steps[0].Field != "x-tenant" {
		t.Error("header names are lowercased")
	}
	for _, s := range []string{
		"request.body.id", "$.request.bodyx", "$.request.body.", "$.request.body..a", "$.request.body[01]",
		"$.request.pathParams", "$.request.pathParams.a.b", "$.response.status", "$.request.body[-1]",
	} {
		if _, err := apispec.ParseExpr(s); err == nil {
			t.Errorf("%s accepted", s)
		}
	}
}

func TestCheckDetectsDrift(t *testing.T) {
	orders := mustLoad(t, "orders", api.Orders)
	cmd := wire.Command{Method: "PUT", Path: "/orders/o1", PathTemplate: "/orders/{orderId}", OperationID: "replaceOrder", PathParams: map[string]string{"orderId": "o1"}}
	if op, err := orders.Check(cmd); err != nil || op.ID != "replaceOrder" {
		t.Fatalf("%v", err)
	}
	drift := cmd
	drift.OperationID = "updateOrder"
	if _, err := orders.Check(drift); err == nil || !strings.Contains(err.Error(), "updateOrder") {
		t.Fatalf("drift not detected: %v", err)
	}
	drift = cmd
	drift.PathParams = map[string]string{"orderId": "o2"}
	if _, err := orders.Check(drift); err == nil {
		t.Fatal("path param drift not detected")
	}
}

func TestSecuritySchemesDeclareSecrets(t *testing.T) {
	s := mustLoad(t, "secure", []byte(`
openapi: 3.0.3
info: { title: S, version: 1.0.0 }
security: [ { token: [] } ]
components:
  securitySchemes:
    token:   { type: apiKey, in: header, name: X-Auth-Token }
    qtoken:  { type: apiKey, in: query,  name: access_token }
    session: { type: apiKey, in: cookie, name: sid }
    bearer:  { type: http, scheme: bearer }
paths:
  /a:
    get:  { operationId: getA, responses: { '200': { description: ok } } }
    post: { operationId: postA, security: [ { qtoken: [], session: [] }, {} ], responses: { '200': { description: ok } } }
    put:  { operationId: putA, security: [], responses: { '200': { description: ok } } }
`))
	sec := s.Secrets()
	if !slices.Equal(sec.Headers, []string{"authorization", "x-auth-token"}) || !slices.Equal(sec.Query, []string{"access_token"}) || !slices.Equal(sec.Cookies, []string{"sid"}) {
		t.Fatalf("secrets %+v", sec)
	}
	if sc, ok := s.SecurityScheme("token"); !ok || sc.In != "header" || sc.Param != "X-Auth-Token" {
		t.Fatalf("scheme %+v", sc)
	}
	get, _ := s.Operation("getA")
	post, _ := s.Operation("postA")
	put, _ := s.Operation("putA")
	if len(get.Security) != 1 || !slices.Equal(get.Security[0], []string{"token"}) {
		t.Fatalf("getA inherits document security: %v", get.Security)
	}
	if len(post.Security) != 2 || !slices.Equal(post.Security[0], []string{"qtoken", "session"}) || len(post.Security[1]) != 0 {
		t.Fatalf("postA: %v", post.Security)
	}
	if put.Security != nil {
		t.Fatalf("putA opts out: %v", put.Security)
	}
	if demo := mustLoad(t, "orders", api.Orders).Secrets(); len(demo.Headers)+len(demo.Query)+len(demo.Cookies) != 0 {
		t.Fatalf("orders declares no scheme: %+v", demo)
	}

	_, err := apispec.Load("bad", []byte(`
openapi: 3.0.3
info: { title: S, version: 1.0.0 }
paths:
  /a:
    get: { operationId: getA, security: [ { nope: [] } ], responses: { '200': { description: ok } } }
`))
	if err == nil {
		t.Fatal("requirement on an undeclared scheme must fail at load")
	}
}
