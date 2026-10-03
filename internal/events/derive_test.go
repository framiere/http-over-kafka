package events_test

import (
	"strings"
	"testing"

	"github.com/sderosiaux/kafka-backbone-for-http/api"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/events"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

func TestDeriveOrderCreated(t *testing.T) {
	svc := load(t, "orders", api.Orders)
	cmd := createOrder(t, "orders", orderReq)
	cmd.TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	res := result(t, cmd, answered(cmd, 201, orderResp("ord_1")))

	ev, err := events.Derive(svc, res)
	if err != nil || ev == nil {
		t.Fatalf("want an event, got %v %v", ev, err)
	}
	want := `{"customerId":"c-42","id":"ord_1","items":[{"quantity":2,"sku":"A123"}]}`
	if string(ev.Value) != want || string(ev.Key) != "ord_1" {
		t.Fatalf("key %q value %s, want ord_1 %s", ev.Key, ev.Value, want)
	}
	if ev.Topic != "orders.events" || ev.Type != "OrderCreated" || ev.ID != cmd.RequestID ||
		ev.Source != "/services/orders/operations/createOrder" || !ev.Time.Equal(res.CompletedAt) || ev.TraceParent != cmd.TraceParent {
		t.Fatalf("metadata: %+v", ev)
	}
}

// Only a real 201 from B is a fact. Everything else is a Result and nothing more.
func TestDeriveNoEventWithoutFact(t *testing.T) {
	svc := load(t, "orders", api.Orders)
	cmd := createOrder(t, "orders", orderReq)
	tooLarge := wire.FaultResponse(cmd.RequestID, wire.FaultResponseTooLarge, "")
	tooLarge.UpstreamStatus = 400
	del := command(t, "orders", "deleteOrder", "DELETE", "/orders/ord_1", "/orders/{orderId}", "")
	del.PathParams = map[string]string{"orderId": "ord_1"}

	cases := map[string]wire.Result{
		"400 from B":            result(t, cmd, answered(cmd, 400, `{"title":"bad"}`)),
		"200 instead of 201":    result(t, cmd, answered(cmd, 200, orderResp("ord_1"))),
		"outcome unknown":       result(t, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown, "")),
		"B unavailable":         result(t, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultUpstreamUnavailable, "")),
		"stale command":         result(t, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultCommandStale, "")),
		"key reused":            result(t, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultIdempotencyKeyReused, "")),
		"in flight":             result(t, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultIdempotencyInFlight, "")),
		"secret in command":     result(t, cmd, wire.FaultResponse(cmd.RequestID, wire.FaultSecretInCommand, "")),
		"too large, B said 4xx": result(t, cmd, tooLarge),
		"operation unmapped":    result(t, del, answered(del, 204, "")),
	}
	for name, res := range cases {
		if ev, err := events.Derive(svc, res); ev != nil || err != nil {
			t.Errorf("%s: want nothing, got %+v %v", name, ev, err)
		}
	}
}

func TestDeriveFailures(t *testing.T) {
	svc := load(t, "orders", api.Orders)
	cmd := createOrder(t, "orders", orderReq)
	tooLarge := wire.FaultResponse(cmd.RequestID, wire.FaultResponseTooLarge, "")
	tooLarge.UpstreamStatus = 201
	incomplete := wire.FaultResponse(cmd.RequestID, wire.FaultResponseIncomplete, "")
	incomplete.UpstreamStatus = 201
	unknown := cmd
	unknown.OperationID = "createOrderV2"
	drift := cmd
	drift.PathTemplate = "/v2/orders"
	other := createOrder(t, "payments", orderReq)

	cases := []struct {
		name   string
		res    wire.Result
		reason string
		detail []string
	}{
		{"B ran, body not relayed", result(t, cmd, tooLarge), events.ReasonBodyNotRelayed, []string{"answered 201"}},
		{"B ran, response cut off", result(t, cmd, incomplete), events.ReasonBodyNotRelayed, []string{"answered 201", "response_incomplete"}},
		{"unknown operation", result(t, unknown, answered(unknown, 201, orderResp("x"))), events.ReasonUnknownOperation, []string{"createOrderV2"}},
		{"spec drift", result(t, drift, answered(drift, 201, orderResp("x"))), events.ReasonOperationMismatch, []string{"/v2/orders"}},
		{"other service", result(t, other, answered(other, 201, orderResp("x"))), events.ReasonOperationMismatch, []string{"payments"}},
		{"response not JSON", result(t, cmd, answered(cmd, 201, "created")), events.ReasonEvaluationFailed,
			[]string{"key ($.response.body.id): response.body is not JSON", "value.id ($.response.body.id): response.body is not JSON"}},
		{"response empty", result(t, cmd, answered(cmd, 201, "")), events.ReasonEvaluationFailed,
			[]string{"response.body is empty"}},
		{"id absent", result(t, cmd, answered(cmd, 201, `{"customerId":"c-42"}`)), events.ReasonEvaluationFailed,
			[]string{"$.response.body.id absent"}},
		{"key null", result(t, cmd, answered(cmd, 201, `{"id":null}`)), events.ReasonEvaluationFailed,
			[]string{"key is null"}},
		{"key object", result(t, cmd, answered(cmd, 201, `{"id":{"v":1}}`)), events.ReasonEvaluationFailed,
			[]string{"key is an object"}},
		{"key empty", result(t, cmd, answered(cmd, 201, `{"id":""}`)), events.ReasonEvaluationFailed,
			[]string{"key is an empty string"}},
		{"response is an array", result(t, cmd, answered(cmd, 201, `[1]`)), events.ReasonEvaluationFailed,
			[]string{"$.response.body is an array, not an object"}},
	}
	for _, c := range cases {
		ev, err := events.Derive(svc, c.res)
		de, ok := events.AsError(err)
		if ev != nil || !ok || de.Reason != c.reason {
			t.Errorf("%s: want %s, got %+v %v", c.name, c.reason, ev, err)
			continue
		}
		for _, d := range c.detail {
			if !strings.Contains(err.Error(), d) {
				t.Errorf("%s: %q does not mention %q", c.name, err, d)
			}
		}
	}

	// Every failing expression is reported, not just the first.
	bad := createOrder(t, "orders", `{}`)
	_, err := events.Derive(svc, result(t, bad, answered(bad, 201, `{"id":"ord_9"}`)))
	for _, d := range []string{"$.request.body.customerId absent", "$.request.body.items absent"} {
		if err == nil || !strings.Contains(err.Error(), d) {
			t.Errorf("multi-failure: %v does not mention %q", err, d)
		}
	}
}

const cartSpec = `
openapi: 3.0.3
info: {title: carts, version: '1'}
paths:
  /carts/{cartId}/items:
    parameters: [{name: cartId, in: path, required: true, schema: {type: string}}]
    post:
      operationId: addItem
      responses: {'201': {description: ok}}
      x-conduktor-event:
        on: 201
        topic: carts.events
        type: ItemAdded
        key: $.response.body.seq
        value:
          cart: $.request.pathParams.cartId
          source: $.request.query.source
          tenant: $.request.headers.X-Tenant
          location: $.response.headers.Location
          firstSku: $.request.body.items[0].sku
          schema: 2
          kind: item
          nested: {note: $.request.body.note, total: $.response.body.total}
`

func TestEvaluationSources(t *testing.T) {
	svc := load(t, "carts", []byte(cartSpec))
	base := func(mut func(*wire.Command, *wire.Response)) wire.Result {
		cmd := command(t, "carts", "addItem", "POST", "/carts/c%2F1/items", "/carts/{cartId}/items",
			`{"items":[{"sku":"<A&B>"}],"note":null}`)
		cmd.PathParams = map[string]string{"cartId": "c/1"}
		cmd.RawQuery = "source=web&x=1"
		cmd.Headers["x-tenant"] = []string{"acme", "ignored"}
		resp := answered(cmd, 201, `{"seq":9007199254740993,"total":12.50}`)
		if mut != nil {
			mut(&cmd, &resp)
		}
		return result(t, cmd, resp)
	}

	ev, err := events.Derive(svc, base(nil))
	if err != nil {
		t.Fatal(err)
	}
	// Numbers keep their exact text, null stays null, HTML is not escaped,
	// repeated headers yield their first value.
	want := `{"cart":"c/1","firstSku":"<A&B>","kind":"item","location":"/orders/x","nested":{"note":null,"total":12.50},"schema":2,"source":"web","tenant":"acme"}`
	if string(ev.Value) != want || string(ev.Key) != "9007199254740993" {
		t.Fatalf("got key %s value %s\nwant value %s", ev.Key, ev.Value, want)
	}

	fails := map[string]func(*wire.Command, *wire.Response){
		"query parameter \"source\" absent":     func(c *wire.Command, _ *wire.Response) { c.RawQuery = "" },
		"query parameter \"source\" repeated 2": func(c *wire.Command, _ *wire.Response) { c.RawQuery = "source=a&source=b" },
		"header \"x-tenant\" absent":            func(c *wire.Command, _ *wire.Response) { delete(c.Headers, "x-tenant") },
		"header \"location\" absent":            func(_ *wire.Command, r *wire.Response) { delete(r.Headers, "location") },
		"path parameter \"cartId\" absent":      func(c *wire.Command, _ *wire.Response) { c.PathParams = nil },
		"$.request.body.items[0] absent (array length 0)": func(c *wire.Command, _ *wire.Response) {
			c.Body = wire.NewBody([]byte(`{"items":[],"note":1}`))
		},
		"$.request.body.items[0] is a string, not an object": func(c *wire.Command, _ *wire.Response) {
			c.Body = wire.NewBody([]byte(`{"items":["A"],"note":1}`))
		},
		"$.request.body.items is a string, not an array": func(c *wire.Command, _ *wire.Response) {
			c.Body = wire.NewBody([]byte(`{"items":"A","note":1}`))
		},
		"$.request.body.note absent": func(c *wire.Command, _ *wire.Response) {
			c.Body = wire.NewBody([]byte(`{"items":[{"sku":"A"}]}`))
		},
		"key is a boolean": func(_ *wire.Command, r *wire.Response) {
			r.Body = wire.NewBody([]byte(`{"seq":true,"total":1}`))
		},
	}
	for want, mut := range fails {
		_, err := events.Derive(svc, base(mut))
		if de, ok := events.AsError(err); !ok || de.Reason != events.ReasonEvaluationFailed || !strings.Contains(err.Error(), want) {
			t.Errorf("want failure mentioning %q, got %v", want, err)
		}
	}
}
