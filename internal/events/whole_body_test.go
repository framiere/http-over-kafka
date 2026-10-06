package events_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/events"
)

const wholeBodySpec = `openapi: 3.0.3
info: {title: bodies, version: '1'}
paths:
  /orders:
    post:
      operationId: createOrder
      responses: {'201': {description: ok}}
      x-conduktor-event:
        on: 201
        topic: orders.events
        type: OrderCreated
        key: %s
        value:
          request: $.request.body
          response: $.response.body
`

// Load and evaluate together: parsing an expression successfully is not enough
// if validating its mapping panics, or the resulting event changes its value.
func TestDeriveWholeBodies(t *testing.T) {
	for _, tc := range []struct{ name, key, request, response, wantKey, wantValue string }{
		{"object and array", "$.request.body.id", `{"id":"order-1","n":9007199254740993}`, `[1,null,{"x":true}]`, "order-1", `{"request":{"id":"order-1","n":9007199254740993},"response":[1,null,{"x":true}]}`},
		{"scalar response key", "$.response.body", `null`, `9007199254740993`, "9007199254740993", `{"request":null,"response":9007199254740993}`},
		{"scalar request key", "$.request.body", `"order-1"`, `null`, "order-1", `{"request":"order-1","response":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := load(t, "orders", []byte(fmt.Sprintf(wholeBodySpec, tc.key)))
			cmd := createOrder(t, "orders", tc.request)
			ev, err := events.Derive(svc, result(t, cmd, answered(cmd, 201, tc.response)))
			if err != nil || ev == nil {
				t.Fatalf("derive: event=%v error=%v", ev, err)
			}
			if string(ev.Key) != tc.wantKey || string(ev.Value) != tc.wantValue {
				t.Fatalf("key=%s value=%s; want key=%s value=%s", ev.Key, ev.Value, tc.wantKey, tc.wantValue)
			}
		})
	}
}

func TestWholeBodyMappingStillRejectsInvalidValues(t *testing.T) {
	svc := load(t, "orders", []byte(fmt.Sprintf(wholeBodySpec, "$.response.body")))
	for _, body := range []string{`{}`, `[]`, `null`, `true`, `""`, "", "not json"} {
		t.Run(fmt.Sprintf("response=%q", body), func(t *testing.T) {
			cmd := createOrder(t, "orders", `{}`)
			ev, err := events.Derive(svc, result(t, cmd, answered(cmd, 201, body)))
			de, ok := events.AsError(err)
			if ev != nil || !ok || de.Reason != events.ReasonEvaluationFailed {
				t.Fatalf("event=%v error=%v", ev, err)
			}
		})
	}
	// Whole-body fields must not bypass checks on other expressions.
	for _, expr := range []string{"$.request.headers.Authorization", "$.response.headers.Set-Cookie", "$.request.pathParams.missing"} {
		t.Run(expr, func(t *testing.T) {
			src := fmt.Sprintf(wholeBodySpec, "$.response.body") + "          forbidden: " + expr + "\n"
			if _, err := apispec.Load("orders", []byte(src)); err == nil || !strings.Contains(err.Error(), expr) {
				t.Fatalf("want rejected expression %s, got %v", expr, err)
			}
		})
	}
}
