package events_test

import (
	"testing"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

// command builds a valid command as the gateway would.
func command(t testing.TB, service, opID, method, path, tmpl string, body string) wire.Command {
	t.Helper()
	now := time.Now().UTC()
	return wire.Command{
		V:            wire.Version,
		RequestID:    wire.NewRequestID(),
		Service:      service,
		OperationID:  opID,
		Method:       method,
		PathTemplate: tmpl,
		Path:         path,
		Caller:       wire.Caller{Application: "checkout", Instance: "checkout-1"},
		Headers:      wire.Headers{"content-type": {"application/json"}},
		Body:         wire.NewBody([]byte(body)),
		ReplyTo:      wire.ReplyTopic("gw-1"),
		IssuedAt:     now,
		Deadline:     now.Add(5 * time.Second),
		ExpiresAt:    now.Add(time.Minute),
	}
}

func createOrder(t testing.TB, service, body string) wire.Command {
	return command(t, service, "createOrder", "POST", "/orders", "/orders", body)
}

// answered is B's real response.
func answered(cmd wire.Command, status int, body string) wire.Response {
	return wire.Response{
		V:         wire.Version,
		RequestID: cmd.RequestID,
		Status:    status,
		Headers:   wire.Headers{"content-type": {"application/json"}, "location": {"/orders/x"}},
		Body:      wire.NewBody([]byte(body)),
	}
}

func result(t testing.TB, cmd wire.Command, resp wire.Response) wire.Result {
	t.Helper()
	r, err := wire.NewResult(cmd, resp, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func load(t testing.TB, service string, spec []byte) *apispec.Service {
	t.Helper()
	s, err := apispec.Load(service, spec)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const orderReq = `{"customerId":"c-42","items":[{"sku":"A123","quantity":2}]}`

func orderResp(id string) string {
	return `{"id":"` + id + `","customerId":"c-42","items":[{"sku":"A123","quantity":2}],"version":1,"createdAt":"2026-10-02T10:00:00Z"}`
}
