package apispec_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
)

// Parsing a body expression and loading it in a nested event value must
// agree. In particular, zero navigation steps must never panic in validation.
func FuzzWholeBodyEventExpressionValidation(f *testing.F) {
	for _, suffix := range []string{"", ".items[0].id", "[0]", "[999999]", "[1000000]", ".Authorization", "x", ".", "[01]", ".x[0].nested"} {
		f.Add(suffix, false)
		f.Add(suffix, true)
	}
	f.Fuzz(func(t *testing.T, suffix string, response bool) {
		if len(suffix) > 4096 {
			t.Skip()
		}
		source := "$.request.body"
		if response {
			source = "$.response.body"
		}
		expr := source + suffix
		quoted, err := json.Marshal(expr)
		if err != nil {
			t.Fatal(err)
		}
		src := fmt.Sprintf(`openapi: 3.0.3
info: {title: expressions, version: '1'}
paths:
  /orders:
    post:
      operationId: createOrder
      responses: {'201': {description: ok}}
      x-conduktor-event:
        on: 201
        topic: orders.events
        type: OrderCreated
        key: $.response.body.id
        value:
          nested:
            body: %s
`, quoted)
		_, parseErr := apispec.ParseExpr(expr)
		_, loadErr := apispec.Load("orders", []byte(src))
		if (parseErr == nil) != (loadErr == nil) {
			t.Fatalf("expression %q: parser=%v loader=%v", expr, parseErr, loadErr)
		}
	})
}
