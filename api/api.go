// Package api holds the OpenAPI documents of the demo services, embedded so
// tests and binaries load the exact same bytes.
package api

import _ "embed"

//go:embed orders.openapi.yaml
var Orders []byte

//go:embed payments.openapi.yaml
var Payments []byte
