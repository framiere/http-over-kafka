// Package httpfailure provides credential-free descriptions of HTTP transport
// errors. Error strings may contain URLs, headers or response fragments; none
// are safe to persist, log or return to a caller after credential injection.
package httpfailure

import (
	"context"
	"errors"
	"io"
	"net"
)

// Detail classifies an error using its types and causes, never its text.
// Keep these descriptions fixed: even an inner error can contain a secret.
func Detail(err error) string {
	var network net.Error
	var dns *net.DNSError
	var operation *net.OpError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "upstream request canceled"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &network) && network.Timeout():
		return "upstream timeout"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "upstream response ended unexpectedly"
	case errors.Is(err, io.EOF):
		return "upstream connection closed before the response was complete"
	case errors.As(err, &dns):
		return "upstream name lookup failed"
	case errors.As(err, &operation):
		return "upstream network error"
	default:
		return "upstream HTTP exchange failed"
	}
}
