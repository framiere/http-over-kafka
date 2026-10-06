package httpfailure_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"testing"

	"github.com/sderosiaux/http-over-kafka/internal/httpfailure"
)

func TestDetailDoesNotFormatTransportErrors(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
		want  string
	}{
		{"opaque", errors.New("secret in unknown error"), "upstream HTTP exchange failed"},
		{"cancel", context.Canceled, "upstream request canceled"},
		{"timeout", context.DeadlineExceeded, "upstream timeout"},
		{"closed", io.EOF, "upstream connection closed before the response was complete"},
		{"incomplete", io.ErrUnexpectedEOF, "upstream response ended unexpectedly"},
		{"dns", &net.DNSError{Name: "secret-host", Err: "secret diagnostic"}, "upstream name lookup failed"},
		{"network", &net.OpError{Op: "secret operation", Net: "tcp", Err: errors.New("secret")}, "upstream network error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nested := fmt.Errorf("secret in wrapper: %w", tc.cause)
			wrapped := &url.Error{Op: "secret operation", URL: "https://u:secret@example.invalid/?key=secret", Err: nested}
			for _, err := range []error{tc.cause, nested, wrapped} {
				if got := httpfailure.Detail(err); got != tc.want {
					t.Fatalf("detail %q, want %q", got, tc.want)
				}
			}
		})
	}
}
