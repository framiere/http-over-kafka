package wire

import (
	"fmt"
	"net/http"
	"net/textproto"
	"slices"
	"strings"
)

// Headers is a multi-valued HTTP header set keyed by lowercase name.
// Value order is preserved: for repeated headers it can be significant.
type Headers map[string][]string

// Caller secrets never enter Kafka (D5). The gateway authenticates with them;
// the bridge rebuilds whatever B needs from the signed Caller.
var secretRequestHeaders = map[string]bool{
	"authorization":       true,
	"proxy-authorization": true,
	"cookie":              true,
	"x-api-key":           true,
}

// Connection-scoped (RFC 9110 §7.6.1) or recomputed on the far side.
var hopHeaders = map[string]bool{
	"connection":        true,
	"keep-alive":        true,
	"proxy-connection":  true,
	"te":                true,
	"trailer":           true,
	"transfer-encoding": true,
	"upgrade":           true,
	"content-length":    true,
	"host":              true,
	"expect":            true,
}

// RequestHeaders converts the caller's headers for transport: secrets, hop-by-hop
// headers and anything listed in Connection are dropped.
func RequestHeaders(h http.Header) Headers {
	return fromHTTP(h, true)
}

// ResponseHeaders converts B's response headers for transport. Set-Cookie is kept:
// the caller expects it, and it is B's secret to hand out, not the caller's.
func ResponseHeaders(h http.Header) Headers {
	return fromHTTP(h, false)
}

func fromHTTP(h http.Header, request bool) Headers {
	drop := map[string]bool{}
	for _, v := range h.Values("Connection") {
		for name := range strings.SplitSeq(v, ",") {
			drop[strings.ToLower(strings.TrimSpace(name))] = true
		}
	}
	out := Headers{}
	for name, values := range h {
		n := strings.ToLower(name)
		if hopHeaders[n] || drop[n] || (request && secretRequestHeaders[n]) || len(values) == 0 {
			continue
		}
		out[n] = slices.Clone(values)
	}
	return out
}

// HTTP returns a fresh http.Header; mutating it does not touch h.
func (h Headers) HTTP() http.Header {
	out := make(http.Header, len(h))
	for name, values := range h {
		out[textproto.CanonicalMIMEHeaderKey(name)] = slices.Clone(values)
	}
	return out
}

// Get returns the first value of name (case-insensitive), or "".
func (h Headers) Get(name string) string {
	if v := h[strings.ToLower(name)]; len(v) > 0 {
		return v[0]
	}
	return ""
}

func (h Headers) validate(request bool) error {
	for name, values := range h {
		if name != strings.ToLower(name) || !validToken(name) {
			return fmt.Errorf("header name %q is not a lowercase token", name)
		}
		if hopHeaders[name] {
			return fmt.Errorf("header %q must not be transported", name)
		}
		if request && secretRequestHeaders[name] {
			return fmt.Errorf("header %q is a caller secret and must never be in a command", name)
		}
		if len(values) == 0 {
			return fmt.Errorf("header %q has no value", name)
		}
		for _, v := range values {
			if strings.ContainsAny(v, "\r\n\x00") {
				return fmt.Errorf("header %q value contains CR, LF or NUL", name)
			}
		}
	}
	return nil
}

func validToken(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.ContainsRune("!#$%&'*+-.^_`|~", c):
		default:
			return false
		}
	}
	return true
}
