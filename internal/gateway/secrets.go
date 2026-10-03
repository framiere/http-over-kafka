package gateway

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

// D12. What A sends where B's contract expects a credential is A's secret:
// it never enters Kafka, and B never receives it on any path. The gateway has
// already authenticated A (JWT); B learns who called from X-Caller-*.
//
// When B's contract requires a credential, that credential belongs to the
// provider side, not to A: it is configured per service and injected by
// whoever calls B (the bridge for mutations, the gateway for passthrough).
// Forwarding A's secret instead would either put it in Kafka (mutations,
// D5) or give B two different credentials depending on the method.
type secretFilter struct {
	headers map[string]bool
	query   map[string]bool
}

func newSecretFilter(s apispec.Secrets) secretFilter {
	f := secretFilter{headers: map[string]bool{}, query: map[string]bool{}}
	for _, h := range s.Headers {
		f.headers[h] = true
	}
	for _, q := range s.Query {
		f.query[q] = true
	}
	// Cookie-scheme secrets travel in Cookie, which wire.RequestHeaders
	// already drops whole.
	return f
}

// requestHeaders is wire.RequestHeaders minus the contract's secret headers.
func (f secretFilter) requestHeaders(h http.Header) wire.Headers {
	out := wire.RequestHeaders(h)
	for name := range out {
		if f.headers[name] {
			delete(out, name)
		}
	}
	return out
}

// rawQuery drops secret parameters and keeps every other byte as received:
// no re-encoding, no reordering.
func (f secretFilter) rawQuery(raw string) string {
	if raw == "" || len(f.query) == 0 {
		return raw
	}
	kept := make([]string, 0, strings.Count(raw, "&")+1)
	for part := range strings.SplitSeq(raw, "&") {
		key, _, _ := strings.Cut(part, "=")
		if k, err := url.QueryUnescape(key); err == nil && f.query[k] {
			continue
		}
		kept = append(kept, part)
	}
	return strings.Join(kept, "&")
}
