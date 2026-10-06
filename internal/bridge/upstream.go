package bridge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/httpfailure"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// Headers the bridge sets on every request to B. Any value the caller sent
// under these names is replaced: B may trust them exactly as much as it trusts
// its network path from the bridge. A service that ignores them works as is (D2).
const (
	HeaderCallerApplication = wire.CallerApplicationHeader
	HeaderCallerInstance    = wire.CallerInstanceHeader
)

// upstream calls B. Its one job beyond HTTP is to never send a request twice
// on its own and to say precisely whether B may have seen it.
type upstream struct {
	base    *url.URL
	client  *http.Client
	timeout time.Duration
	// D12: B's own credentials, injected per operation; nil spec: none.
	spec  *apispec.Service
	creds apispec.Credentials
}

func newUpstream(base string, timeout time.Duration) (*upstream, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("upstream url: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("upstream url %q: want http(s)://host[:port][/prefix]", base)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	// The caller's Accept-Encoding travels in the command; adding our own
	// would make Go decompress and strip Content-Encoding behind its back.
	tr.DisableCompression = true
	tr.MaxIdleConnsPerHost = 64
	return &upstream{
		base: u,
		client: &http.Client{
			Transport: tr,
			// A 307/308 would make the client send the mutation again,
			// to wherever B points: relay it instead.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		timeout: timeout,
	}, nil
}

// emptyBody is a request body that is neither nil nor http.NoBody. Go's
// transport silently resends a request on a reused connection that broke
// after the request was written when it deems the request "replayable": no
// body (or a rewindable one) and an idempotent method or an Idempotency-Key
// header. A POST carrying the caller's Idempotency-Key qualifies, B may have
// executed it, and we would never know. A non-nil, non-rewindable body makes
// every request non-replayable for the transport; it still goes out as
// Content-Length: 0.
type emptyBody struct{}

func (emptyBody) Read([]byte) (int, error) { return 0, io.EOF }
func (emptyBody) Close() error             { return nil }

func (u *upstream) request(ctx context.Context, cmd wire.Command, op *apispec.Operation) (*http.Request, error) {
	h := cmd.Headers.HTTP()
	rawQuery := cmd.RawQuery
	hasCredentials := false
	if u.spec != nil {
		// The gateway already removed A's secrets; dropping the contract's
		// credential headers again costs nothing and leaves only ours.
		for _, name := range u.spec.Secrets().Headers {
			h.Del(name)
		}
		h.Del("Cookie")
		if op != nil {
			alt, ok := u.creds.Alternative(op)
			if !ok { // Check at startup makes this unreachable
				return nil, fmt.Errorf("no configured credential satisfies %v", op.Security)
			}
			rawQuery = u.creds.Inject(u.spec, alt, h, rawQuery)
			hasCredentials = len(alt) > 0
		}
	}
	target := *u.base
	target.RawPath = strings.TrimSuffix(u.base.EscapedPath(), "/") + cmd.Path
	p, err := url.PathUnescape(target.RawPath)
	if err != nil {
		return nil, err
	}
	target.Path = p
	target.RawQuery = rawQuery

	req, err := http.NewRequestWithContext(ctx, cmd.Method, target.String(), nil)
	if err != nil {
		return nil, err
	}
	if b := cmd.Body.Bytes(); len(b) > 0 {
		req.Body = io.NopCloser(bytes.NewReader(b))
		req.ContentLength = int64(len(b))
	} else {
		req.Body = emptyBody{}
	}
	req.GetBody = nil

	for _, name := range []string{HeaderCallerApplication, HeaderCallerInstance, wire.RequestIDHeader} {
		h.Del(name)
	}
	h.Set(HeaderCallerApplication, cmd.Caller.Application)
	h.Set(HeaderCallerInstance, cmd.Caller.Instance)
	h.Set(wire.RequestIDHeader, cmd.RequestID)
	if cmd.TraceParent != "" {
		h.Set("Traceparent", cmd.TraceParent)
	}
	if cmd.IdempotencyKey != "" && h.Get(wire.IdempotencyKeyHeader) == "" {
		h.Set(wire.IdempotencyKeyHeader, cmd.IdempotencyKey)
	}
	if h.Get("User-Agent") == "" {
		h["User-Agent"] = nil // Go would otherwise add its own
	}
	req.Header = h
	// Go's HTTP/1 transport logs unsolicited bytes on idle connections to
	// log.Default. A malformed response can reflect a credential there,
	// outside our error sanitizer. Never return a credential-bearing
	// connection to the pool; anonymous requests retain normal pooling.
	req.Close = hasCredentials || req.URL.User != nil || h.Get("Authorization") != "" || h.Get("Proxy-Authorization") != "" || h.Get("Cookie") != ""
	return req, nil
}

// call executes cmd against B exactly once and classifies the result. It
// never returns an error: every failure is a Response whose Fault says
// whether B ran.
func (u *upstream) call(ctx context.Context, cmd wire.Command, op *apispec.Operation) wire.Response {
	ctx, cancel := context.WithTimeout(ctx, u.timeout)
	defer cancel()
	// Until a connection is handed to the transport, nothing can have
	// reached B. After that, anything may have, and only a response proves
	// what happened.
	var connected atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) { connected.Store(true) },
	})
	req, err := u.request(ctx, cmd, op)
	if err != nil {
		return wire.FaultResponse(cmd.RequestID, wire.FaultUpstreamUnavailable, "cannot build upstream request")
	}
	resp, err := u.client.Do(req)
	if err != nil {
		if !connected.Load() {
			return wire.FaultResponse(cmd.RequestID, wire.FaultUpstreamUnavailable, httpfailure.Detail(err))
		}
		return wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown, httpfailure.Detail(err))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, wire.MaxBodyBytes+1))
	if err != nil {
		r := wire.FaultResponse(cmd.RequestID, wire.FaultResponseIncomplete, httpfailure.Detail(err))
		r.UpstreamStatus = resp.StatusCode
		return r
	}
	if len(body) > wire.MaxBodyBytes {
		r := wire.FaultResponse(cmd.RequestID, wire.FaultResponseTooLarge, fmt.Sprintf("body exceeds %d bytes", wire.MaxBodyBytes))
		r.UpstreamStatus = resp.StatusCode
		return r
	}
	out := wire.Response{
		V:         wire.Version,
		RequestID: cmd.RequestID,
		Status:    resp.StatusCode,
		Headers:   wire.ResponseHeaders(resp.Header),
		Body:      wire.NewBody(body),
	}
	if err := out.Validate(); err != nil {
		// B answered something we cannot relay or classify (e.g. a status
		// outside 200..599): saying more than "unknown" would be a guess.
		return wire.FaultResponse(cmd.RequestID, wire.FaultOutcomeUnknown, "service returned an invalid HTTP response")
	}
	return out
}

// redactSecrets finds credentials the service's contract declares (D12) in a
// command, and returns the command without them, for the Result: the secret
// must not spread further than the command topic it already reached. found
// names the locations, never the values.
func (b *Bridge) redactSecrets(cmd wire.Command) (wire.Command, []string) {
	sec := b.spec.Secrets()
	var found []string
	headers := wire.Headers{}
	for name, v := range cmd.Headers {
		if slices.Contains(sec.Headers, name) || name == "cookie" {
			found = append(found, "header "+name)
			continue
		}
		headers[name] = v
	}
	var kept []string
	if cmd.RawQuery != "" {
		for part := range strings.SplitSeq(cmd.RawQuery, "&") {
			key, _, _ := strings.Cut(part, "=")
			if k, err := url.QueryUnescape(key); err == nil && slices.Contains(sec.Query, k) {
				found = append(found, "query parameter "+k)
				continue
			}
			kept = append(kept, part)
		}
	}
	if len(found) == 0 {
		return cmd, nil
	}
	slices.Sort(found)
	out := cmd
	out.Headers = headers
	out.RawQuery = strings.Join(kept, "&")
	return out, found
}
