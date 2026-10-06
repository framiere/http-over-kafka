package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

type passthroughKey struct{}

type passthroughInfo struct {
	requestID string
	caller    wire.Caller
	op        *apispec.Operation
}

var errPassthroughTimeout = errors.New("passthrough timeout")

// passthrough proxies a read straight to B (D8): nothing touches Kafka. The
// timeout bounds the whole exchange, body included.
func (g *Gateway) passthrough(w http.ResponseWriter, r *http.Request, requestID string, svc *service, op *apispec.Operation, caller wire.Caller) {
	ctx, cancel := context.WithTimeoutCause(r.Context(), g.cfg.Timeout, errPassthroughTimeout)
	defer cancel()
	svc.proxy.ServeHTTP(w, r.WithContext(context.WithValue(ctx, passthroughKey{}, passthroughInfo{requestID, caller, op})))
}

func infoOf(r *http.Request) passthroughInfo {
	return r.Context().Value(passthroughKey{}).(passthroughInfo)
}

// newPassthrough builds the proxy for one B. B receives the request the bridge
// would build for a mutation (no caller secrets, standard or declared by the
// contract; no hop-by-hop; caller identity and requestId set by us; the
// service's own credential when the operation requires one), so B sees one
// request shape whichever transport was used.
func (g *Gateway) newPassthrough(svc *service) http.Handler {
	upstream := svc.Upstream
	return &httputil.ReverseProxy{
		Transport: g.cfg.Transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			in := pr.In.URL
			// Path kept exactly as received, escaping included: no cleaning,
			// no re-encoding, so "/orders/a%2Fb" stays one segment.
			pr.Out.URL = &url.URL{
				Scheme:   upstream.Scheme,
				Host:     upstream.Host,
				Path:     upstream.Path + in.Path,
				RawPath:  upstream.EscapedPath() + in.EscapedPath(),
				RawQuery: svc.secrets.rawQuery(in.RawQuery),
			}
			pr.Out.Host = ""
			info := infoOf(pr.In)
			h := svc.secrets.requestHeaders(pr.In.Header).HTTP()
			alt, _ := svc.Credentials.Alternative(info.op) // satisfiable: checked in New
			pr.Out.URL.RawQuery = svc.Credentials.Inject(svc.Spec, alt, h, pr.Out.URL.RawQuery)
			h.Set(wire.CallerApplicationHeader, info.caller.Application)
			h.Set(wire.CallerInstanceHeader, info.caller.Instance)
			h.Set(wire.RequestIDHeader, info.requestID)
			pr.Out.Header = h
		},
		ModifyResponse: func(resp *http.Response) error {
			resp.Header.Set(wire.RequestIDHeader, infoOf(resp.Request).requestID)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			requestID := infoOf(r).requestID
			switch {
			case errors.Is(context.Cause(r.Context()), errPassthroughTimeout):
				writeProblem(w, requestID, http.StatusGatewayTimeout, wire.ProblemTypeTimeout,
					"Gateway timeout", "no response from the service within "+g.cfg.Timeout.String())
			case r.Context().Err() != nil:
				// Caller left; nobody to answer.
			default:
				g.log.Warn("passthrough failed", "requestId", requestID, "err", err)
				writeProblem(w, requestID, http.StatusServiceUnavailable, ProblemUpstreamUnavailable,
					"Service unavailable", "the service could not be reached")
			}
		},
	}
}
