package gateway

import (
	"net/http"
	"strconv"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

// Problem types the gateway itself answers with. Like the bridge faults they
// are RFC 9457 bodies carrying the requestId, so a caller parses one format
// whichever component refused the request.
const (
	ProblemUnauthorized         = "urn:kafka-backbone:unauthorized"
	ProblemUnknownService       = "urn:kafka-backbone:unknown_service"
	ProblemNoOperation          = "urn:kafka-backbone:no_operation"
	ProblemMethodNotAllowed     = "urn:kafka-backbone:method_not_allowed"
	ProblemPayloadTooLarge      = "urn:kafka-backbone:payload_too_large"
	ProblemInvalidRequest       = "urn:kafka-backbone:invalid_request"
	ProblemTransportUnavailable = "urn:kafka-backbone:transport_unavailable"
)

// ProblemUpstreamUnavailable is a GET passthrough that could not reach B. It
// reuses the bridge's fault type: same meaning, B was not reached.
var ProblemUpstreamUnavailable = wire.ProblemType(wire.FaultUpstreamUnavailable)

func writeProblem(w http.ResponseWriter, requestID string, status int, typ, title, detail string) {
	b := wire.Problem{Type: typ, Title: title, Status: status, Detail: detail, RequestID: requestID}.Bytes()
	h := w.Header()
	h.Set("Content-Type", wire.ProblemContentType)
	h.Set("Content-Length", strconv.Itoa(len(b)))
	h.Set(wire.RequestIDHeader, requestID)
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
