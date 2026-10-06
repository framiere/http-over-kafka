package wire

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// IdempotencyKeyHeader is read by the gateway (IETF httpapi idempotency draft).
const IdempotencyKeyHeader = "Idempotency-Key"

// RequestIDHeader is set by the gateway on every response, including 504s.
const RequestIDHeader = "X-Request-Id"

// Caller identity as B receives it, whichever path the request took: the
// bridge sets them for mutations, the gateway for GET passthrough. Values a
// caller sent under these names are replaced, never forwarded.
const (
	CallerApplicationHeader = "X-Caller-Application"
	CallerInstanceHeader    = "X-Caller-Instance"
)

// Caller is the authenticated identity the gateway vouches for. It is only
// trustworthy once the command signature has been verified.
type Caller struct {
	Application string `json:"application"`
	Instance    string `json:"instance"`
}

// Command is one HTTP mutation in transit. It is a request, not a fact: it
// becomes a Result once processed, and a domain event only through a mapping.
type Command struct {
	V         int    `json:"v"`
	RequestID string `json:"requestId"`
	Service   string `json:"service"`
	// OperationID and PathTemplate come from the gateway's resolution. The
	// bridge re-resolves Method+Path against its own spec and rejects drift.
	OperationID  string `json:"operationId"`
	Method       string `json:"method"`
	PathTemplate string `json:"pathTemplate"`
	// Path is the escaped path exactly as received (URL.EscapedPath), so
	// "/orders/a%2Fb" stays one segment. RawQuery likewise, without '?'.
	Path           string            `json:"path"`
	RawQuery       string            `json:"rawQuery,omitempty"`
	PathParams     map[string]string `json:"pathParams,omitempty"`
	Caller         Caller            `json:"caller"`
	Headers        Headers           `json:"headers"`
	Body           Body              `json:"body"`
	IdempotencyKey string            `json:"idempotencyKey,omitempty"`
	TraceParent    string            `json:"traceparent,omitempty"`
	// ReplyTo is the reply topic of the gateway instance holding the connection.
	ReplyTo string `json:"replyTo"`
	// Deadline is when the gateway stops waiting and answers 504. Passing it
	// does not cancel the command (D4).
	Deadline time.Time `json:"deadline"`
	// IssuedAt/ExpiresAt bound how long the signed command may be acted upon.
	// Past ExpiresAt plus ClockSkew, the bridge must not start another call
	// to B. A call already in progress may finish. Dedup state must outlive
	// ExpiresAt + 3*ClockSkew to cover all owners' clock differences.
	IssuedAt  time.Time `json:"issuedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

var mutationMethods = map[string]bool{
	http.MethodPost: true, http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true,
}

// IsMutation reports whether method travels through Kafka. Everything else
// (GET, HEAD, OPTIONS) is passthrough and never touches Kafka (D8).
func IsMutation(method string) bool { return mutationMethods[method] }

func (c Command) Validate() error {
	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if c.V != Version {
		add(fmt.Errorf("unsupported version %d", c.V))
	}
	add(validRequestID(c.RequestID))
	add(ValidateName("service", c.Service))
	if c.OperationID == "" {
		add(errors.New("operationId is empty"))
	}
	if !IsMutation(c.Method) {
		add(fmt.Errorf("method %q is not a mutation", c.Method))
	}
	if !strings.HasPrefix(c.Path, "/") || !strings.HasPrefix(c.PathTemplate, "/") {
		add(errors.New("path and pathTemplate must start with /"))
	}
	if strings.ContainsAny(c.Path+c.RawQuery, " \r\n\x00#") {
		add(errors.New("path or query contains forbidden characters"))
	}
	if c.Caller.Application == "" || c.Caller.Instance == "" {
		add(errors.New("caller application and instance are required"))
	}
	add(c.Headers.validate(true))
	if c.Body.Len() > MaxBodyBytes {
		add(fmt.Errorf("body exceeds %d bytes", MaxBodyBytes))
	}
	add(ValidateIdempotencyKey(c.IdempotencyKey))
	add(validReplyTopic(c.ReplyTo))
	if c.IssuedAt.IsZero() || c.Deadline.Before(c.IssuedAt) || !c.ExpiresAt.After(c.Deadline) {
		add(errors.New("times must satisfy issuedAt <= deadline < expiresAt"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid command: %w", err)
	}
	return nil
}

// ValidateIdempotencyKey accepts "" (no key) or 1..255 printable ASCII chars.
func ValidateIdempotencyKey(k string) error {
	if len(k) > 255 {
		return errors.New("idempotency key longer than 255")
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x20 || k[i] > 0x7e {
			return errors.New("idempotency key must be printable ASCII")
		}
	}
	return nil
}

// DedupKey identifies "the same mutation" for the bridge and is the Kafka key
// of the command and its result. With an Idempotency-Key it is the D4 scope
// (caller application, service, operation, key), so every retry of a request
// lands on the same partition and is seen by one bridge consumer in order.
// Without a key, each request is its own scope (Kafka redelivery only).
func (c Command) DedupKey() string {
	if c.IdempotencyKey == "" {
		return "r:" + c.RequestID
	}
	return "i:" + hashFields(c.Caller.Application, c.Service, c.OperationID, c.IdempotencyKey)
}

// Fingerprint identifies the request payload within an idempotency scope. Same
// DedupKey with a different Fingerprint is a key reuse: answer 422 (D4).
// Headers other than Content-Type are excluded: retries legitimately differ
// in tracing, user-agent, date headers.
func (c Command) Fingerprint() string {
	return hashFields(c.Method, c.Path, c.RawQuery, c.Headers.Get("content-type"), string(c.Body.b))
}

func hashFields(fields ...string) string {
	h := sha256.New()
	for _, f := range fields {
		fmt.Fprintf(h, "%d:", len(f))
		h.Write([]byte(f))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// URL returns path and query relative to B's base URL.
func (c Command) URL() string {
	if c.RawQuery == "" {
		return c.Path
	}
	return c.Path + "?" + c.RawQuery
}
