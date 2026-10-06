package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Fault is set when the Response was produced by the bridge instead of B.
// Every fault states whether B ran, because that is what the caller needs to
// decide whether retrying is safe.
type Fault string

const (
	// B may or may not have executed (e.g. bridge crashed after calling B, or
	// B's connection broke after the request was sent). Never retried
	// silently for non-replayable operations (D3).
	FaultOutcomeUnknown Fault = "outcome_unknown"
	// B was definitely not called: the connection could not be established.
	FaultUpstreamUnavailable Fault = "upstream_unavailable"
	// Authentic but past ExpiresAt or issued in the future; B not called.
	FaultCommandStale Fault = "command_stale"
	// The bridge's spec resolves Method+Path to another operation than the
	// gateway's. B not called.
	FaultOperationMismatch Fault = "operation_mismatch"
	// Same idempotency scope, different Fingerprint. B not called (D4: 422).
	FaultIdempotencyKeyReused Fault = "idempotency_key_reused"
	// Same idempotency scope, original still executing. B not called again.
	FaultIdempotencyInFlight Fault = "idempotency_in_flight"
	// B's response body exceeded MaxBodyBytes. B DID execute; status is known
	// but the body could not be transported.
	FaultResponseTooLarge Fault = "response_too_large"
	// B answered with a status but its body could not be read in full (B
	// closed the connection or timed out mid-body). B DID execute.
	FaultResponseIncomplete Fault = "response_incomplete"
	// The command carries a credential the service's contract declares
	// (D12): the gateway should have removed it. B is not called, and the
	// secret is not copied into the Result.
	FaultSecretInCommand Fault = "secret_in_command"
)

type faultInfo struct {
	status int
	title  string
	ran    Execution
}

var faults = map[Fault]faultInfo{
	FaultOutcomeUnknown:       {http.StatusBadGateway, "Outcome unknown: the operation may or may not have been applied", ExecutionUnknown},
	FaultUpstreamUnavailable:  {http.StatusServiceUnavailable, "Service unavailable: the operation was not applied", ExecutionNone},
	FaultCommandStale:         {http.StatusServiceUnavailable, "Request expired before processing: the operation was not applied", ExecutionNone},
	FaultOperationMismatch:    {http.StatusBadGateway, "Routing mismatch: the operation was not applied", ExecutionNone},
	FaultIdempotencyKeyReused: {http.StatusUnprocessableEntity, "Idempotency-Key reused with a different request", ExecutionNone},
	FaultIdempotencyInFlight:  {http.StatusConflict, "A request with this Idempotency-Key is still being processed", ExecutionNone},
	FaultResponseTooLarge:     {http.StatusBadGateway, "Response too large to relay: the operation was applied", ExecutionDone},
	FaultSecretInCommand:      {http.StatusBadGateway, "Request carried a credential that must not cross the backbone: the operation was not applied", ExecutionNone},
	FaultResponseIncomplete:   {http.StatusBadGateway, "Response cut off: the service processed the request, its answer was lost", ExecutionDone},
}

type Execution int

const (
	ExecutionNone Execution = iota
	ExecutionUnknown
	ExecutionDone
)

func (f Fault) Known() bool { _, ok := faults[f]; return ok }

// Status is the HTTP status the caller receives for this fault.
func (f Fault) Status() int { return faults[f].status }

// Execution tells whether B ran.
func (f Fault) Execution() Execution { return faults[f].ran }

// Response travels from the bridge to the gateway instance in Command.ReplyTo.
type Response struct {
	V         int     `json:"v"`
	RequestID string  `json:"requestId"`
	Status    int     `json:"status"`
	Headers   Headers `json:"headers"`
	Body      Body    `json:"body"`
	Fault     Fault   `json:"fault,omitempty"`
	// UpstreamStatus is B's status when B ran but its reply could not be
	// relayed (FaultResponseTooLarge). Zero otherwise.
	UpstreamStatus int `json:"upstreamStatus,omitempty"`
	// ReplayOf is the original requestId when this is a stored outcome served
	// to an idempotent retry: B was not called for RequestID.
	ReplayOf string `json:"replayOf,omitempty"`
}

func (r Response) Validate() error {
	var errs []error
	if r.V != Version {
		errs = append(errs, fmt.Errorf("unsupported version %d", r.V))
	}
	errs = append(errs, validRequestID(r.RequestID))
	if r.Status < 200 || r.Status > 599 {
		errs = append(errs, fmt.Errorf("status %d out of range", r.Status))
	}
	if r.Fault != "" {
		if !r.Fault.Known() {
			errs = append(errs, fmt.Errorf("unknown fault %q", r.Fault))
		} else if r.Status != r.Fault.Status() {
			errs = append(errs, fmt.Errorf("fault %s requires status %d", r.Fault, r.Fault.Status()))
		}
	}
	if needs := r.Fault != "" && r.Fault.Execution() == ExecutionDone; needs != (r.UpstreamStatus != 0) {
		errs = append(errs, errors.New("upstreamStatus is required exactly for faults where B ran"))
	} else if needs && (r.UpstreamStatus < 200 || r.UpstreamStatus > 599) {
		errs = append(errs, fmt.Errorf("upstreamStatus %d out of range", r.UpstreamStatus))
	}
	if r.ReplayOf != "" {
		errs = append(errs, validRequestID(r.ReplayOf))
	}
	errs = append(errs, r.Headers.validate(false))
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("invalid response: %w", err)
	}
	return nil
}

// Problem is an RFC 9457 body, used for every response the system itself
// generates (bridge faults, gateway 504) so callers parse one format.
type Problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"requestId"`
}

const ProblemContentType = "application/problem+json"

// ProblemTypeTimeout is the type of the gateway's 504 (D4): the command was
// published and may still execute.
const ProblemTypeTimeout = "urn:http-over-kafka:gateway-timeout"

func ProblemType(f Fault) string { return "urn:http-over-kafka:" + string(f) }

func (p Problem) Bytes() []byte {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err) // only strings and an int
	}
	return b
}

// FaultResponse builds the bridge-generated Response for f. For faults where
// B ran (FaultResponseTooLarge) set UpstreamStatus on the result.
func FaultResponse(requestID string, f Fault, detail string) Response {
	p := Problem{Type: ProblemType(f), Title: faults[f].title, Status: f.Status(), Detail: detail, RequestID: requestID}
	return Response{
		V:         Version,
		RequestID: requestID,
		Status:    f.Status(),
		Headers:   Headers{"content-type": {ProblemContentType}},
		Body:      NewBody(p.Bytes()),
		Fault:     f,
	}
}

// Outcome classifies a processed command. It is what Result carries.
type Outcome string

const (
	OutcomeSucceeded   Outcome = "Succeeded"   // B answered 2xx
	OutcomeFailed      Outcome = "Failed"      // B answered non-2xx
	OutcomeNotExecuted Outcome = "NotExecuted" // B was not called
	OutcomeUnknown     Outcome = "OutcomeUnknown"
)

func OutcomeOf(r Response) Outcome {
	if r.Fault != "" {
		switch r.Fault.Execution() {
		case ExecutionNone:
			return OutcomeNotExecuted
		case ExecutionUnknown:
			return OutcomeUnknown
		}
		return statusOutcome(r.UpstreamStatus)
	}
	return statusOutcome(r.Status)
}

func statusOutcome(status int) Outcome {
	if status >= 200 && status < 300 {
		return OutcomeSucceeded
	}
	return OutcomeFailed
}

// Result is the durable record that a command was processed (D7). Exactly one
// per original command; replays (Response.ReplayOf != "") never produce one,
// otherwise audit double-counts and event derivation emits duplicates.
// It embeds the full command so consumers derive events statelessly.
type Result struct {
	V int `json:"v"`
	// Type is e.g. "CreateOrderSucceeded": operationId + Outcome.
	Type        string    `json:"type"`
	Outcome     Outcome   `json:"outcome"`
	Command     Command   `json:"command"`
	Response    Response  `json:"response"`
	CompletedAt time.Time `json:"completedAt"`
}

func NewResult(cmd Command, resp Response, completedAt time.Time) (Result, error) {
	out := OutcomeOf(resp)
	r := Result{V: Version, Type: ResultType(cmd.OperationID, out), Outcome: out, Command: cmd, Response: resp, CompletedAt: completedAt.UTC()}
	return r, r.Validate()
}

func ResultType(operationID string, o Outcome) string {
	if operationID == "" {
		return string(o)
	}
	first := operationID[0]
	if first >= 'a' && first <= 'z' {
		first -= 'a' - 'A'
	}
	return string(first) + operationID[1:] + string(o)
}

func (r Result) Validate() error {
	if r.V != Version {
		return fmt.Errorf("invalid result: unsupported version %d", r.V)
	}
	if err := r.Command.Validate(); err != nil {
		return fmt.Errorf("invalid result: %w", err)
	}
	if err := r.Response.Validate(); err != nil {
		return fmt.Errorf("invalid result: %w", err)
	}
	switch {
	case r.Response.RequestID != r.Command.RequestID:
		return errors.New("invalid result: response and command requestId differ")
	case r.Response.ReplayOf != "":
		return errors.New("invalid result: replayed responses never produce a result")
	case r.Outcome != OutcomeOf(r.Response) || r.Type != ResultType(r.Command.OperationID, r.Outcome):
		return errors.New("invalid result: outcome/type inconsistent with response")
	case r.CompletedAt.IsZero():
		return errors.New("invalid result: completedAt is zero")
	}
	return nil
}
