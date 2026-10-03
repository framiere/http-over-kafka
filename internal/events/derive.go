// Package events turns Results into domain events (D7, D9).
//
// A domain event is a claim that a business fact happened. It is emitted only
// when B really executed the command once and answered the status the
// x-conduktor-event mapping names; never from a command, a Response, a fault,
// an unknown outcome or a replay. Anything the mapping cannot evaluate goes to
// a failure topic instead of becoming a partial event.
package events

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/apispec"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

// Event is a derived domain event, before it becomes a Kafka record.
type Event struct {
	Topic string
	Type  string
	Key   []byte
	Value []byte
	// ID is the requestId of the command: one command, at most one event, so
	// consumers that see the same ID twice know it is the same fact.
	ID          string
	Source      string
	Time        time.Time
	TraceParent string
}

// Error is a derivation that should have produced an event and could not.
type Error struct {
	Reason  string   // machine-readable
	Details []string // one entry per failing expression or check
}

func (e *Error) Error() string {
	return e.Reason + ": " + strings.Join(e.Details, "; ")
}

const (
	ReasonUnknownOperation  = "unknown_operation"
	ReasonOperationMismatch = "operation_mismatch"
	ReasonBodyNotRelayed    = "body_not_relayed"
	ReasonEvaluationFailed  = "evaluation_failed"
	ReasonUndecodableResult = "undecodable_result"
	ReasonNotAuthentic      = "not_authentic"
	ReasonMisrouted         = "misrouted"
	ReasonDuplicateResult   = "duplicate_result"
	ReasonBeyondDedupWindow = "beyond_dedup_window"
)

func fail(reason string, format string, a ...any) *Error {
	return &Error{Reason: reason, Details: []string{fmt.Sprintf(format, a...)}}
}

// Derive returns the event res yields under svc's mappings: nil when no
// mapping applies, an *Error when one applies but cannot be honoured.
// res must come from wire.DecodeResult: authenticated, validated, never a
// replay.
func Derive(svc *apispec.Service, res wire.Result) (*Event, error) {
	cmd, resp := res.Command, res.Response
	if cmd.Service != svc.Name() {
		return nil, fail(ReasonOperationMismatch, "result for service %q, deriver serves %q", cmd.Service, svc.Name())
	}
	op, ok := svc.Operation(cmd.OperationID)
	if !ok {
		// The bridge knows an operation we do not: we cannot tell whether
		// it maps to an event, so say so rather than guess "no".
		return nil, fail(ReasonUnknownOperation, "operation %q is not in the deriver's spec", cmd.OperationID)
	}
	if op.Method != cmd.Method || op.PathTemplate != cmd.PathTemplate {
		return nil, fail(ReasonOperationMismatch, "operation %q is %s %s in the deriver's spec, result says %s %s",
			op.ID, op.Method, op.PathTemplate, cmd.Method, cmd.PathTemplate)
	}
	m := op.Event
	if m == nil {
		return nil, nil
	}
	if resp.Fault != "" {
		// B ran and answered the mapped status, but its body never reached
		// Kafka: the fact happened and cannot be described. Silence would
		// lose it; a partial event would lie.
		if resp.Fault.Execution() == wire.ExecutionDone && resp.UpstreamStatus == m.On {
			return nil, fail(ReasonBodyNotRelayed, "B answered %d but the response was not relayed (%s)", m.On, resp.Fault)
		}
		return nil, nil // not executed, or outcome unknown: no fact to report
	}
	if resp.Status != m.On || res.Outcome != wire.OutcomeSucceeded {
		return nil, nil
	}

	x := newExchange(cmd, resp)
	var errs []string
	key, err := x.renderKey(m.Key)
	if err != nil {
		errs = append(errs, "key ("+m.Key.String()+"): "+err.Error())
	}
	value := x.renderValue(m.Value, "value", &errs)
	if len(errs) > 0 {
		return nil, &Error{Reason: ReasonEvaluationFailed, Details: errs}
	}
	b, err := marshalCompact(value)
	if err != nil {
		return nil, fail(ReasonEvaluationFailed, "encode value: %v", err)
	}
	return &Event{
		Topic:       m.Topic,
		Type:        m.Type,
		Key:         key,
		Value:       b,
		ID:          cmd.RequestID,
		Source:      Source(cmd.Service, cmd.OperationID),
		Time:        res.CompletedAt,
		TraceParent: cmd.TraceParent,
	}, nil
}

// Source is the CloudEvents source of events derived from an operation.
func Source(service, operationID string) string {
	return "/services/" + service + "/operations/" + operationID
}

// AsError extracts a derivation error.
func AsError(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}
