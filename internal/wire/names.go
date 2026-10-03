package wire

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/oklog/ulid/v2"
)

// Version is the wire major version. Any field change, additive included,
// bumps it: commands are decoded strictly (see DecodeCommand).
const Version = 1

// Service and gateway instance names become topic suffixes, hence the charset.
var namePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)

func ValidateName(kind, s string) error {
	if !namePattern.MatchString(s) {
		return fmt.Errorf("%s %q must match %s", kind, s, namePattern)
	}
	return nil
}

const (
	commandTopicPrefix = "http.requests."
	replyTopicPrefix   = "http.responses."
	resultTopicPrefix  = "http.results."
)

// CommandTopic carries commands for one service. Record key: Command.DedupKey().
func CommandTopic(service string) string { return commandTopicPrefix + service }

// ReplyTopic is owned by exactly one gateway instance (D6): the bridge sends the
// Response to the topic named in Command.ReplyTo, keyed by requestId. Instance
// ids must be stable across restarts, or reply topics pile up.
func ReplyTopic(gatewayInstance string) string { return replyTopicPrefix + gatewayInstance }

// ResultTopic is the durable per-service outcome stream (D7). Record key:
// Command.DedupKey(), the same key as the command, so with equal partition
// counts a result lands on the partition number of its command.
func ResultTopic(service string) string { return resultTopicPrefix + service }

func validReplyTopic(t string) error {
	inst, ok := strings.CutPrefix(t, replyTopicPrefix)
	if !ok {
		return fmt.Errorf("replyTo %q must start with %q", t, replyTopicPrefix)
	}
	return ValidateName("gateway instance", inst)
}

// NewRequestID mints a request id. Only the gateway mints them, never from a
// client-supplied header: a caller choosing ids could collide with another
// caller's request and receive its response.
func NewRequestID() string { return ulid.Make().String() }

func validRequestID(s string) error {
	if _, err := ulid.ParseStrict(s); err != nil {
		return fmt.Errorf("requestId %q is not a ULID: %w", s, err)
	}
	return nil
}
