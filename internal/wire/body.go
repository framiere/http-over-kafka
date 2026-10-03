package wire

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// MaxBodyBytes caps request and response bodies. A Result carries both, base64
// in the worst case, so it must fit the topic's max.message.bytes (kafkaenv).
const MaxBodyBytes = 1 << 20

// Body is an HTTP body transported byte-exact.
//
// On the wire it is {"json": <value>} when the bytes are already compact JSON
// without HTML-sensitive characters (so any JSON encoder reproduces them
// verbatim and audit consumers can read them), {"base64": "..."} otherwise,
// and null when empty. Decoding always yields the original bytes.
type Body struct {
	b []byte
}

func NewBody(b []byte) Body {
	if len(b) == 0 {
		return Body{}
	}
	return Body{b: bytes.Clone(b)}
}

// Bytes returns a copy of the raw body.
func (b Body) Bytes() []byte { return bytes.Clone(b.b) }
func (b Body) Len() int      { return len(b.b) }
func (b Body) IsEmpty() bool { return len(b.b) == 0 }

// JSON reports whether the body is syntactically valid JSON, regardless of
// Content-Type. Consumers evaluating expressions on the body need this.
func (b Body) JSON() (json.RawMessage, bool) {
	if len(b.b) == 0 || !json.Valid(b.b) {
		return nil, false
	}
	return json.RawMessage(bytes.Clone(b.b)), true
}

type bodyWire struct {
	JSON   json.RawMessage `json:"json,omitempty"`
	Base64 *string         `json:"base64,omitempty"`
}

func (b Body) MarshalJSON() ([]byte, error) {
	if len(b.b) == 0 {
		return []byte("null"), nil
	}
	if verbatimJSON(b.b) {
		return json.Marshal(bodyWire{JSON: b.b})
	}
	s := base64.StdEncoding.EncodeToString(b.b)
	return json.Marshal(bodyWire{Base64: &s})
}

func (b *Body) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		*b = Body{}
		return nil
	}
	var w bodyWire
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return fmt.Errorf("body: %w", err)
	}
	switch {
	case w.JSON != nil && w.Base64 != nil:
		return fmt.Errorf("body: both json and base64 set")
	case w.JSON != nil:
		*b = Body{b: bytes.Clone(w.JSON)}
	case w.Base64 != nil:
		raw, err := base64.StdEncoding.DecodeString(*w.Base64)
		if err != nil {
			return fmt.Errorf("body: %w", err)
		}
		*b = NewBody(raw)
	default:
		return fmt.Errorf("body: neither json nor base64 set")
	}
	if len(b.b) > MaxBodyBytes {
		return fmt.Errorf("body: %d bytes exceeds %d", len(b.b), MaxBodyBytes)
	}
	return nil
}

// encoding/json compacts and HTML-escapes embedded RawMessage, so only bytes
// that are invariant under both can travel as inline JSON without changing.
func verbatimJSON(b []byte) bool {
	if !utf8.Valid(b) || !json.Valid(b) || bytes.ContainsAny(b, "<>&") ||
		bytes.Contains(b, []byte("\u2028")) || bytes.Contains(b, []byte("\u2029")) {
		return false
	}
	var c bytes.Buffer
	if err := json.Compact(&c, b); err != nil {
		return false
	}
	return bytes.Equal(c.Bytes(), b)
}
