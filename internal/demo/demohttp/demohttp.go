// Package demohttp is plumbing shared by the demo services. Like them, it must
// never import Kafka or the internal wire contract (D2).
package demohttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
)

const maxBody = 1 << 20

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func Problem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"title":  http.StatusText(status),
		"status": status,
		"detail": detail,
	})
}

// DecodeJSON reads a JSON body strictly and writes the error response itself;
// callers return when ok is false.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) (ok bool) {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		Problem(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			Problem(w, http.StatusRequestEntityTooLarge, err.Error())
			return false
		}
		Problem(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return false
	}
	if dec.More() {
		Problem(w, http.StatusBadRequest, "trailing data after JSON body")
		return false
	}
	return true
}
