//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// echo is a B that tells A exactly what it received, and answers with the
// status and extra headers A asks for. It is a plain net/http app: no Kafka,
// no proprietary header needed (D2).
const echoSpec = `openapi: 3.0.3
info: { title: Echo, version: 1.0.0 }
paths:
  /echo/{id}:
    parameters:
      - { name: id, in: path, required: true, schema: { type: string } }
    get:    { operationId: getEcho,    responses: { '200': { description: ok } } }
    post:   { operationId: postEcho,   responses: { '200': { description: ok } } }
    put:    { operationId: putEcho,    responses: { '200': { description: ok } } }
    patch:  { operationId: patchEcho,  responses: { '200': { description: ok } } }
    delete: { operationId: deleteEcho, responses: { '200': { description: ok } } }
`

// echoSecuredSpec is echoSpec whose contract declares where B expects
// credentials (D12): an X-Auth-Token header and an access_token query param.
const echoSecuredSpec = echoSpec + `components:
  securitySchemes:
    token: { type: apiKey, in: header, name: X-Auth-Token }
    qtoken: { type: apiKey, in: query, name: access_token }
`

type echoed struct {
	Method        string              `json:"method"`
	EscapedPath   string              `json:"escapedPath"`
	RawQuery      string              `json:"rawQuery"`
	Header        map[string][]string `json:"header"`
	ContentLength int64               `json:"contentLength"`
	Body          []byte              `json:"body"`
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	e := echoed{Method: r.Method, EscapedPath: r.URL.EscapedPath(), RawQuery: r.URL.RawQuery, Header: r.Header, ContentLength: r.ContentLength, Body: b}
	status := http.StatusOK
	if s := r.URL.Query().Get("status"); s != "" {
		status, _ = strconv.Atoi(s)
	}
	h := w.Header()
	h.Add("Set-Cookie", "a=1; Path=/")
	h.Add("Set-Cookie", "b=2; Path=/")
	h.Set("Cache-Control", "no-store")
	h.Add("X-Multi", "one")
	h.Add("X-Multi", "two")
	h.Set("Content-Type", "application/json; charset=utf-8")
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return
	}
	out, _ := json.Marshal(e)
	w.WriteHeader(status)
	_, _ = w.Write(out)
}

func startEcho(t *testing.T, specDir string) *svc {
	return startEchoSpec(t, specDir, "echo")
}

func startEchoSpec(t *testing.T, specDir, kind string) *svc {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(echoHandler))
	t.Cleanup(srv.Close)
	name := randName("echo")
	writeSpec(t, specDir, name, kind)
	return &svc{name: name, kind: "echo", url: srv.URL}
}
