//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// Headers that legitimately differ between a direct call and a gateway call:
// per-response values (Date), framing (Content-Length) and what the gateway
// adds (X-Request-Id).
var perResponse = map[string]bool{"Date": true, "Content-Length": true, "X-Request-Id": true}

func significant(h http.Header) http.Header {
	out := http.Header{}
	for k, v := range h {
		if !perResponse[k] {
			out[k] = v
		}
	}
	return out
}

func sameHeaders(a, b http.Header) (bool, string) {
	a, b = significant(a), significant(b)
	var diffs []string
	for k, v := range a {
		if !slices.Equal(v, b[k]) {
			diffs = append(diffs, k+": direct="+strings.Join(v, "|")+" gateway="+strings.Join(b[k], "|"))
		}
	}
	for k, v := range b {
		if _, ok := a[k]; !ok {
			diffs = append(diffs, k+": direct=<absent> gateway="+strings.Join(v, "|"))
		}
	}
	slices.Sort(diffs)
	return len(diffs) == 0, strings.Join(diffs, "; ")
}

// Criterion 1 on the demo orders service: everything except generated ids
// and timestamps must match a direct call, status, body and headers.
func TestC1_OrdersParity(t *testing.T) {
	spec := t.TempDir()
	ord := startOrders(t, spec)
	g := newGateway(t, spec, 10*time.Second, ord).startReady(t)
	startBridgeReady(t, newBridge(t, spec, ord, "b1"))
	a := newCaller(t, "checkout")

	idRe := regexp.MustCompile(`ord_[0-9A-Z]{26}`)
	tsRe := regexp.MustCompile(`"createdAt":"[^"]+"`)
	norm := func(b []byte) []byte {
		return tsRe.ReplaceAll(idRe.ReplaceAll(b, []byte("ord_X")), []byte(`"createdAt":"T"`))
	}
	normH := func(h http.Header) http.Header {
		h = h.Clone()
		for _, k := range []string{"Location", "Etag"} {
			for i, v := range h[k] {
				h[k][i] = idRe.ReplaceAllString(v, "ord_X")
			}
		}
		return h
	}
	compare := func(name string, r req, wantStatus int) []byte {
		t.Helper()
		d := a.direct(ord.url, r)
		v := a.via(g, ord.name, r)
		if d.err != nil || v.err != nil {
			t.Fatalf("%s: direct=%v gateway=%v", name, d.err, v.err)
		}
		if d.status != wantStatus || v.status != d.status {
			t.Errorf("%s: status direct=%d gateway=%d (want %d); gateway body %s", name, d.status, v.status, wantStatus, v.body)
		}
		if !bytes.Equal(norm(d.body), norm(v.body)) {
			t.Errorf("%s: body differs\n direct:  %s\n gateway: %s", name, d.body, v.body)
		}
		if ok, diff := sameHeaders(normH(d.header), normH(v.header)); !ok {
			t.Errorf("%s: headers differ: %s", name, diff)
		}
		if v.header.Get("X-Request-Id") == "" {
			t.Errorf("%s: gateway response without X-Request-Id", name)
		}
		t.Logf("%s: %d, %d body bytes, headers equal", name, v.status, len(v.body))
		return v.body
	}

	body := `{"customerId":"c-1","items":[{"sku":"A1","quantity":2}]}`
	created := compare("POST /orders 201", req{method: "POST", path: "/orders", body: body}, 201)
	compare("POST /orders 400 (validation)", req{method: "POST", path: "/orders", body: `{"customerId":"","items":[]}`}, 400)
	compare("POST /orders 400 (unknown field)", req{method: "POST", path: "/orders", body: `{"x":1}`}, 400)
	compare("POST /orders 415", req{method: "POST", path: "/orders", body: body, header: map[string]string{"Content-Type": "text/plain"}}, 415)
	compare("PUT unknown 404", req{method: "PUT", path: "/orders/nope", body: body}, 404)
	compare("DELETE unknown 404", req{method: "DELETE", path: "/orders/nope"}, 404)

	// Same resource reached both ways: create via gateway, read/replace/delete
	// both directly and through the gateway.
	var o struct{ ID string }
	_ = json.Unmarshal(created, &o)
	d := a.direct(ord.url, req{method: "GET", path: "/orders/" + o.ID})
	v := a.via(g, ord.name, req{method: "GET", path: "/orders/" + o.ID})
	if d.status != 200 || !bytes.Equal(d.body, v.body) {
		t.Errorf("GET created order: direct %s / gateway %s", d, v)
	}
	put := a.via(g, ord.name, req{method: "PUT", path: "/orders/" + o.ID, body: `{"customerId":"c-2","items":[{"sku":"B","quantity":1}]}`})
	if put.status != 200 || put.header.Get("Etag") != `"`+o.ID+`-2"` {
		t.Errorf("PUT via gateway: %s etag=%q", put, put.header.Get("Etag"))
	}
	del := a.via(g, ord.name, req{method: "DELETE", path: "/orders/" + o.ID})
	if del.status != 204 || len(del.body) != 0 {
		t.Errorf("DELETE via gateway: %s", del)
	}
	if d := a.direct(ord.url, req{method: "GET", path: "/orders/" + o.ID}); d.status != 404 {
		t.Errorf("order still present directly after DELETE via gateway: %s", d)
	}
}

// Criterion 1 with a B that reflects what it received: the request B sees
// through the gateway must be the request A sent, minus the caller's secret,
// plus the vouched identity; the response A sees must be B's, header for
// header, including multi-valued ones, every status class and odd bytes.
func TestC1_EchoParity(t *testing.T) {
	spec := t.TempDir()
	e := startEcho(t, spec)
	g := newGateway(t, spec, 10*time.Second, e).startReady(t)
	startBridgeReady(t, newBridge(t, spec, e, "b1"))
	a := newCaller(t, "checkout")

	cases := []struct {
		name string
		r    req
	}{
		{"POST json", req{method: "POST", path: "/echo/1?x=1&x=2&y=%2F", body: `{"a":1}`}},
		{"POST html chars + unicode", req{method: "POST", path: "/echo/1", body: `{"q":"<b>&amp;</b>","u":"é "}`}},
		{"POST non-compact json", req{method: "POST", path: "/echo/1", body: "{ \"a\" : 1 }\n"}},
		{"PUT encoded segment", req{method: "PUT", path: "/echo/a%2Fb%20c", body: `{}`}},
		{"PATCH 409", req{method: "PATCH", path: "/echo/1?status=409", body: `{"p":true}`}},
		{"DELETE 204", req{method: "DELETE", path: "/echo/1?status=204"}},
		{"POST 500", req{method: "POST", path: "/echo/1?status=500", body: `{}`}},
		{"POST 202 extra headers", req{method: "POST", path: "/echo/1?status=202", body: `{}`, header: map[string]string{
			"X-Custom": "v", "Accept": "application/json", "Accept-Language": "fr", "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			"Cookie": "session=SECRET-COOKIE", "X-Api-Key": "SECRET-APIKEY", "Proxy-Authorization": "Basic U0VDUkVU",
		}}},
	}
	for _, c := range cases {
		d := a.direct(e.url, c.r)
		v := a.via(g, e.name, c.r)
		if d.err != nil || v.err != nil {
			t.Fatalf("%s: direct=%v gateway=%v", c.name, d.err, v.err)
		}
		if d.status != v.status {
			t.Errorf("%s: status direct=%d gateway=%d body=%s", c.name, d.status, v.status, v.body)
			continue
		}
		if ok, diff := sameHeaders(d.header, v.header); !ok {
			t.Errorf("%s: response headers differ: %s", c.name, diff)
		}
		if d.status == 204 {
			if len(v.body) != 0 {
				t.Errorf("%s: 204 with body via gateway", c.name)
			}
			continue
		}
		var de, ve echoed
		if err := json.Unmarshal(d.body, &de); err != nil {
			t.Fatalf("%s: direct body %s", c.name, d.body)
		}
		if err := json.Unmarshal(v.body, &ve); err != nil {
			t.Fatalf("%s: gateway body %s", c.name, v.body)
		}
		if de.Method != ve.Method || de.EscapedPath != ve.EscapedPath || de.RawQuery != ve.RawQuery || !bytes.Equal(de.Body, ve.Body) || de.ContentLength != ve.ContentLength {
			t.Errorf("%s: B saw a different request\n direct:  %s %s ?%s len=%d %q\n gateway: %s %s ?%s len=%d %q",
				c.name, de.Method, de.EscapedPath, de.RawQuery, de.ContentLength, de.Body, ve.Method, ve.EscapedPath, ve.RawQuery, ve.ContentLength, ve.Body)
		}
		// Request headers: B must see A's headers, minus caller secrets,
		// plus identity. Anything else is a transparency defect.
		secrets := map[string]bool{"Authorization": true, "Cookie": true, "X-Api-Key": true, "Proxy-Authorization": true}
		added := map[string]bool{"X-Caller-Application": true, "X-Caller-Instance": true, "X-Request-Id": true}
		for k, dv := range de.Header {
			if secrets[k] {
				if _, leaked := ve.Header[k]; leaked {
					t.Errorf("%s: caller secret %s reached B through the gateway", c.name, k)
				}
				continue
			}
			if !slices.Equal(dv, ve.Header[k]) {
				t.Errorf("%s: B saw header %s=%q via gateway, %q direct", c.name, k, ve.Header[k], dv)
			}
		}
		for k, vv := range ve.Header {
			if _, ok := de.Header[k]; !ok && !added[k] {
				t.Errorf("%s: B saw extra header %s=%q via gateway", c.name, k, vv)
			}
		}
		if got := ve.Header["X-Caller-Application"]; !slices.Equal(got, []string{"checkout"}) {
			t.Errorf("%s: B saw caller application %q", c.name, got)
		}
		t.Logf("%s: %d, request and response equivalent", c.name, v.status)
	}
}
