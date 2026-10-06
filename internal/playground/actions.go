package playground

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sderosiaux/http-over-kafka/internal/devidp"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
)

// Actions are fixed: the browser picks one by name and never supplies a
// method, path, header or body. The playground holds a token-minting key,
// so it must not become an open proxy to the gateway.
const (
	ActionCreateOrder  = "create-order"
	ActionInvalidOrder = "invalid-order"
	ActionCharge       = "charge"
	ActionRetry        = "retry"
)

// Exchange is one HTTP call to the gateway, as the page shows it.
type Exchange struct {
	ID        string         `json:"id"`
	Action    string         `json:"action"`
	RetryOf   string         `json:"retryOf,omitempty"` // exchange replayed by a retry
	Request   ShownRequest   `json:"request"`
	Response  *ShownResponse `json:"response,omitempty"`
	Error     *Failure       `json:"error,omitempty"`
	RequestID string         `json:"requestId,omitempty"` // the gateway's X-Request-Id
	// Curl reproduces the call from a terminal; it gets its token from
	// this playground's GET /token.
	Curl      string `json:"curl"`
	Retryable bool   `json:"retryable"` // carries an Idempotency-Key
	Check     *Check `json:"check,omitempty"`
}

type ShownRequest struct {
	Method  string   `json:"method"`
	URL     string   `json:"url"`
	Headers []Header `json:"headers"`
	Body    string   `json:"body"`
}

type ShownResponse struct {
	Status     int      `json:"status"`
	StatusText string   `json:"statusText"`
	Headers    []Header `json:"headers"`
	Body       string   `json:"body"`
	DurationMs float64  `json:"durationMs"`
}

type Failure struct {
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Hint   string `json:"hint"`
}

// Check is the service's own view after the call, read with a GET through
// the gateway. GETs are passed straight to the service and write nothing to
// Kafka (D8), so the check does not appear in the feed.
type Check struct {
	Request string `json:"request"`
	Status  int    `json:"status,omitempty"`
	Body    string `json:"body,omitempty"`
	Summary string `json:"summary,omitempty"`
	Error   string `json:"error,omitempty"`
}

// call is a request the playground can send, and send again on retry.
type call struct {
	action  string
	service string // routed by Host
	method  string
	path    string
	body    []byte
	idemKey string
	check   func(context.Context) *Check
}

const maxStored = 200

func (s *Server) remember(id string, c call) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[id] = c
	s.order = append(s.order, id)
	if len(s.order) > maxStored {
		delete(s.calls, s.order[0])
		s.order = slices.Delete(s.order, 0, 1)
	}
}

func (s *Server) recall(id string) (call, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.calls[id]
	return c, ok
}

func (s *Server) handleAction(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r) {
		http.Error(w, "cross-origin request refused", http.StatusForbidden)
		return
	}
	var c call
	var retryOf string
	switch name := r.PathValue("name"); name {
	case ActionCreateOrder:
		c = s.createOrder()
	case ActionInvalidOrder:
		c = s.invalidOrder()
	case ActionCharge:
		c = s.charge()
	case ActionRetry:
		var in struct {
			Of string `json:"of"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&in); err != nil || in.Of == "" {
			writeJSON(w, http.StatusBadRequest, Failure{Title: "Nothing to retry", Detail: "the retry names no earlier request", Hint: "Create an order or charge a card first."})
			return
		}
		prev, ok := s.recall(in.Of)
		if !ok || prev.idemKey == "" {
			writeJSON(w, http.StatusNotFound, Failure{Title: "Nothing to retry",
				Detail: "the playground no longer knows that request (it keeps the last " + fmt.Sprint(maxStored) + ", and forgets them on restart)",
				Hint:   "Create an order or charge a card, then retry it."})
			return
		}
		c, retryOf = prev, in.Of
		c.action = ActionRetry
	default:
		http.NotFound(w, r)
		return
	}
	// The token URL in the curl command is this page's own origin, as the
	// visitor's browser reached it.
	ex := s.send(r.Context(), c, "http://"+r.Host+"/token")
	ex.RetryOf = retryOf
	if c.idemKey != "" && retryOf == "" {
		s.remember(ex.ID, c)
	}
	writeJSON(w, http.StatusOK, ex)
}

func (s *Server) createOrder() call {
	customer := "cust-" + randHex(3)
	body, _ := json.Marshal(map[string]any{"customerId": customer, "items": []map[string]any{{"sku": "A123", "quantity": 2}}})
	return call{
		action: ActionCreateOrder, service: s.cfg.Orders.Name(), method: http.MethodPost, path: "/orders",
		body: body, idemKey: newIdemKey(),
		check: func(ctx context.Context) *Check { return s.countOrders(ctx, customer) },
	}
}

// invalidOrder breaks the service's own rule (items must not be empty), so
// the 400 comes from the service, not from the gateway.
func (s *Server) invalidOrder() call {
	body, _ := json.Marshal(map[string]any{"customerId": "cust-" + randHex(3), "items": []any{}})
	return call{action: ActionInvalidOrder, service: s.cfg.Orders.Name(), method: http.MethodPost, path: "/orders", body: body}
}

func (s *Server) charge() call {
	account := "acct-" + randHex(3)
	body, _ := json.Marshal(map[string]any{"accountId": account, "amountCents": 500, "currency": "EUR"})
	return call{
		action: ActionCharge, service: s.cfg.Payments.Name(), method: http.MethodPost, path: "/charges",
		body: body, idemKey: newIdemKey(),
		check: func(ctx context.Context) *Check { return s.readAccount(ctx, account) },
	}
}

func (s *Server) send(ctx context.Context, c call, tokenURL string) Exchange {
	ex := Exchange{ID: ulid.Make().String(), Action: c.action, Retryable: c.idemKey != ""}
	token, err := s.token()
	if err != nil {
		ex.Error = &Failure{Title: "Could not mint a caller token", Detail: err.Error(), Hint: "Check HOK_DEV_IDP_KEY, HOK_JWT_ISSUER and HOK_JWT_AUDIENCE (deploy/dev-idp.env)."}
		return ex
	}
	headers := []Header{
		{"Host", c.service},
		{"Authorization", "Bearer " + token},
		{"Content-Type", "application/json"},
	}
	if c.idemKey != "" {
		headers = append(headers, Header{wire.IdempotencyKeyHeader, c.idemKey})
	}
	ex.Request = ShownRequest{Method: c.method, URL: s.public(c.path), Headers: shortenToken(headers, token), Body: string(c.body)}
	ex.Curl = curl(tokenURL, s.cfg.Application, c.method, s.public(c.path), headers, c.body)

	req, err := http.NewRequestWithContext(ctx, c.method, s.cfg.Gateway.JoinPath(c.path).String(), bytes.NewReader(c.body))
	if err != nil {
		ex.Error = &Failure{Title: "Invalid gateway URL", Detail: err.Error(), Hint: "Check HOK_PLAYGROUND_GATEWAY."}
		return ex
	}
	for _, h := range headers {
		if h.Key == "Host" {
			req.Host = h.Value
			continue
		}
		req.Header.Set(h.Key, h.Value)
	}
	start := time.Now()
	resp, err := s.client.Do(req)
	if err != nil {
		ex.Error = s.unreachable(err)
		return ex
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, wire.MaxBodyBytes))
	took := time.Since(start)
	if err != nil {
		ex.Error = &Failure{Title: "The gateway's response was cut off", Detail: err.Error(), Hint: "docker compose logs gateway"}
		return ex
	}
	ex.RequestID = resp.Header.Get(wire.RequestIDHeader)
	ex.Response = &ShownResponse{
		Status: resp.StatusCode, StatusText: http.StatusText(resp.StatusCode),
		Headers: sortedHeaders(resp.Header), Body: string(body),
		DurationMs: float64(took.Microseconds()) / 1000,
	}
	if c.check != nil && resp.StatusCode < 500 {
		ex.Check = c.check(ctx)
	}
	return ex
}

func (s *Server) unreachable(err error) *Failure {
	f := &Failure{Title: "The gateway did not answer", Detail: err.Error(),
		Hint: "The playground calls the gateway at " + s.cfg.Gateway.String() + ". Check that it runs: docker compose ps gateway, then docker compose logs gateway."}
	if errors.Is(err, context.DeadlineExceeded) {
		f.Title = "The gateway did not answer in time"
	}
	return f
}

func (s *Server) token() (string, error) {
	return devidp.Token{
		KeyID: s.cfg.IdP.KeyID, Key: s.cfg.IdP.Key, Issuer: s.cfg.IdP.Issuer, Audience: s.cfg.IdP.Audience,
		Application: s.cfg.Application, Instance: s.cfg.Instance,
		IssuedAt: time.Now(), TTL: time.Hour,
	}.Sign()
}

// get reads from the service through the gateway (GET passthrough).
func (s *Server) get(ctx context.Context, service, path string) (*Check, []byte) {
	ck := &Check{Request: "GET " + path + " (Host: " + service + ")"}
	token, err := s.token()
	if err != nil {
		ck.Error = err.Error()
		return ck, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.Gateway.JoinPath(path).String(), nil)
	if err != nil {
		ck.Error = err.Error()
		return ck, nil
	}
	req.Host = service
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := s.client.Do(req)
	if err != nil {
		ck.Error = err.Error()
		return ck, nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, wire.MaxBodyBytes))
	if err != nil {
		ck.Error = err.Error()
		return ck, nil
	}
	ck.Status = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		ck.Body, ck.Error = string(body), "unexpected status "+resp.Status
		return ck, nil
	}
	return ck, body
}

func (s *Server) countOrders(ctx context.Context, customer string) *Check {
	ck, body := s.get(ctx, s.cfg.Orders.Name(), "/orders")
	if body == nil {
		return ck
	}
	var all []struct {
		ID         string `json:"id"`
		CustomerID string `json:"customerId"`
	}
	if err := json.Unmarshal(body, &all); err != nil {
		ck.Error = "unexpected body: " + err.Error()
		return ck
	}
	var mine []string
	for _, o := range all {
		if o.CustomerID == customer {
			mine = append(mine, o.ID)
		}
	}
	out, _ := json.Marshal(map[string]any{"customerId": customer, "orders": mine})
	ck.Body = string(out)
	ck.Summary = plural(len(mine), "order") + " for " + customer + " in the orders service"
	return ck
}

func (s *Server) readAccount(ctx context.Context, account string) *Check {
	ck, body := s.get(ctx, s.cfg.Payments.Name(), "/accounts/"+url.PathEscape(account))
	if body == nil {
		return ck
	}
	ck.Body = string(body)
	var a struct {
		DebitCount   int   `json:"debitCount"`
		DebitedCents int64 `json:"debitedCents"`
	}
	if err := json.Unmarshal(body, &a); err != nil {
		ck.Error = "unexpected body: " + err.Error()
		return ck
	}
	ck.Summary = fmt.Sprintf("%s debited %s, %d cents in total", account, plural(a.DebitCount, "time"), a.DebitedCents)
	return ck
}

func (s *Server) public(path string) string {
	return strings.TrimSuffix(s.cfg.PublicGateway, "/") + path
}

func curl(tokenURL, app, method, target string, headers []Header, body []byte) string {
	var b strings.Builder
	fmt.Fprintf(&b, "TOKEN=$(curl -s %s)  # dev identity provider: caller %s, valid 1 h\n", shellQuote(tokenURL), app)
	fmt.Fprintf(&b, "curl -i -X %s %s", method, shellQuote(target))
	for _, h := range headers {
		if h.Key == "Authorization" {
			b.WriteString(" \\\n  -H \"Authorization: Bearer $TOKEN\"")
			continue
		}
		fmt.Fprintf(&b, " \\\n  -H %s", shellQuote(h.Key+": "+h.Value))
	}
	if len(body) > 0 {
		fmt.Fprintf(&b, " \\\n  -d %s", shellQuote(string(body)))
	}
	return b.String()
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

func abbreviate(token string) string {
	if len(token) <= 24 {
		return token
	}
	return token[:16] + "…" + token[len(token)-6:]
}

func shortenToken(hs []Header, token string) []Header {
	out := slices.Clone(hs)
	for i, h := range out {
		if h.Key == "Authorization" {
			out[i].Value = "Bearer " + abbreviate(token)
		}
	}
	return out
}

func sortedHeaders(h http.Header) []Header {
	var out []Header
	for name, values := range h {
		for _, v := range values {
			out = append(out, Header{name, v})
		}
	}
	slices.SortStableFunc(out, func(a, b Header) int { return strings.Compare(a.Key, b.Key) })
	return out
}

// sameOrigin refuses POSTs a third-party page would make on the visitor's
// behalf. Browsers send Origin on every cross-origin POST.
func sameOrigin(r *http.Request) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return r.Header.Get("Sec-Fetch-Site") != "cross-site"
	}
	u, err := url.Parse(o)
	return err == nil && u.Host == r.Host
}

func newIdemKey() string { return "pg-" + strings.ToLower(ulid.Make().String()) }

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}
