package demo_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/demo/orders"
	"github.com/sderosiaux/http-over-kafka/internal/demo/payments"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
)

func do(t *testing.T, method, url, contentType, body string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestOrdersCRUD(t *testing.T) {
	svc := orders.New()
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()

	resp, body := do(t, "POST", srv.URL+"/orders", "application/json", `{"customerId":"c1","items":[{"sku":"A","quantity":2}]}`)
	if resp.StatusCode != 201 || resp.Header.Get("Location") == "" || resp.Header.Get("ETag") == "" {
		t.Fatalf("create: %d %v %s", resp.StatusCode, resp.Header, body)
	}
	var o orders.Order
	if err := json.Unmarshal([]byte(body), &o); err != nil || !strings.HasPrefix(o.ID, "ord_") {
		t.Fatalf("%v %s", err, body)
	}
	if resp.Header.Get("Location") != "/orders/"+o.ID {
		t.Fatalf("location %s", resp.Header.Get("Location"))
	}

	if resp, _ := do(t, "PUT", srv.URL+"/orders/"+o.ID, "application/json", `{"customerId":"c1","items":[{"sku":"B","quantity":1}]}`); resp.StatusCode != 200 {
		t.Fatalf("replace: %d", resp.StatusCode)
	}
	if resp, body := do(t, "GET", srv.URL+"/orders/"+o.ID, "", ""); resp.StatusCode != 200 || !strings.Contains(body, `"sku":"B"`) || resp.Header.Get("ETag") != `"`+o.ID+`-2"` {
		t.Fatalf("get: %d %s %s", resp.StatusCode, body, resp.Header.Get("ETag"))
	}
	if resp, _ := do(t, "DELETE", srv.URL+"/orders/"+o.ID, "", ""); resp.StatusCode != 204 {
		t.Fatalf("delete: %d", resp.StatusCode)
	}
	if resp, _ := do(t, "DELETE", srv.URL+"/orders/"+o.ID, "", ""); resp.StatusCode != 404 {
		t.Fatalf("second delete: %d", resp.StatusCode)
	}

	for name, c := range map[string]struct {
		ct, body string
		status   int
	}{
		"empty body":    {"application/json", "", 400},
		"not json":      {"application/json", "nope", 400},
		"no items":      {"application/json", `{"customerId":"c1","items":[]}`, 400},
		"unknown field": {"application/json", `{"customerId":"c1","items":[{"sku":"A","quantity":1}],"x":1}`, 400},
		"wrong type":    {"text/plain", `{"customerId":"c1"}`, 415},
		"no type":       {"", `{"customerId":"c1"}`, 415},
		"charset is ok": {"application/json; charset=utf-8", `{"customerId":"c1","items":[{"sku":"A","quantity":1}]}`, 201},
	} {
		resp, body := do(t, "POST", srv.URL+"/orders", c.ct, c.body)
		if resp.StatusCode != c.status {
			t.Errorf("%s: %d %s", name, resp.StatusCode, body)
		}
		if c.status >= 400 && resp.Header.Get("Content-Type") != "application/problem+json" {
			t.Errorf("%s: errors are problem+json", name)
		}
	}
	if svc.Creates() != 2 {
		t.Fatalf("creates=%d", svc.Creates())
	}
}

// The debit happens before the response; a caller that gives up still leaves
// the debit applied. This is the double-charge trap the bridge must avoid.
func TestPaymentsDebitSurvivesCallerTimeout(t *testing.T) {
	svc := payments.New()
	srv := httptest.NewServer(svc.Handler())
	defer srv.Close()
	svc.SetDelay(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/charges", strings.NewReader(`{"accountId":"acc-1","amountCents":500,"currency":"EUR"}`))
	req.Header.Set("Content-Type", "application/json")
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("expected client timeout")
	}
	kafkatest.Eventually(t, 2*time.Second, func() bool { return svc.Account("acc-1").DebitCount == 1 }, "debit applied despite timeout")

	svc.SetDelay(0)
	resp, body := do(t, "POST", srv.URL+"/charges", "application/json", `{"accountId":"acc-1","amountCents":500,"currency":"EUR"}`)
	if resp.StatusCode != 201 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	resp, body = do(t, "GET", srv.URL+"/accounts/acc-1", "", "")
	var a payments.Account
	_ = json.Unmarshal([]byte(body), &a)
	if resp.StatusCode != 200 || a.DebitCount != 2 || a.DebitedCents != 1000 {
		t.Fatalf("no idempotency in B: two calls, two debits; got %+v", a)
	}
	if resp, _ := do(t, "POST", srv.URL+"/charges", "application/json", `{"accountId":"acc-1","amountCents":0,"currency":"EUR"}`); resp.StatusCode != 400 {
		t.Fatalf("invalid amount: %d", resp.StatusCode)
	}
}

// D2: the demo services are ordinary HTTP apps. Their binaries must not link
// any Kafka client or the internal contract.
func TestDemoServicesKnowNothingOfTheSystem(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "../../cmd/orders", "../../cmd/payments").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	const module = "github.com/sderosiaux/http-over-kafka/"
	for _, dep := range strings.Fields(string(out)) {
		rel, ours := strings.CutPrefix(dep, module)
		bad := (ours && !strings.HasPrefix(rel, "internal/demo/") && !strings.HasPrefix(rel, "cmd/")) ||
			(!ours && (strings.Contains(dep, "kafka") || strings.Contains(dep, "franz-go")))
		if bad {
			t.Errorf("demo service depends on %s", dep)
		}
	}
}
