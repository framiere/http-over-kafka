//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

type auditLine struct {
	Kind, RequestID, Service, OperationID, Method, Path, Authenticity, Outcome, Fault string
	Status                                                                            int
	Caller                                                                            *struct{ Application, Instance string }
}

// auditReader parses the auditor's stdout incrementally, keeping only the
// lines of the given services: the e2e broker holds every earlier test's
// history, which a fresh audit group replays in full.
type auditReader struct {
	p        *proc
	pos      int
	services map[string]bool
	lines    []auditLine
}

func (r *auditReader) read() []auditLine {
	out := r.p.logs()
	end := strings.LastIndexByte(out, '\n') + 1
	if end <= r.pos {
		return r.lines
	}
	chunk := out[r.pos:end]
	r.pos = end
	for _, line := range strings.Split(chunk, "\n") {
		if !strings.HasPrefix(line, `{"kind"`) {
			continue
		}
		hit := false
		for s := range r.services {
			if strings.Contains(line, `"service":"`+s+`"`) {
				hit = true
				break
			}
		}
		if !hit {
			continue
		}
		var l auditLine
		if json.Unmarshal([]byte(line), &l) == nil {
			r.lines = append(r.lines, l)
		}
	}
	return r.lines
}

// Criterion 7: an independent consumer, plugged in after the fact, reads who
// did what with which outcome, with A and B untouched.
func TestC7_AuditReadsTheMutationStream(t *testing.T) {
	spec := t.TempDir()
	ord := startOrders(t, spec)
	pay := startPayments(t, spec, 0)
	g := newGateway(t, spec, 10*time.Second, ord, pay).startReady(t)
	startBridgeReady(t, newBridge(t, spec, ord, "bo"))
	bp := newBridge(t, spec, pay, "bp")
	startBridgeReady(t, bp)
	a := newCaller(t, "checkout")

	want := map[string]string{} // requestId -> expected outcome
	r := a.via(g, ord.name, req{method: "POST", path: "/orders", body: orderBody("c1", 1)})
	want[r.header.Get("X-Request-Id")] = "Succeeded"
	loc := r.header.Get("Location")
	r = a.via(g, ord.name, req{method: "PUT", path: loc, body: orderBody("c1", 2)})
	want[r.header.Get("X-Request-Id")] = "Succeeded"
	r = a.via(g, ord.name, req{method: "POST", path: "/orders", body: `{"bad":1}`})
	want[r.header.Get("X-Request-Id")] = "Failed"
	r = a.via(g, ord.name, req{method: "DELETE", path: loc})
	want[r.header.Get("X-Request-Id")] = "Succeeded"
	r = a.via(g, pay.name, req{method: "POST", path: "/charges", body: charge("acct-audit"), header: map[string]string{"Cookie": "SECRET-COOKIE"}})
	want[r.header.Get("X-Request-Id")] = "Succeeded"
	getID := a.via(g, ord.name, req{method: "GET", path: "/orders"}).header.Get("X-Request-Id")

	// A forged command (attacker rewrote the caller): audit must flag it.
	var genuine = readTopic(t, "http.requests."+pay.name, true)[0]
	forged := clone(genuine)
	forged.Value = bytes.Replace(forged.Value, []byte(`"application":"checkout"`), []byte(`"application":"admin"`), 1)
	produce(t, forged)

	// The auditor starts last, with a fresh group: it replays history.
	au := newProc(t, "audit", "audit", map[string]string{
		"KAFKA_BROKERS": brokers, "HOK_AUDIT_GROUP": randName("audit-"), "HOK_TRUSTED_GATEWAY_KEYS": devEnv["HOK_TRUSTED_GATEWAY_KEYS"],
		"HOK_TRUSTED_BRIDGE_KEYS": devEnv["HOK_TRUSTED_BRIDGE_KEYS"],
	}).start()
	ar := &auditReader{p: au, services: map[string]bool{ord.name: true, pay.name: true}}
	eventually(t, 5*time.Minute, "audit saw every request and completion", func() bool {
		seen := map[string]map[string]bool{}
		for _, l := range ar.read() {
			if seen[l.RequestID] == nil {
				seen[l.RequestID] = map[string]bool{}
			}
			seen[l.RequestID][l.Kind] = true
		}
		for id := range want {
			if !seen[id]["requested"] || !seen[id]["completed"] {
				return false
			}
		}
		return true
	})
	var forgedFlagged bool
	ours := map[string]bool{ord.name: true, pay.name: true}
	for _, l := range ar.read() {
		if !ours[l.Service] {
			continue // the auditor reads every service, including other tests' forgeries
		}
		if exp, ok := want[l.RequestID]; ok && l.Kind == "completed" && l.Outcome != exp {
			t.Errorf("audit outcome for %s %s: %s, want %s", l.Method, l.Path, l.Outcome, exp)
		}
		if l.RequestID == getID {
			t.Errorf("a GET appears in the audit stream")
		}
		if l.Kind == "requested" && l.Caller != nil && l.Caller.Application == "admin" {
			forgedFlagged = l.Authenticity == "not-authentic"
			t.Logf("forged command audit line: identity=%s", l.Authenticity)
		}
		if l.Kind == "completed" && l.Authenticity != "authentic" {
			t.Errorf("genuine Result not marked authentic: %+v", l)
		}
		if l.Kind == "requested" && l.Caller != nil && l.Caller.Application == "checkout" && l.Authenticity != "authentic" {
			t.Errorf("genuine command not marked signed: %+v", l)
		}
	}
	if !forgedFlagged {
		t.Errorf("forged command not flagged not-authentic")
	}
	if strings.Contains(au.logs(), "SECRET-COOKIE") || strings.Contains(au.logs(), a.token) {
		t.Errorf("audit output carries a caller secret")
	}
	t.Logf("%d audit lines for this test's services; %d mutations each requested+completed with the right outcome; GET absent", len(ar.read()), len(want))
}
