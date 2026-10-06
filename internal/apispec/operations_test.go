package apispec_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/sderosiaux/http-over-kafka/internal/apispec"
)

func TestOperationsIncludesRoutesWithoutIDs(t *testing.T) {
	svc := mustLoad(t, "svc", []byte(header+`
  /a:
    get: {responses: {'200': {description: ok}}}
    head: {responses: {'200': {description: ok}}}
    options: {responses: {'204': {description: ok}}}
    post: {operationId: createA, responses: {'201': {description: ok}}}
  /b:
    get: {responses: {'200': {description: ok}}}
`))
	var got []string
	for _, op := range svc.Operations() {
		got = append(got, op.PathTemplate+" "+op.Method)
		match, err := svc.Resolve(op.Method, op.PathTemplate)
		if err != nil || match.Operation != op {
			t.Fatalf("listed operation differs from routing: %+v, %v", op, err)
		}
	}
	want := []string{"/a GET", "/a HEAD", "/a OPTIONS", "/a POST", "/b GET"}
	if !slices.Equal(got, want) {
		t.Fatalf("operations %v, want %v", got, want)
	}
	if _, ok := svc.Operation(""); ok {
		t.Fatal("unnamed operations must not share an artificial lookup ID")
	}
	if op, ok := svc.Operation("createA"); !ok || op.Method != "POST" {
		t.Fatal("named lookup lost")
	}
	// Sorting or replacing the returned slice must not mutate the routing table.
	ops := svc.Operations()
	ops[0] = nil
	if svc.Operations()[0] == nil {
		t.Fatal("Operations exposes its backing slice")
	}
}

func TestCredentialsCheckUnnamedOperations(t *testing.T) {
	const spec = `openapi: 3.0.3
info: {title: secure, version: '1'}
components:
  securitySchemes:
    key: {type: apiKey, in: header, name: X-Service-Key}
security: [{key: []}]
paths:
  /private:
    get: {responses: {'200': {description: ok}}}
  /public:
    get: {security: [], responses: {'200': {description: ok}}}
`
	svc := mustLoad(t, "svc", []byte(spec))
	reads := func(op *apispec.Operation) bool { return !op.Transported() }
	if err := (apispec.Credentials{}).Check(svc, reads); err == nil {
		t.Fatal("missing credential for unnamed GET accepted")
	}
	if err := (apispec.Credentials{"key": "provider-key"}).Check(svc, reads); err != nil {
		t.Fatal(err)
	}
	public, err := svc.Resolve("GET", "/public")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := (apispec.Credentials{}).Alternative(public.Operation); !ok {
		t.Fatal("anonymous operation override lost")
	}
}

// Enumeration must not broaden a component's credential needs: each side
// checks only the operations it executes, preserving AND and OR semantics.
func TestUnnamedCredentialAlternativesAndRoleBoundaries(t *testing.T) {
	const prefix = `openapi: 3.0.3
info: {title: secure, version: '1'}
components:
  securitySchemes:
    read: {type: apiKey, in: header, name: X-Read-Key}
    second: {type: apiKey, in: query, name: second_key}
    write: {type: http, scheme: bearer}
paths:
  /private:
    get:
      security: SECURITY
      responses: {'200': {description: ok}}
    post:
      operationId: writePrivate
      security: [{write: []}]
      responses: {'201': {description: ok}}
`
	for _, tc := range []struct {
		name, requirement string
		creds             apispec.Credentials
		wantOK            bool
	}{
		{"missing", "[{read: []}]", nil, false},
		{"AND partial", "[{read: [], second: []}]", apispec.Credentials{"read": "read-value"}, false},
		{"AND complete", "[{read: [], second: []}]", apispec.Credentials{"read": "read-value", "second": "second-value"}, true},
		{"OR second", "[{read: []}, {second: []}]", apispec.Credentials{"second": "second-value"}, true},
		{"anonymous OR", "[{read: []}, {}]", nil, true},
		{"security disabled", "[]", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := mustLoad(t, "svc", []byte(strings.Replace(prefix, "SECURITY", tc.requirement, 1)))
			reads := func(op *apispec.Operation) bool { return !op.Transported() }
			writes := func(op *apispec.Operation) bool { return op.Transported() }
			err := tc.creds.Check(svc, reads)
			if (err == nil) != tc.wantOK {
				t.Fatalf("read credential check: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), "GET /private") {
				t.Fatalf("missing route diagnostic: %v", err)
			}
			if err := (apispec.Credentials{"write": "write-value"}).Check(svc, writes); err != nil {
				t.Fatalf("bridge required an unused read credential: %v", err)
			}
			if err := tc.creds.Check(svc, writes); err == nil {
				t.Fatal("bridge accepted missing write credential")
			}
		})
	}
}
