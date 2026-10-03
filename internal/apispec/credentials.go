package apispec

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Credentials are the provider side's own credentials for one service, by
// securityScheme name (D12). A's secret never reaches B: when B's contract
// requires a credential, whoever calls B (gateway for passthrough, bridge for
// mutations) injects this one. Values are secrets: never log them.
type Credentials map[string]string

// ParseCredentials reads "service.scheme=value,..." and keeps the entries of
// service. Values may not contain commas.
func ParseCredentials(service, s string) (Credentials, error) {
	out := Credentials{}
	for entry := range strings.SplitSeq(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		name, value, ok := strings.Cut(entry, "=")
		svc, scheme, ok2 := strings.Cut(name, ".")
		if !ok || !ok2 || svc == "" || scheme == "" || value == "" {
			return nil, fmt.Errorf("credential entry %q: want service.scheme=value", name)
		}
		if svc == service {
			out[scheme] = value
		}
	}
	return out, nil
}

// Alternative picks the first alternative of op's security requirement that
// c satisfies. ok is false when none is.
func (c Credentials) Alternative(op *Operation) (alt []string, ok bool) {
	if op.Security == nil {
		return nil, true
	}
	for _, a := range op.Security {
		all := true
		for _, name := range a {
			if _, has := c[name]; !has {
				all = false
			}
		}
		if all {
			return a, true
		}
	}
	return nil, false
}

// Check fails at startup, rather than with a 401 per request, when a
// credential names an undeclared or non-injectable scheme, or an operation
// selected by include cannot be satisfied.
func (c Credentials) Check(s *Service, include func(*Operation) bool) error {
	for name := range c {
		sc, ok := s.SecurityScheme(name)
		if !ok {
			return fmt.Errorf("service %q: credential for undeclared securityScheme %q", s.Name(), name)
		}
		if sc.Type == "mutualTLS" {
			return fmt.Errorf("service %q: securityScheme %q is mutualTLS, not injectable", s.Name(), name)
		}
	}
	for _, op := range s.Operations() {
		if !include(op) {
			continue
		}
		if _, ok := c.Alternative(op); !ok {
			return fmt.Errorf("service %q: %s %s requires one of %v; configure an upstream credential",
				s.Name(), op.Method, op.PathTemplate, op.Security)
		}
	}
	return nil
}

// Inject sets the credentials of alt on h and returns rawQuery with apiKey
// query parameters appended.
func (c Credentials) Inject(s *Service, alt []string, h http.Header, rawQuery string) string {
	var cookies []string
	for _, name := range alt {
		sc, _ := s.SecurityScheme(name)
		v := c[name]
		switch {
		case sc.Type == "apiKey" && sc.In == "header":
			h.Set(sc.Param, v)
		case sc.Type == "apiKey" && sc.In == "query":
			p := url.QueryEscape(sc.Param) + "=" + url.QueryEscape(v)
			if rawQuery == "" {
				rawQuery = p
			} else {
				rawQuery += "&" + p
			}
		case sc.Type == "apiKey" && sc.In == "cookie":
			cookies = append(cookies, sc.Param+"="+v)
		case sc.Type == "http" && strings.EqualFold(sc.Scheme, "bearer"):
			h.Set("Authorization", "Bearer "+v)
		case sc.Type == "http" && strings.EqualFold(sc.Scheme, "basic"):
			h.Set("Authorization", "Basic "+v)
		case sc.Type == "http":
			h.Set("Authorization", sc.Scheme+" "+v)
		default: // oauth2, openIdConnect: a service token, sent as bearer
			h.Set("Authorization", "Bearer "+v)
		}
	}
	if len(cookies) > 0 {
		h.Set("Cookie", strings.Join(cookies, "; "))
	}
	return rawQuery
}
