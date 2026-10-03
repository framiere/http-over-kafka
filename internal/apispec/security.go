package apispec

import (
	"fmt"
	"slices"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
)

// SecurityScheme is a components.securitySchemes entry: where B expects a
// credential (D12). Whatever a caller sends there is a secret and never
// enters Kafka.
type SecurityScheme struct {
	Name string // key under components.securitySchemes
	Type string // apiKey, http, oauth2, openIdConnect, mutualTLS
	// In and Param locate an apiKey: In is header, query or cookie.
	In    string
	Param string
	// Scheme is the http auth scheme (bearer, basic, ...).
	Scheme string
}

// Secrets are the request locations that carry credentials for this service
// according to its contract: header names (lowercase), query parameter names
// and cookie names. http, oauth2 and openIdConnect schemes put theirs in
// Authorization.
type Secrets struct {
	Headers []string
	Query   []string
	Cookies []string
}

// Secrets lists the credential locations declared by the service's contract.
func (s *Service) Secrets() Secrets { return s.secrets }

func (s *Service) SecurityScheme(name string) (SecurityScheme, bool) {
	sc, ok := s.schemes[name]
	return sc, ok
}

func loadSecurity(doc *openapi3.T) (map[string]SecurityScheme, Secrets, error) {
	schemes := map[string]SecurityScheme{}
	var sec Secrets
	add := func(list *[]string, v string) {
		if !slices.Contains(*list, v) {
			*list = append(*list, v)
		}
	}
	if doc.Components == nil {
		return schemes, sec, nil
	}
	for name, ref := range doc.Components.SecuritySchemes {
		if ref == nil || ref.Value == nil {
			return nil, sec, fmt.Errorf("securityScheme %q: unresolved", name)
		}
		v := ref.Value
		sc := SecurityScheme{Name: name, Type: v.Type, In: v.In, Param: v.Name, Scheme: strings.ToLower(v.Scheme)}
		switch v.Type {
		case "apiKey":
			switch v.In {
			case "header":
				add(&sec.Headers, strings.ToLower(v.Name))
			case "query":
				add(&sec.Query, v.Name)
			case "cookie":
				add(&sec.Cookies, v.Name)
			default:
				return nil, sec, fmt.Errorf("securityScheme %q: apiKey in %q", name, v.In)
			}
		case "http", "oauth2", "openIdConnect":
			add(&sec.Headers, "authorization")
		case "mutualTLS":
			// Carried by the TLS handshake, never by the request.
		default:
			return nil, sec, fmt.Errorf("securityScheme %q: unknown type %q", name, v.Type)
		}
		schemes[name] = sc
	}
	slices.Sort(sec.Headers)
	slices.Sort(sec.Query)
	slices.Sort(sec.Cookies)
	return schemes, sec, nil
}

// effectiveSecurity is the operation's security requirement: its own when
// declared, else the document's. Each alternative lists the schemes required
// together; an empty alternative means anonymous access is allowed. Nil means
// no requirement at all.
func effectiveSecurity(doc *openapi3.T, o *openapi3.Operation, schemes map[string]SecurityScheme) ([][]string, error) {
	reqs := doc.Security
	if o.Security != nil {
		reqs = *o.Security
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	out := make([][]string, 0, len(reqs))
	for _, r := range reqs {
		alt := make([]string, 0, len(r))
		for name := range r {
			if _, ok := schemes[name]; !ok {
				return nil, fmt.Errorf("security requirement names undeclared scheme %q", name)
			}
			alt = append(alt, name)
		}
		slices.Sort(alt)
		out = append(out, alt)
	}
	return out, nil
}
