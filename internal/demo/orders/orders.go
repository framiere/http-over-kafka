// Package orders is demo service B: an ordinary in-memory CRUD HTTP API.
package orders

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/demo/demohttp"
)

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Input struct {
	CustomerID string `json:"customerId"`
	Items      []Item `json:"items"`
}

type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customerId"`
	Items      []Item    `json:"items"`
	Version    int       `json:"version"`
	CreatedAt  time.Time `json:"createdAt"`
}

type Service struct {
	mu      sync.Mutex
	orders  map[string]Order
	creates int
}

func New() *Service { return &Service{orders: map[string]Order{}} }

// Creates counts successful POST /orders executions, for tests asserting
// how many times B really ran.
func (s *Service) Creates() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.creates
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", s.create)
	mux.HandleFunc("GET /orders", s.list)
	mux.HandleFunc("GET /orders/{orderId}", s.get)
	mux.HandleFunc("PUT /orders/{orderId}", s.replace)
	mux.HandleFunc("DELETE /orders/{orderId}", s.delete)
	return mux
}

func (in Input) validate() string {
	if strings.TrimSpace(in.CustomerID) == "" {
		return "customerId is required"
	}
	if len(in.Items) == 0 {
		return "items must not be empty"
	}
	for i, it := range in.Items {
		if it.SKU == "" || it.Quantity < 1 {
			return fmt.Sprintf("items[%d] needs a sku and a quantity >= 1", i)
		}
	}
	return ""
}

func etag(o Order) string { return fmt.Sprintf(`"%s-%d"`, o.ID, o.Version) }

func (s *Service) create(w http.ResponseWriter, r *http.Request) {
	var in Input
	if !demohttp.DecodeJSON(w, r, &in) {
		return
	}
	if msg := in.validate(); msg != "" {
		demohttp.Problem(w, http.StatusBadRequest, msg)
		return
	}
	o := Order{ID: "ord_" + ulid.Make().String(), CustomerID: in.CustomerID, Items: in.Items, Version: 1, CreatedAt: time.Now().UTC()}
	s.mu.Lock()
	s.orders[o.ID] = o
	s.creates++
	s.mu.Unlock()
	w.Header().Set("Location", "/orders/"+o.ID)
	w.Header().Set("ETag", etag(o))
	demohttp.WriteJSON(w, http.StatusCreated, o)
}

func (s *Service) list(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]Order, 0, len(s.orders))
	for _, o := range s.orders {
		out = append(out, o)
	}
	s.mu.Unlock()
	slices.SortFunc(out, func(a, b Order) int { return strings.Compare(a.ID, b.ID) })
	demohttp.WriteJSON(w, http.StatusOK, out)
}

func (s *Service) get(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	o, ok := s.orders[r.PathValue("orderId")]
	s.mu.Unlock()
	if !ok {
		demohttp.Problem(w, http.StatusNotFound, "no such order")
		return
	}
	w.Header().Set("ETag", etag(o))
	demohttp.WriteJSON(w, http.StatusOK, o)
}

func (s *Service) replace(w http.ResponseWriter, r *http.Request) {
	var in Input
	if !demohttp.DecodeJSON(w, r, &in) {
		return
	}
	if msg := in.validate(); msg != "" {
		demohttp.Problem(w, http.StatusBadRequest, msg)
		return
	}
	id := r.PathValue("orderId")
	s.mu.Lock()
	o, ok := s.orders[id]
	if ok {
		o.CustomerID, o.Items, o.Version = in.CustomerID, in.Items, o.Version+1
		s.orders[id] = o
	}
	s.mu.Unlock()
	if !ok {
		demohttp.Problem(w, http.StatusNotFound, "no such order")
		return
	}
	w.Header().Set("ETag", etag(o))
	demohttp.WriteJSON(w, http.StatusOK, o)
}

func (s *Service) delete(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	_, ok := s.orders[r.PathValue("orderId")]
	delete(s.orders, r.PathValue("orderId"))
	s.mu.Unlock()
	if !ok {
		demohttp.Problem(w, http.StatusNotFound, "no such order")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
