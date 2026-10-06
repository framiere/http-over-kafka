// Package payments is demo service B with an observable, countable side
// effect: every executed POST /charges debits an account. It deliberately has
// no idempotency of its own: duplicates must be prevented upstream, and any
// that slip through are visible in the account's debitCount.
package payments

import (
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/sderosiaux/http-over-kafka/internal/demo/demohttp"
)

type Input struct {
	AccountID   string `json:"accountId"`
	AmountCents int64  `json:"amountCents"`
	Currency    string `json:"currency"`
}

type Charge struct {
	ID          string    `json:"id"`
	AccountID   string    `json:"accountId"`
	AmountCents int64     `json:"amountCents"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"createdAt"`
}

type Account struct {
	AccountID    string `json:"accountId"`
	DebitCount   int    `json:"debitCount"`
	DebitedCents int64  `json:"debitedCents"`
}

type Service struct {
	mu       sync.Mutex
	charges  map[string]Charge
	accounts map[string]Account
	delay    time.Duration
}

func New() *Service {
	return &Service{charges: map[string]Charge{}, accounts: map[string]Account{}}
}

// SetDelay makes each charge take d after the debit is applied and before
// the response is written: the window in which a caller times out while the
// effect has already happened.
func (s *Service) SetDelay(d time.Duration) {
	s.mu.Lock()
	s.delay = d
	s.mu.Unlock()
}

// Account returns the debits applied so far; tests poll it to count effects.
func (s *Service) Account(id string) Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.accounts[id]
	a.AccountID = id
	return a
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /charges", s.charge)
	mux.HandleFunc("GET /charges/{chargeId}", s.getCharge)
	mux.HandleFunc("GET /accounts/{accountId}", s.getAccount)
	return mux
}

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

func (s *Service) charge(w http.ResponseWriter, r *http.Request) {
	var in Input
	if !demohttp.DecodeJSON(w, r, &in) {
		return
	}
	switch {
	case strings.TrimSpace(in.AccountID) == "":
		demohttp.Problem(w, http.StatusBadRequest, "accountId is required")
		return
	case in.AmountCents < 1:
		demohttp.Problem(w, http.StatusBadRequest, "amountCents must be >= 1")
		return
	case !currencyRe.MatchString(in.Currency):
		demohttp.Problem(w, http.StatusBadRequest, "currency must be ISO 4217, e.g. EUR")
		return
	}
	c := Charge{ID: "ch_" + ulid.Make().String(), AccountID: in.AccountID, AmountCents: in.AmountCents, Currency: in.Currency, CreatedAt: time.Now().UTC()}
	s.mu.Lock()
	s.charges[c.ID] = c
	a := s.accounts[c.AccountID]
	a.DebitCount++
	a.DebitedCents += c.AmountCents
	s.accounts[c.AccountID] = a
	delay := s.delay
	s.mu.Unlock()
	if delay > 0 {
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-t.C:
		case <-r.Context().Done():
			// The debit stands even if nobody hears about it: that is the
			// failure mode under test.
			return
		}
	}
	w.Header().Set("Location", "/charges/"+c.ID)
	demohttp.WriteJSON(w, http.StatusCreated, c)
}

func (s *Service) getCharge(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	c, ok := s.charges[r.PathValue("chargeId")]
	s.mu.Unlock()
	if !ok {
		demohttp.Problem(w, http.StatusNotFound, "no such charge")
		return
	}
	demohttp.WriteJSON(w, http.StatusOK, c)
}

func (s *Service) getAccount(w http.ResponseWriter, r *http.Request) {
	demohttp.WriteJSON(w, http.StatusOK, s.Account(r.PathValue("accountId")))
}
