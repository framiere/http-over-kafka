// Command payments runs demo service B "payments" on $ADDR (default :8082).
// PAYMENTS_DELAY (a Go duration) delays every charge response after the debit
// is applied, to reproduce client timeouts across processes.
package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/demo/payments"
)

func main() {
	addr := envOr("ADDR", ":8082")
	svc := payments.New()
	if v := os.Getenv("PAYMENTS_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("PAYMENTS_DELAY: %v", err)
		}
		svc.SetDelay(d)
	}
	srv := &http.Server{Addr: addr, Handler: svc.Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("payments listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
