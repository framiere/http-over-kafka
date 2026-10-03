// Command orders runs demo service B "orders" on $ADDR (default :8081).
package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/demo/orders"
)

func main() {
	addr := envOr("ADDR", ":8081")
	srv := &http.Server{Addr: addr, Handler: orders.New().Handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("orders listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
