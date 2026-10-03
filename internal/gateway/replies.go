package gateway

import (
	"sync"

	"github.com/sderosiaux/kafka-backbone-for-http/internal/wire"
)

// replies routes Responses from this instance's reply topic to the handler
// holding the connection. Keys are requestIds the gateway minted itself, so
// a caller can never claim another caller's response.
//
// Every entry is removed by the handler that registered it, whatever the exit
// path (reply, timeout, client gone, produce failure): the map holds at most
// one entry per request currently being served.
type replies struct {
	mu      sync.Mutex
	pending map[string]chan wire.Response
}

func newReplies() *replies { return &replies{pending: map[string]chan wire.Response{}} }

func (r *replies) register(requestID string) <-chan wire.Response {
	ch := make(chan wire.Response, 1)
	r.mu.Lock()
	r.pending[requestID] = ch
	r.mu.Unlock()
	return ch
}

func (r *replies) forget(requestID string) {
	r.mu.Lock()
	delete(r.pending, requestID)
	r.mu.Unlock()
}

// deliver hands resp to its waiter and reports whether one was waiting. The
// entry is removed under the lock, so a duplicate Response (bridge redelivery)
// is dropped instead of blocking, and the buffered send never blocks.
func (r *replies) deliver(resp wire.Response) bool {
	r.mu.Lock()
	ch, ok := r.pending[resp.RequestID]
	delete(r.pending, resp.RequestID)
	r.mu.Unlock()
	if ok {
		ch <- resp
	}
	return ok
}

func (r *replies) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}
