//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// D6: with several gateway instances, every caller gets the response to its
// own request, never another's, including while one instance restarts.
func TestD6_ResponsesReachTheRightConnection(t *testing.T) {
	spec := t.TempDir()
	e := startEcho(t, spec)
	gws := []*gw{newGateway(t, spec, 5*time.Second, e).startReady(t), newGateway(t, spec, 5*time.Second, e).startReady(t), newGateway(t, spec, 5*time.Second, e).startReady(t)}
	b1, b2 := newBridge(t, spec, e, "b1"), newBridge(t, spec, e, "b2")
	startBridgeReady(t, b1)
	b2.start()
	a := newCaller(t, "checkout")

	var ok, mismatched, failed atomic.Int64
	var wg sync.WaitGroup
	var seq atomic.Int64
	deadline := time.Now().Add(20 * time.Second)
	for w := range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				n := seq.Add(1)
				nonce := fmt.Sprintf(`{"nonce":%d,"worker":%d}`, n, w)
				r := a.via(gws[int(n)%3], e.name, req{method: "POST", path: fmt.Sprintf("/echo/%d", n), body: nonce})
				if r.status != 200 {
					failed.Add(1)
					continue
				}
				var got echoed
				_ = json.Unmarshal(r.body, &got)
				if string(got.Body) != nonce || got.EscapedPath != fmt.Sprintf("/echo/%d", n) {
					mismatched.Add(1)
					t.Errorf("request %d got the response of %s %s", n, got.EscapedPath, got.Body)
					continue
				}
				ok.Add(1)
			}
		}()
	}
	time.Sleep(7 * time.Second) // mid-run
	gws[1].kill()
	gws[1].startReady(t)
	wg.Wait()
	t.Logf("3 gateways (one killed -9 and restarted mid-run), 24 workers: %d correct, %d mismatched, %d failed (in-flight on the killed instance or during its restart)", ok.Load(), mismatched.Load(), failed.Load())
}
