package bridge

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// The multi-broker branches cannot run against the one-broker test cluster;
// the decision is pure and tested here. The one-broker branch runs in every
// integration test.
func TestDurabilityProblem(t *testing.T) {
	cases := []struct {
		name               string
		brokers, rf, isr   int
		unclean, wantFails bool
	}{
		{"dev: one broker", 1, 1, 1, false, false},
		{"dev: one broker, unclean", 1, 1, 1, true, true},
		{"prod: RF1 on 3 brokers", 3, 1, 1, false, true},
		{"prod: RF3 min.isr 1 (verifier's case)", 3, 3, 1, false, true},
		{"prod: RF3 min.isr 2", 3, 3, 2, false, false},
		{"prod: RF2 min.isr 2", 2, 2, 2, false, false},
		{"prod: RF3 min.isr 2 unclean", 3, 3, 2, true, true},
	}
	for _, c := range cases {
		got := durabilityProblem(c.brokers, c.rf, c.isr, c.unclean)
		if (got != "") != c.wantFails {
			t.Errorf("%s: %q", c.name, got)
		}
	}
}

// A broker that does not answer (restart, network) must not stop the
// bridge: the partition open fails and is retried. Only an answer proving
// the state broken is fatal (tested against a real broker in bridge_test).
func TestVerifyStateTransientErrorIsNotFatal(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.RetryTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var abandoned error
	b := &Bridge{admin: kadm.NewClient(cl), stateTopic: "http.bridge-state.x", stateTopicID: "id",
		abandon: func(err error) { abandoned = err }}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err = b.verifyState(ctx)
	if err == nil {
		t.Fatal("an unreachable broker cannot vouch for the state")
	}
	var f fatal
	if errors.As(err, &f) || abandoned != nil {
		t.Fatalf("transient error treated as fatal: %v (abandoned: %v)", err, abandoned)
	}
}
