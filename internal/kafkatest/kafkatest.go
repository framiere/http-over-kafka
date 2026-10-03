// Package kafkatest gives integration tests a real Kafka broker.
//
// One broker per test binary, started lazily by the first Brokers call
// (Testcontainers, image Image, auto topic creation off) or taken from $KAFKA_BROKERS when set, e.g.
// the compose broker for a faster local loop. Tests isolate themselves by
// using a unique service name (Service), not by starting brokers.
//
// Use Main from TestMain to terminate the container at the end of the run;
// without it Testcontainers' reaper removes it when the process exits.
package kafkatest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/sderosiaux/kafka-backbone-for-http/internal/kafkaenv"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Image is the broker used by tests, the same as compose.yaml. The
// Testcontainers kafka module is not used: it only supports confluent-local,
// whose recent versions fail to start under it.
const Image = "apache/kafka:3.9.1"

var (
	once      sync.Once
	brokers   []string
	startErr  error
	container testcontainers.Container
)

const advertisedFile = "/tmp/advertised.env"

func start() {
	if v, err := kafkaenv.BrokersFromEnv(); err == nil {
		brokers = v
		return
	}
	container, brokers, startErr = runBroker()
}

func runBroker() (testcontainers.Container, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	// The advertised listener needs the host port, known only after start:
	// the container waits for a file written by the post-start hook.
	c, err := testcontainers.Run(ctx, Image,
		testcontainers.WithExposedPorts("9092/tcp"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,BROKER://:29092,CONTROLLER://:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "BROKER",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
		}),
		testcontainers.WithEntrypoint("sh"),
		testcontainers.WithCmd("-c", "while [ ! -f "+advertisedFile+" ]; do sleep 0.1; done; . "+advertisedFile+"; exec /etc/kafka/docker/run"),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{func(ctx context.Context, c testcontainers.Container) error {
				if err := wait.ForMappedPort("9092/tcp").WaitUntilReady(ctx, c); err != nil {
					return err
				}
				ep, err := c.PortEndpoint(ctx, "9092/tcp", "PLAINTEXT")
				if err != nil {
					return err
				}
				env := fmt.Sprintf("export KAFKA_ADVERTISED_LISTENERS=%s,BROKER://localhost:29092\n", ep)
				if err := c.CopyToContainer(ctx, []byte(env), advertisedFile, 0o644); err != nil {
					return err
				}
				return wait.ForLog("Kafka Server started").WaitUntilReady(ctx, c)
			}},
		}),
	)
	if err != nil {
		return c, nil, fmt.Errorf("kafkatest: start %s: %w", Image, err)
	}
	ep, err := c.PortEndpoint(ctx, "9092/tcp", "")
	if err != nil {
		return c, nil, err
	}
	return c, []string{ep}, nil
}

// Broker is a broker owned by one test, for failure scenarios that must not
// disturb the shared one.
type Broker struct {
	Brokers   []string
	container testcontainers.Container
}

// Dedicated starts a fresh broker for t (always a container, even when
// KAFKA_BROKERS is set), terminated at cleanup. Slow: use only to break it.
func Dedicated(t testing.TB) *Broker {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Kafka; skipped with -short")
	}
	c, bs, err := runBroker()
	if c != nil {
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatal(err)
	}
	return &Broker{Brokers: bs, container: c}
}

// Pause freezes the broker process: connections stay open but nothing is
// answered, like a broker hung or partitioned away. Unpause resumes it.
func (b *Broker) Pause(t testing.TB) { t.Helper(); b.docker(t, true) }

func (b *Broker) Unpause(t testing.TB) { t.Helper(); b.docker(t, false) }

func (b *Broker) docker(t testing.TB, pause bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cli, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	id := b.container.GetContainerID()
	if pause {
		_, err = cli.ContainerPause(ctx, id, client.ContainerPauseOptions{})
	} else {
		_, err = cli.ContainerUnpause(ctx, id, client.ContainerUnpauseOptions{})
	}
	if err != nil {
		t.Fatal(err)
	}
}

// Main runs the tests and then terminates the broker if this process started it.
func Main(m *testing.M) {
	code := m.Run()
	if container != nil {
		_ = container.Terminate(context.Background())
	}
	os.Exit(code)
}

// Brokers returns the broker addresses, starting the broker on first use.
// Under -short, tests needing Kafka are skipped.
func Brokers(t testing.TB) []string {
	t.Helper()
	if testing.Short() {
		t.Skip("needs Kafka; skipped with -short")
	}
	once.Do(start)
	if startErr != nil {
		t.Fatal(startErr)
	}
	return brokers
}

// Client returns a client with the project's base options, closed at cleanup.
func Client(t testing.TB, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(Brokers(t), opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func Admin(t testing.TB) *kadm.Client {
	t.Helper()
	return kadm.NewClient(Client(t))
}

// Service returns a service name unique to this run, so tests sharing the
// broker never read each other's topics.
func Service(t testing.TB, prefix string) string {
	t.Helper()
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return prefix + "-" + hex.EncodeToString(b)
}

// CreateTopics provisions topics and fails the test on error.
func CreateTopics(t testing.TB, topics ...kafkaenv.Topic) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kafkaenv.EnsureTopics(ctx, Admin(t), topics...); err != nil {
		t.Fatal(err)
	}
}

// Eventually polls cond every 20ms until it holds or timeout elapses.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, format string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s: %s", timeout, fmt.Sprintf(format, args...))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Consume reads topic from the beginning (read_committed: records of aborted
// or open transactions are invisible, as for any real consumer) until done
// returns true for the records seen so far, or fails after timeout.
func Consume(t testing.TB, topic string, timeout time.Duration, done func([]*kgo.Record) bool) []*kgo.Record {
	t.Helper()
	cl := Client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var recs []*kgo.Record
	for !done(recs) {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consume %s: %d records after %s, condition not met", topic, len(recs), timeout)
		}
		fs.EachError(func(tp string, p int32, err error) {
			t.Fatalf("consume %s[%d]: %v", tp, p, err)
		})
		recs = append(recs, fs.Records()...)
	}
	return recs
}

// EndOffsets sums the end offsets of topics, or of every topic in the
// cluster when none is given. Transaction markers count. To assert that an
// action wrote nothing to Kafka, compare the all-topics sum before and after;
// that check is only sound when no other test writes concurrently (no
// t.Parallel, and not against a shared compose broker in use).
func EndOffsets(t testing.TB, topics ...string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	adm := Admin(t)
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) {
		if strings.HasPrefix(o.Topic, "__") { // consumer offsets, transaction state
			return
		}
		if o.Err != nil {
			t.Fatalf("end offsets %s[%d]: %v", o.Topic, o.Partition, o.Err)
		}
		total += o.Offset
	})
	return total
}
