package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/sderosiaux/http-over-kafka/internal/events"
	"github.com/sderosiaux/http-over-kafka/internal/kafkaenv"
	"github.com/sderosiaux/http-over-kafka/internal/kafkatest"
	"github.com/sderosiaux/http-over-kafka/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Removing a live member produces the same UNKNOWN_MEMBER_ID heartbeat as
// session expiry. Use a dedicated broker, even when KAFKA_BROKERS is set.
func TestDeriverRejoinsAfterMembershipLoss(t *testing.T) {
	broker := kafkatest.Dedicated(t)
	name := kafkatest.Service(t, "rejoin")
	svc := load(t, name, ordersSpec(name))
	cl, err := kgo.NewClient(kafkaenv.ClientOpts(broker.Brokers)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	adm := kadm.NewClient(cl)
	topics := append(kafkaenv.ServiceTopics(name, partitions), events.Topics(svc, partitions, testWindow)...)
	for _, topic := range topics {
		if _, err := adm.CreateTopic(ctx, topic.Partitions, 1, topic.Configs, topic.Name); err != nil {
			t.Fatal(err)
		}
	}
	signer, _ := bridgeSigner(t, "rejoin")
	d, err := events.New(events.Config{Brokers: broker.Brokers, Service: svc, Instance: "rejoin",
		BridgeKeys: signer.Self(), SessionTimeout: shortSession, DedupWindow: testWindow})
	if err != nil {
		t.Fatal(err)
	}
	run := start(d)
	defer run.cancel()
	want := map[string]bool{}
	produceAndCatchUp := func() {
		t.Helper()
		for range 3 {
			cmd := createOrder(t, name, orderReq)
			want[cmd.RequestID] = true
			rec, err := wire.EncodeResult(result(t, cmd, answered(cmd, 201, orderResp(cmd.RequestID))), signer)
			if err != nil {
				t.Fatal(err)
			}
			if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		kafkatest.Eventually(t, 20*time.Second, func() bool {
			select {
			case err := <-run.done:
				t.Fatalf("deriver stopped during recovery: %v", err)
			default:
			}
			ends, err := adm.ListEndOffsets(ctx, wire.ResultTopic(name))
			if err != nil {
				t.Fatal(err)
			}
			offsets, err := adm.FetchOffsets(ctx, events.GroupID(name))
			if err != nil {
				return false
			}
			ready := true
			ends.Each(func(end kadm.ListedOffset) {
				offset, ok := offsets.Lookup(end.Topic, end.Partition)
				if end.Err != nil || (end.Offset > 0 && (!ok || offset.Err != nil || offset.At < end.Offset)) {
					ready = false
				}
			})
			return ready
		}, "transactional offsets did not resume")
	}
	produceAndCatchUp()
	for round := range 2 {
		groups, err := adm.DescribeGroups(ctx, events.GroupID(name))
		if err != nil {
			t.Fatal(err)
		}
		group := groups[events.GroupID(name)]
		if group.Err != nil || group.State != "Stable" || len(group.Members) != 1 {
			t.Fatalf("expected one stable member: %+v", group)
		}
		oldID := group.Members[0].MemberID
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = events.GroupID(name)
		req.MemberID = oldID
		req.Members = []kmsg.LeaveGroupRequestMember{{MemberID: oldID}}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ErrorCode != 0 {
			t.Fatal(kerr.ErrorForCode(resp.ErrorCode))
		}
		for _, member := range resp.Members {
			if member.ErrorCode != 0 {
				t.Fatal(kerr.ErrorForCode(member.ErrorCode))
			}
		}
		kafkatest.Eventually(t, 20*time.Second, func() bool {
			select {
			case err := <-run.done:
				t.Fatalf("deriver stopped on membership loss: %v", err)
			default:
			}
			groups, err := adm.DescribeGroups(ctx, events.GroupID(name))
			group := groups[events.GroupID(name)]
			return err == nil && group.Err == nil && group.State == "Stable" &&
				len(group.Members) == 1 && group.Members[0].MemberID != oldID
		}, "removed member did not rejoin with a new identity")
		produceAndCatchUp()
		t.Logf("membership loss %d: new member joined and transactional offsets resumed", round+1)
	}
	run.stop(t)

	// Read the complete committed event stream, including its end markers,
	// so an extra event after the expected count cannot escape the assertion.
	eventTopic := name + ".events"
	ends, err := adm.ListEndOffsets(ctx, eventTopic)
	if err != nil {
		t.Fatal(err)
	}
	need := map[int32]int64{}
	ends.Each(func(end kadm.ListedOffset) {
		if end.Err != nil {
			t.Fatal(end.Err)
		}
		if end.Offset > 0 {
			need[end.Partition] = end.Offset
		}
	})
	reader, err := kgo.NewClient(kafkaenv.ClientOpts(broker.Brokers,
		kgo.ConsumeTopics(eventTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())...)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := map[string]bool{}
	for len(need) > 0 {
		fs := reader.PollFetches(ctx)
		if err := fs.Err(); err != nil {
			t.Fatal(err)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if !r.Attrs.IsControl() {
				id := header(r, "ce_id")
				if !want[id] || seen[id] {
					t.Fatalf("unexpected or duplicate committed event %q", id)
				}
				seen[id] = true
			}
			if end, ok := need[r.Partition]; ok && r.Offset+1 >= end {
				delete(need, r.Partition)
			}
		})
	}
	if len(seen) != len(want) {
		t.Fatalf("committed events=%d, want=%d", len(seen), len(want))
	}
	t.Logf("all %d expected events committed exactly once", len(seen))
}
