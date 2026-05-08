package store_test

import (
	"encoding/json"
	"testing"

	"github.com/DaiYuANg/ech0/store"
)

func TestMemorySnapshotRoundTripsJSONWithCollectionLists(t *testing.T) {
	st := store.NewMemoryStore()
	topic := store.NewTopicConfig("orders")
	requireNoError(t, st.CreateTopic(topic))
	_, err := st.AppendRecord(store.NewTopicPartition("orders", 0), store.RecordAppend{
		Headers: []store.RecordHeader{{Key: "trace_id", Value: []byte("trace-1")}},
		Payload: []byte("m1"),
	})
	requireNoError(t, err)
	requireNoError(t, st.SaveGroupMember(store.ConsumerGroupMember{
		Group:            "workers",
		MemberID:         "worker-1",
		Topics:           []string{"orders"},
		SessionTimeoutMS: 1000,
		LastHeartbeatMS:  10,
	}))
	requireNoError(t, st.SaveGroupAssignment(store.ConsumerGroupAssignment{
		Group:      "workers",
		Generation: 1,
		Assignments: []store.GroupPartitionAssignment{
			{MemberID: "worker-1", Topic: "orders", Partition: 0},
		},
	}))
	snapshot, err := st.Snapshot()
	requireNoError(t, err)
	raw, err := json.Marshal(snapshot)
	requireNoError(t, err)
	var decoded store.Snapshot
	requireNoError(t, json.Unmarshal(raw, &decoded))

	restored := store.NewMemoryStore()
	requireNoError(t, restored.Restore(decoded))
	records, err := restored.ReadFrom(store.NewTopicPartition("orders", 0), 0, 10)
	requireNoError(t, err)
	if len(records) != 1 || string(records[0].Payload) != "m1" {
		t.Fatalf("unexpected restored records: %#v", records)
	}
	members, err := restored.ListGroupMembers("workers")
	requireNoError(t, err)
	if len(members) != 1 || members[0].MemberID != "worker-1" || len(members[0].Topics) != 1 {
		t.Fatalf("unexpected restored members: %#v", members)
	}
	assignment, err := restored.LoadGroupAssignment("workers")
	requireNoError(t, err)
	if assignment == nil || len(assignment.Assignments) != 1 {
		t.Fatalf("unexpected restored assignment: %#v", assignment)
	}
}
