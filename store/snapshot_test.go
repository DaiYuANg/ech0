package store_test

import (
	"encoding/json"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
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
	requireNoError(t, st.SaveShardPlacement(store.NewShardPlacement("orders", 0, 2)))
	requireNoError(t, st.SaveACLPolicy(store.ACLPolicy{
		PolicyID:     "tenant-a-orders-produce",
		Tenant:       "tenant-a",
		Namespace:    "default",
		Principal:    "svc-a",
		ResourceType: "topic",
		ResourceName: "orders",
		Actions:      []string{"produce"},
		Effect:       store.ACLPolicyEffectAllow,
		Priority:     1,
	}))
	requireNoError(t, st.SaveProducerBatch(store.ProducerPublishedBatch{
		ProducerID:    7,
		ProducerEpoch: 1,
		Topic:         "orders",
		Partition:     0,
		BaseSequence:  10,
		RecordCount:   1,
		BaseOffset:    0,
		LastOffset:    0,
		NextOffset:    1,
		UpdatedAtMS:   100,
	}))
	snapshot, err := st.Snapshot()
	requireNoError(t, err)
	raw, err := json.Marshal(snapshot)
	requireNoError(t, err)
	var decoded store.Snapshot
	requireNoError(t, json.Unmarshal(raw, &decoded))

	restored := store.NewMemoryStore()
	requireNoError(t, restored.Restore(decoded))
	requireRestoredMemorySnapshot(t, restored)
}

func requireRestoredMemorySnapshot(t *testing.T, restored *store.MemoryStore) {
	t.Helper()
	requireRestoredRecords(t, restored)
	requireRestoredMembers(t, restored)
	requireRestoredAssignment(t, restored)
	requireRestoredShardPlacement(t, restored)
	requireRestoredACLPolicies(t, restored)
	requireRestoredProducerBatches(t, restored)
}

func requireRestoredRecords(t *testing.T, restored *store.MemoryStore) {
	t.Helper()
	records, err := restored.ReadFrom(store.NewTopicPartition("orders", 0), 0, 10)
	requireNoError(t, err)
	if len(records) != 1 || string(records[0].Payload) != "m1" {
		t.Fatalf("unexpected restored records: %#v", records)
	}
}

func requireRestoredMembers(t *testing.T, restored *store.MemoryStore) {
	t.Helper()
	members, err := restored.ListGroupMembers("workers")
	requireNoError(t, err)
	if len(members) != 1 || members[0].MemberID != "worker-1" || len(members[0].Topics) != 1 {
		t.Fatalf("unexpected restored members: %#v", members)
	}
}

func requireRestoredAssignment(t *testing.T, restored *store.MemoryStore) {
	t.Helper()
	assignment, err := restored.LoadGroupAssignment("workers")
	requireNoError(t, err)
	if assignment == nil || len(assignment.Assignments) != 1 {
		t.Fatalf("unexpected restored assignment: %#v", assignment)
	}
}

func requireRestoredShardPlacement(t *testing.T, restored *store.MemoryStore) {
	t.Helper()
	placement, err := restored.LoadShardPlacement(store.NewTopicPartition("orders", 0))
	requireNoError(t, err)
	if placement == nil || placement.ShardID != 2 {
		t.Fatalf("unexpected restored shard placement: %#v", placement)
	}
}

func requireRestoredACLPolicies(t *testing.T, restored *store.MemoryStore) {
	t.Helper()
	policies, err := restored.ListACLPolicies(store.ACLPolicyFilter{Tenant: "tenant-a"})
	requireNoError(t, err)
	if len(policies) != 1 || policies[0].PolicyID != "tenant-a-orders-produce" || len(policies[0].Actions) != 1 {
		t.Fatalf("unexpected restored acl policies: %#v", policies)
	}
}

func requireRestoredProducerBatches(t *testing.T, restored *store.MemoryStore) {
	t.Helper()
	batches, err := restored.ListProducerBatches(store.ProducerBatchFilter{ProducerID: 7})
	requireNoError(t, err)
	if len(batches) != 1 || batches[0].BaseSequence != 10 || batches[0].NextOffset != 1 {
		t.Fatalf("unexpected restored producer batches: %#v", batches)
	}
}
