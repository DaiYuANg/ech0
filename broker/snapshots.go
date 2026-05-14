package broker

import (
	"cmp"
	"context"
	"strconv"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) TopicSummaries() ([]TopicSummary, error) {
	return b.TopicSummariesFor(context.Background())
}

func (b *Broker) TopicSummariesFor(ctx context.Context) ([]TopicSummary, error) {
	topics, err := b.ListTopicsFor(ctx)
	if err != nil {
		return nil, err
	}
	out := collectionlist.NewList[TopicSummary]()
	for i := range topics {
		summary, summaryErr := b.topicSummary(topics[i])
		if summaryErr != nil {
			return nil, summaryErr
		}
		out.Add(summary)
	}
	return out.Sort(func(left, right TopicSummary) int {
		return cmp.Compare(left.Name, right.Name)
	}).Values(), nil
}

func (b *Broker) GroupMembersSnapshot(group string) ([]GroupMemberSummary, error) {
	members, err := b.meta.ListGroupMembers(group)
	if err != nil {
		return nil, wrapBrokerStore(err, "list group members")
	}
	out := collectionlist.NewList[GroupMemberSummary]()
	for _, member := range members {
		out.Add(GroupMemberSummary{
			Group:            member.Group,
			MemberID:         member.MemberID,
			Topics:           collectionlist.NewList(member.Topics...).Values(),
			SessionTimeoutMS: member.SessionTimeoutMS,
			JoinedAtMS:       member.JoinedAtMS,
			LastHeartbeatMS:  member.LastHeartbeatMS,
			ExpiresAtMS:      member.ExpiresAtMS(),
		})
	}
	return out.Sort(func(left, right GroupMemberSummary) int {
		return cmp.Compare(left.MemberID, right.MemberID)
	}).Values(), nil
}

func (b *Broker) GroupAssignmentSnapshot(group string) (*GroupAssignmentSummary, error) {
	assignment, err := b.meta.LoadGroupAssignment(group)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group assignment")
	}
	if assignment == nil {
		return &GroupAssignmentSummary{Group: group}, nil
	}
	summary := groupAssignmentSummary(*assignment)
	return &summary, nil
}

func (b *Broker) GroupLagSnapshot(group string) (*GroupLagSummary, error) {
	assignment, err := b.meta.LoadGroupAssignment(group)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group assignment")
	}
	if assignment == nil {
		return &GroupLagSummary{Group: group}, nil
	}
	summary, err := b.groupLagFromAssignment(*assignment)
	if err != nil {
		return nil, err
	}
	return &summary, nil
}

func (b *Broker) StreamMetricsSnapshot() (StreamMetricsSnapshot, error) {
	return b.StreamMetricsSnapshotFor(context.Background())
}

func (b *Broker) StreamMetricsSnapshotFor(ctx context.Context) (StreamMetricsSnapshot, error) {
	topics, err := b.TopicSummariesFor(ctx)
	if err != nil {
		return StreamMetricsSnapshot{}, err
	}
	snapshot := StreamMetricsSnapshot{TopicCount: len(topics)}
	for i := range topics {
		topic := topics[i]
		if topic.TotalBacklogRecords > 0 {
			snapshot.TopicsWithBacklog++
		}
		snapshot.TotalTopicBacklogRecords += topic.TotalBacklogRecords
		snapshot.MaxTopicBacklogRecords = max(snapshot.MaxTopicBacklogRecords, topic.TotalBacklogRecords)
		snapshot.MaxPartitionBacklogRecords = max(snapshot.MaxPartitionBacklogRecords, topic.MaxPartitionBacklog)
	}
	assignments, err := b.meta.ListGroupAssignments()
	if err != nil {
		return StreamMetricsSnapshot{}, wrapBrokerStore(err, "list group assignments")
	}
	snapshot.ConsumerGroupCount = len(assignments)
	for _, assignment := range assignments {
		lag, lagErr := b.groupLagFromAssignment(assignment)
		if lagErr != nil {
			return StreamMetricsSnapshot{}, lagErr
		}
		if lag.TotalLagRecords > 0 {
			snapshot.ConsumerGroupsWithLag++
		}
		snapshot.TotalConsumerGroupBacklogRecords += lag.TotalBacklogRecords
		snapshot.TotalConsumerGroupLagRecords += lag.TotalLagRecords
		snapshot.MaxConsumerGroupLagRecords = max(snapshot.MaxConsumerGroupLagRecords, lag.TotalLagRecords)
	}
	return snapshot, nil
}

func (b *Broker) topicSummary(topic store.TopicConfig) (TopicSummary, error) {
	summary := TopicSummary{
		Name:                           topic.Name,
		Partitions:                     topic.Partitions,
		SegmentMaxBytes:                topic.SegmentMaxBytes,
		IndexIntervalBytes:             topic.IndexIntervalBytes,
		RetentionMaxBytes:              topic.RetentionMaxBytes,
		CleanupPolicy:                  string(topic.CleanupPolicy),
		RetentionMS:                    topic.RetentionMS,
		CompactionTombstoneRetentionMS: topic.CompactionTombstoneRetentionMS,
		MaxMessageBytes:                topic.MaxMessageBytes,
		MaxBatchBytes:                  topic.MaxBatchBytes,
		RetryMaxAttempts:               topic.RetryPolicy.MaxAttempts,
		DeadLetterTopic:                topic.DeadLetterTopic,
		DelayEnabled:                   topic.DelayEnabled,
		CompactionEnabled:              topic.CompactionEnabled,
	}
	for partition := range topic.Partitions {
		tp := store.NewTopicPartition(topic.Name, partition)
		highWatermark, err := b.queue.LastOffset(tp)
		if err != nil {
			return TopicSummary{}, wrapBroker("topic_summary_high_watermark_failed", err, "load topic partition high watermark")
		}
		backlog := highWatermarkBacklog(highWatermark)
		summary.ProducedRecordsTotal += backlog
		summary.TotalBacklogRecords += backlog
		if backlog > summary.HottestPartitionRecords {
			selected := partition
			summary.HottestPartition = &selected
			summary.HottestPartitionRecords = backlog
		}
		summary.MaxPartitionBacklog = max(summary.MaxPartitionBacklog, backlog)
	}
	return summary, nil
}

func (b *Broker) groupLagFromAssignment(assignment store.ConsumerGroupAssignment) (GroupLagSummary, error) {
	out := GroupLagSummary{
		Group:      assignment.Group,
		Generation: assignment.Generation,
	}
	partitions := collectionlist.NewList[GroupPartitionLagSummary]()
	for _, item := range assignment.Assignments {
		tp := store.NewTopicPartition(item.Topic, item.Partition)
		highWatermark, err := b.queue.LastOffset(tp)
		if err != nil {
			return GroupLagSummary{}, wrapBroker("group_lag_high_watermark_failed", err, "load group partition high watermark")
		}
		committed := uint64(0)
		if offset, offsetErr := b.meta.LoadConsumerOffset(groupConsumer(assignment.Group), tp); offsetErr != nil {
			return GroupLagSummary{}, wrapBrokerStore(offsetErr, "load group committed offset")
		} else if offset != nil {
			committed = *offset
		}
		backlog := highWatermarkBacklog(highWatermark)
		lag := lagRecords(committed, highWatermark)
		out.TotalBacklogRecords += backlog
		out.TotalLagRecords += lag
		partitions.Add(GroupPartitionLagSummary{
			MemberID:            item.MemberID,
			Topic:               item.Topic,
			Partition:           item.Partition,
			CommittedNextOffset: committed,
			HighWatermark:       highWatermark,
			BacklogRecords:      backlog,
			LagRecords:          lag,
		})
	}
	out.Partitions = partitions.Sort(func(left, right GroupPartitionLagSummary) int {
		if left.Topic == right.Topic {
			return cmp.Compare(left.Partition, right.Partition)
		}
		return cmp.Compare(left.Topic, right.Topic)
	}).Values()
	return out, nil
}

func groupAssignmentSummary(assignment store.ConsumerGroupAssignment) GroupAssignmentSummary {
	items := collectionlist.NewList[GroupPartitionOwnerSummary]()
	for _, item := range assignment.Assignments {
		items.Add(GroupPartitionOwnerSummary{
			MemberID:  item.MemberID,
			Topic:     item.Topic,
			Partition: item.Partition,
		})
	}
	return GroupAssignmentSummary{
		Group:      assignment.Group,
		Generation: assignment.Generation,
		Assignments: items.Sort(func(left, right GroupPartitionOwnerSummary) int {
			if left.Topic == right.Topic {
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).Values(),
		UpdatedAtMS: assignment.UpdatedAtMS,
	}
}

func displayUint64Ptr(value *uint64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatUint(*value, 10)
}
