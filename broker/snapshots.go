package broker

import (
	"bytes"
	"cmp"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (b *Broker) TopicSummaries() ([]TopicSummary, error) {
	topics, err := b.ListTopics()
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

func (b *Broker) TopicMessagesSnapshot(topic string, partition uint32, offset uint64, limit int) (TopicMessagesPageSummary, error) {
	if limit <= 0 || limit > b.cfg.Broker.MaxFetchRecords {
		limit = b.cfg.Broker.MaxFetchRecords
	}
	tp := store.NewTopicPartition(topic, partition)
	records, err := b.log.ReadFrom(tp, offset, limit)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBrokerStore(err, "read topic messages")
	}
	highWatermark, err := topicPartitionHighWatermark(b.log, tp)
	if err != nil {
		return TopicMessagesPageSummary{}, err
	}
	items := collectionlist.NewList[TopicMessageSummary]()
	nextOffset := offset
	for _, record := range records {
		items.Add(topicMessageSummary(record))
		nextOffset = record.Offset + 1
	}
	return TopicMessagesPageSummary{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		Limit:         limit,
		NextOffset:    nextOffset,
		HighWatermark: highWatermark,
		Records:       items.Values(),
	}, nil
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
			Topics:           append([]string(nil), member.Topics...),
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
	topics, err := b.TopicSummaries()
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
		highWatermark, err := topicPartitionHighWatermark(b.log, tp)
		if err != nil {
			return TopicSummary{}, err
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
		highWatermark, err := topicPartitionHighWatermark(b.log, tp)
		if err != nil {
			return GroupLagSummary{}, err
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

func topicMessageSummary(record store.Record) TopicMessageSummary {
	preview := record.Payload
	if len(preview) > 96 {
		preview = preview[:96]
	}
	utf8Preview := strings.TrimSpace(string(bytes.ToValidUTF8(preview, []byte("."))))
	jsonPreview := compactJSONPreview(preview)
	return TopicMessageSummary{
		Offset:             record.Offset,
		TimestampMS:        record.TimestampMS,
		PayloadSize:        len(record.Payload),
		PayloadUTF8Preview: utf8Preview,
		PayloadHexPreview:  hex.EncodeToString(preview),
		PayloadJSONPreview: jsonPreview,
	}
}

func compactJSONPreview(payload []byte) *string {
	if !json.Valid(payload) {
		return nil
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, payload); err != nil {
		return nil
	}
	value := compacted.String()
	if len(value) > 160 {
		value = value[:160]
	}
	return &value
}

func displayUint64Ptr(value *uint64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatUint(*value, 10)
}
