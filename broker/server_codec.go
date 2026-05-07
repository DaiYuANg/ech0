package broker

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/DaiYuANg/ech0/transport"
)

func (s *TCPServer) recordCommandError(ctx context.Context, frame transport.Frame) {
	if s == nil || s.metrics == nil || frame.Header.Command != protocol.CmdErrorResponse {
		return
	}
	var out protocol.ErrorResponse
	if err := json.Unmarshal(frame.Body, &out); err != nil || out.Code == "" {
		s.metrics.RecordCommandError(ctx, "internal_error")
		return
	}
	s.metrics.RecordCommandError(ctx, out.Code)
}

func decode(frame transport.Frame, target any) error {
	return wrapBroker("frame_decode_failed", json.Unmarshal(frame.Body, target), "decode request frame")
}

func okFrame(command uint16, value any) (transport.Frame, error) {
	body, err := protocol.EncodeJSON(value)
	if err != nil {
		return transport.Frame{}, wrapBroker("response_encode_failed", err, "encode response frame")
	}
	frame, err := transport.NewFrame(protocol.Version1, command, body)
	if err != nil {
		return transport.Frame{}, wrapBroker("response_frame_create_failed", err, "create response frame")
	}
	return frame, nil
}

func errorFrame(code, message string) transport.Frame {
	body, err := protocol.EncodeJSON(protocol.ErrorResponse{Code: code, Message: message})
	if err != nil {
		body = []byte(`{"code":"internal_error","message":"failed to encode error response"}`)
	}
	frame, err := transport.NewFrame(protocol.Version1, protocol.CmdErrorResponse, body)
	if err != nil {
		bodyLen := min(len(body), int(^uint32(0)))
		return transport.Frame{
			Header: transport.NewFrameHeader(protocol.Version1, protocol.CmdErrorResponse, uint32(bodyLen)), // #nosec G115 -- bodyLen is clamped to max uint32 before conversion.
			Body:   body,
		}
	}
	return frame
}

func errorFromErr(err error) transport.Frame {
	return errorFrame(string(store.ErrorCode(err)), err.Error())
}

func topicConfigFromProtocol(req protocol.CreateTopicRequest) store.TopicConfig {
	topic := store.NewTopicConfig(req.Topic)
	topic.Partitions = req.Partitions
	topic.RetentionMS = req.RetentionMS
	topic.DeadLetterTopic = req.DeadLetterTopic
	topic.CompactionTombstoneRetentionMS = req.CompactionTombstoneRetentionMS
	applyTopicLimitOptions(&topic, req)
	applyTopicPolicyOptions(&topic, req)
	return topic
}

func applyTopicLimitOptions(topic *store.TopicConfig, req protocol.CreateTopicRequest) {
	if req.RetentionMaxBytes != nil {
		topic.RetentionMaxBytes = *req.RetentionMaxBytes
	}
	if req.MaxMessageBytes != nil {
		topic.MaxMessageBytes = *req.MaxMessageBytes
	}
	if req.MaxBatchBytes != nil {
		topic.MaxBatchBytes = *req.MaxBatchBytes
	}
}

func applyTopicPolicyOptions(topic *store.TopicConfig, req protocol.CreateTopicRequest) {
	if req.CleanupPolicy != nil {
		topic.CleanupPolicy = store.TopicCleanupPolicy(*req.CleanupPolicy)
	}
	if req.RetryPolicy != nil {
		topic.RetryPolicy = store.TopicRetryPolicy{
			MaxAttempts:      req.RetryPolicy.MaxAttempts,
			BackoffInitialMS: req.RetryPolicy.BackoffInitialMS,
			BackoffMaxMS:     req.RetryPolicy.BackoffMaxMS,
		}
	}
	if req.DelayEnabled != nil {
		topic.DelayEnabled = *req.DelayEnabled
	}
	if req.CompactionEnabled != nil {
		topic.CompactionEnabled = *req.CompactionEnabled
	}
}

func partitioningFromProtocol(mode protocol.ProducePartitioning, partition *uint32) PublishPartitioning {
	switch mode {
	case protocol.ProducePartitioningExplicit:
		return explicitPartitioning(partition)
	case protocol.ProducePartitioningKeyHash:
		return PublishPartitioning{Mode: PartitionKeyHash}
	case protocol.ProducePartitioningRoundRobin:
		return PublishPartitioning{Mode: PartitionRoundRobin}
	default:
		return PublishPartitioning{Mode: PartitionRoundRobin}
	}
}

func explicitPartitioning(partition *uint32) PublishPartitioning {
	if partition == nil {
		return PublishPartitioning{Mode: PartitionExplicit, Partition: 0}
	}
	return PublishPartitioning{Mode: PartitionExplicit, Partition: *partition}
}

func batchRecordsFromProtocol(req protocol.ProduceBatchRequest) ([]store.RecordAppend, error) {
	if len(req.Payloads) == 0 && len(req.Records) == 0 {
		return nil, errors.New("produce_batch requires payloads or records")
	}
	if len(req.Payloads) > 0 && len(req.Records) > 0 {
		return nil, errors.New("produce_batch must provide only one of payloads or records")
	}
	if len(req.Records) > 0 {
		return batchRecordItemsFromProtocol(req.Records), nil
	}
	return batchPayloadsFromProtocol(req.Payloads), nil
}

func batchRecordItemsFromProtocol(records []protocol.ProduceBatchRecord) []store.RecordAppend {
	out := make([]store.RecordAppend, 0, len(records))
	for _, record := range records {
		out = append(out, recordItemFromProtocol(record))
	}
	return out
}

func recordItemFromProtocol(record protocol.ProduceBatchRecord) store.RecordAppend {
	appendRecord := store.NewRecordAppend(record.Payload)
	appendRecord.Key = append([]byte(nil), record.Key...)
	if record.Tombstone {
		appendRecord.Attributes |= store.RecordAttributeTombstone
	}
	return appendRecord
}

func batchPayloadsFromProtocol(payloads [][]byte) []store.RecordAppend {
	out := make([]store.RecordAppend, 0, len(payloads))
	for _, payload := range payloads {
		out = append(out, store.NewRecordAppend(payload))
	}
	return out
}

func leaseFromStore(member store.ConsumerGroupMember) protocol.ConsumerGroupMemberLease {
	return protocol.ConsumerGroupMemberLease{
		Group:            member.Group,
		MemberID:         member.MemberID,
		Topics:           append([]string(nil), member.Topics...),
		SessionTimeoutMS: member.SessionTimeoutMS,
		JoinedAtMS:       member.JoinedAtMS,
		LastHeartbeatMS:  member.LastHeartbeatMS,
		ExpiresAtMS:      member.ExpiresAtMS(),
	}
}

func assignmentToProtocol(assignment store.ConsumerGroupAssignment) protocol.ConsumerGroupAssignment {
	out := protocol.ConsumerGroupAssignment{
		Group:       assignment.Group,
		Generation:  assignment.Generation,
		UpdatedAtMS: assignment.UpdatedAtMS,
		Assignments: make([]protocol.GroupPartitionAssignment, 0, len(assignment.Assignments)),
	}
	for _, item := range assignment.Assignments {
		out.Assignments = append(out.Assignments, protocol.GroupPartitionAssignment{
			MemberID:  item.MemberID,
			Topic:     item.Topic,
			Partition: item.Partition,
		})
	}
	return out
}

func optionalAssignmentToProtocol(assignment *store.ConsumerGroupAssignment) *protocol.ConsumerGroupAssignment {
	if assignment == nil {
		return nil
	}
	converted := assignmentToProtocol(*assignment)
	return &converted
}

func directMessagesToProtocol(records []direct.InboxRecord) []protocol.DirectMessageRecord {
	out := make([]protocol.DirectMessageRecord, 0, len(records))
	for _, record := range records {
		out = append(out, protocol.DirectMessageRecord{
			Offset:         record.Offset,
			MessageID:      record.Message.MessageID,
			ConversationID: record.Message.ConversationID,
			Sender:         record.Message.Sender,
			Recipient:      record.Message.Recipient,
			TimestampMS:    record.Message.TimestampMS,
			Payload:        record.Message.Payload,
		})
	}
	return out
}
