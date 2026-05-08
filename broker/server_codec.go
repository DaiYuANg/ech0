package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/DaiYuANg/ech0/transport"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/mapper"
)

var controlPlaneMapper = mapper.New()

func (s *TCPServer) recordCommandError(ctx context.Context, frame transport.Frame) {
	if s == nil || s.metrics == nil || frame.Header.Command != protocol.CmdErrorResponse {
		return
	}
	var out protocol.ErrorResponse
	if err := protocol.DecodeBody(frame.Header.Command, frame.Body, &out); err != nil || out.Code == "" {
		s.metrics.RecordCommandError(ctx, "internal_error")
		return
	}
	s.metrics.RecordCommandError(ctx, out.Code)
}

func decode(frame transport.Frame, target any) error {
	return wrapBroker("frame_decode_failed", protocol.DecodeBody(frame.Header.Command, frame.Body, target), "decode request frame")
}

func okFrame(command uint16, value any) (transport.Frame, error) {
	body, err := protocol.EncodeBody(command, value)
	if err != nil {
		return transport.Frame{}, wrapBroker("response_encode_failed", err, "encode response frame")
	}
	frame, err := transport.NewFrame(protocol.Version, command, body)
	if err != nil {
		return transport.Frame{}, wrapBroker("response_frame_create_failed", err, "create response frame")
	}
	return frame, nil
}

func errorFrame(code, message string) transport.Frame {
	body, err := protocol.EncodeBody(protocol.CmdErrorResponse, protocol.ErrorResponse{Code: code, Message: message})
	if err != nil {
		body = nil
	}
	frame, err := transport.NewFrame(protocol.Version, protocol.CmdErrorResponse, body)
	if err != nil {
		bodyLen := min(len(body), int(^uint32(0)))
		return transport.Frame{
			Header: errorFrameHeader(uint32(bodyLen)), // #nosec G115 -- bodyLen is clamped to max uint32 before conversion.
			Body:   body,
		}
	}
	frame.Header.Status = transport.StatusError
	return frame
}

func errorFrameHeader(bodyLen uint32) transport.FrameHeader {
	header := transport.NewFrameHeader(protocol.Version, protocol.CmdErrorResponse, bodyLen)
	header.Status = transport.StatusError
	return header
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
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for _, record := range records {
		out.Add(recordItemFromProtocol(record))
	}
	return out.Values()
}

func recordItemFromProtocol(record protocol.ProduceBatchRecord) store.RecordAppend {
	appendRecord := store.NewRecordAppend(record.Payload)
	appendRecord.Key = append([]byte(nil), record.Key...)
	appendRecord.Headers = storeHeadersFromProtocol(record.Headers)
	if record.Tombstone {
		appendRecord.Attributes |= store.RecordAttributeTombstone
	}
	return appendRecord
}

func batchPayloadsFromProtocol(payloads [][]byte) []store.RecordAppend {
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(payloads))
	for _, payload := range payloads {
		out.Add(store.NewRecordAppend(payload))
	}
	return out.Values()
}

func leaseFromStore(member store.ConsumerGroupMember) (protocol.ConsumerGroupMemberLease, error) {
	var out protocol.ConsumerGroupMemberLease
	if err := controlPlaneMapper.MapInto(&out, member); err != nil {
		return protocol.ConsumerGroupMemberLease{}, fmt.Errorf("map consumer group member lease: %w", err)
	}
	out.ExpiresAtMS = member.ExpiresAtMS()
	return out, nil
}

func assignmentToProtocol(assignment store.ConsumerGroupAssignment) (protocol.ConsumerGroupAssignment, error) {
	var out protocol.ConsumerGroupAssignment
	if err := controlPlaneMapper.MapInto(&out, assignment); err != nil {
		return protocol.ConsumerGroupAssignment{}, fmt.Errorf("map consumer group assignment: %w", err)
	}
	return out, nil
}

func optionalAssignmentToProtocol(assignment *store.ConsumerGroupAssignment) (protocol.ConsumerGroupAssignment, bool, error) {
	if assignment == nil {
		return protocol.ConsumerGroupAssignment{}, false, nil
	}
	converted, err := assignmentToProtocol(*assignment)
	if err != nil {
		return protocol.ConsumerGroupAssignment{}, false, err
	}
	return converted, true, nil
}

func directMessagesToProtocol(records []direct.InboxRecord) []protocol.DirectMessageRecord {
	out := collectionlist.NewListWithCapacity[protocol.DirectMessageRecord](len(records))
	for _, record := range records {
		out.Add(protocol.DirectMessageRecord{
			Offset:         record.Offset,
			MessageID:      record.Message.MessageID,
			ConversationID: record.Message.ConversationID,
			Sender:         record.Message.Sender,
			Recipient:      record.Message.Recipient,
			TimestampMS:    record.Message.TimestampMS,
			Payload:        record.Message.Payload,
		})
	}
	return out.Values()
}
