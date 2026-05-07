//nolint:revive // TCP server and protocol frame helpers are kept together around the wire boundary.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/DaiYuANg/ech0/transport"
)

type TCPServer struct {
	addr     string
	limits   BrokerConfig
	broker   *Broker
	logger   *slog.Logger
	listener net.Listener
	wg       sync.WaitGroup
}

func NewTCPServer(cfg Config, broker *Broker, logger *slog.Logger) *TCPServer {
	return &TCPServer{
		addr:   cfg.Broker.BindAddr,
		limits: cfg.Broker,
		broker: broker,
		logger: logger,
	}
}

func (s *TCPServer) Start(ctx context.Context) error {
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return wrapBroker("tcp_listen_failed", err, "listen tcp broker")
	}
	s.listener = listener
	if s.logger != nil {
		s.logger.Info("tcp broker listening", "addr", s.addr)
	}
	s.wg.Add(1)
	go s.acceptLoop(ctx)
	return nil
}

func (s *TCPServer) Stop(ctx context.Context) error {
	var closeErr error
	if s.listener != nil {
		closeErr = s.listener.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return errors.Join(
			wrapBroker("tcp_stop_context_done", ctx.Err(), "stop tcp server"),
			wrapBroker("tcp_listener_close_failed", closeErr, "close tcp listener"),
		)
	case <-done:
		return wrapBroker("tcp_listener_close_failed", closeErr, "close tcp listener")
	}
}

func (s *TCPServer) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return
			}
			if s.logger != nil {
				s.logger.Warn("accept failed", "error", err)
			}
			continue
		}
		s.wg.Add(1)
		go s.handleConn(ctx, conn)
	}
}

//nolint:gocognit // Connection handling keeps read, dispatch, write, and close behavior in one loop.
func (s *TCPServer) handleConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		if err := conn.Close(); err != nil && s.logger != nil {
			s.logger.Debug("close connection failed", "error", err)
		}
	}()
	for {
		frame, err := transport.ReadFrameWithLimit(conn, s.limits.MaxFrameBodyBytes)
		if err != nil {
			if !errors.Is(err, io.EOF) && s.logger != nil {
				s.logger.Debug("read frame failed", "error", err)
			}
			return
		}
		response, err := s.HandleFrame(ctx, frame)
		if err != nil {
			response = errorFrame("internal_error", err.Error())
		}
		if err := transport.WriteFrame(conn, response); err != nil {
			if s.logger != nil {
				s.logger.Debug("write frame failed", "error", err)
			}
			return
		}
	}
}

//nolint:cyclop,gocyclo,gocognit,funlen // Protocol dispatch enumerates all supported wire commands explicitly.
func (s *TCPServer) HandleFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	if frame.Header.Version != protocol.Version1 {
		return errorFrame("unsupported_version", fmt.Sprintf("unsupported protocol version %d", frame.Header.Version)), nil
	}
	switch frame.Header.Command {
	case protocol.CmdHandshakeRequest:
		var req protocol.HandshakeRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		_ = req
		return okFrame(protocol.CmdHandshakeResponse, protocol.HandshakeResponse{
			ServerID:        fmt.Sprintf("%s-node-%d", s.broker.cfg.Broker.ClusterName, s.broker.cfg.Broker.NodeID),
			ProtocolVersion: protocol.Version1,
		})
	case protocol.CmdPingRequest:
		var req protocol.PingRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		return okFrame(protocol.CmdPingResponse, protocol.PingResponse(req))
	case protocol.CmdCreateTopicRequest:
		var req protocol.CreateTopicRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		topic := topicConfigFromProtocol(req)
		created, err := s.broker.CreateTopic(ctx, topic)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdCreateTopicResponse, protocol.CreateTopicResponse{Topic: created.Name, Partitions: created.Partitions})
	case protocol.CmdListTopicsRequest:
		topics, err := s.broker.ListTopics()
		if err != nil {
			return errorFromErr(err), nil
		}
		out := make([]protocol.TopicMetadata, 0, len(topics))
		for i := range topics {
			topic := topics[i]
			out = append(out, protocol.TopicMetadata{Topic: topic.Name, Partitions: topic.Partitions})
		}
		return okFrame(protocol.CmdListTopicsResponse, protocol.ListTopicsResponse{Topics: out})
	case protocol.CmdProduceRequest:
		var req protocol.ProduceRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		if len(req.Payload) > s.limits.MaxPayloadBytes {
			return errorFrame("payload_too_large", fmt.Sprintf("produce payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)), nil
		}
		result, err := s.broker.Publish(ctx, req.Topic, partitioningFromProtocol(req.Partitioning, req.Partition), req.Key, req.Tombstone, req.Payload)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdProduceResponse, protocol.ProduceResponse{
			Partition:  result.Partition,
			Offset:     result.Record.Offset,
			NextOffset: result.Record.Offset + 1,
		})
	case protocol.CmdProduceBatchRequest:
		var req protocol.ProduceBatchRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		records, err := batchRecordsFromProtocol(req)
		if err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		result, err := s.broker.PublishBatch(ctx, req.Topic, partitioningFromProtocol(req.Partitioning, req.Partition), records)
		if err != nil {
			return errorFromErr(err), nil
		}
		if len(result.Records) == 0 {
			return errorFrame("invalid_request", "produce_batch appended no records"), nil
		}
		return okFrame(protocol.CmdProduceBatchResponse, protocol.ProduceBatchResponse{
			Partition:  result.Partition,
			BaseOffset: result.Records[0].Offset,
			LastOffset: result.Records[len(result.Records)-1].Offset,
			NextOffset: result.Records[len(result.Records)-1].Offset + 1,
			Appended:   len(result.Records),
		})
	case protocol.CmdFetchRequest:
		var req protocol.FetchRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		poll, err := s.broker.Fetch(req.Consumer, req.Topic, req.Partition, req.Offset, req.MaxRecords)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdFetchResponse, protocol.FetchResponse{
			Topic:         req.Topic,
			Partition:     req.Partition,
			Records:       fetchRecordsFromStore(poll.Records),
			NextOffset:    poll.NextOffset,
			HighWatermark: poll.HighWatermark,
		})
	case protocol.CmdFetchBatchRequest:
		var req protocol.FetchBatchRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		items := make([]protocol.FetchBatchItemResponse, 0, len(req.Items))
		for _, item := range req.Items {
			poll, err := s.broker.Fetch(req.Consumer, item.Topic, item.Partition, item.Offset, item.MaxRecords)
			if err != nil {
				return errorFromErr(err), nil
			}
			items = append(items, protocol.FetchBatchItemResponse{
				Topic:         item.Topic,
				Partition:     item.Partition,
				Records:       fetchRecordsFromStore(poll.Records),
				NextOffset:    poll.NextOffset,
				HighWatermark: poll.HighWatermark,
			})
		}
		return okFrame(protocol.CmdFetchBatchResponse, protocol.FetchBatchResponse{Items: items})
	case protocol.CmdCommitOffsetRequest:
		var req protocol.CommitOffsetRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		if err := s.broker.CommitOffset(ctx, req.Consumer, req.Topic, req.Partition, req.NextOffset); err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdCommitOffsetResponse, protocol.CommitOffsetResponse(req))
	case protocol.CmdNackRequest:
		var req protocol.NackRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		result, err := s.broker.Nack(ctx, req.Consumer, req.Topic, req.Partition, req.Offset, req.LastError)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdNackResponse, protocol.NackResponse{
			RetryTopic:      result.RetryTopic,
			RetryPartition:  result.RetryPartition,
			RetryOffset:     result.RetryOffset,
			RetryNextOffset: result.RetryNextOffset,
			RetryCount:      result.RetryCount,
		})
	case protocol.CmdProcessRetryRequest:
		var req protocol.ProcessRetryRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		result, err := s.broker.ProcessRetryBatch(ctx, req.Consumer, req.SourceTopic, req.Partition, req.MaxRecords)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdProcessRetryResponse, protocol.ProcessRetryResponse{
			RetryTopic:          result.RetryTopic,
			Partition:           result.Partition,
			MovedToOrigin:       result.MovedToOrigin,
			MovedToDeadLetter:   result.MovedToDeadLetter,
			CommittedNextOffset: result.CommittedNextOffset,
		})
	case protocol.CmdScheduleDelayRequest:
		var req protocol.ScheduleDelayRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		if len(req.Payload) > s.limits.MaxPayloadBytes {
			return errorFrame("payload_too_large", fmt.Sprintf("schedule_delay payload size %d exceeds limit %d", len(req.Payload), s.limits.MaxPayloadBytes)), nil
		}
		result, err := s.broker.ScheduleDelay(ctx, req.Topic, req.Partition, req.Payload, req.DeliverAtMS)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdScheduleDelayResponse, protocol.ScheduleDelayResponse{
			DelayTopic:  result.DelayTopic,
			Partition:   result.Partition,
			Offset:      result.Offset,
			NextOffset:  result.NextOffset,
			DeliverAtMS: result.DeliverAtMS,
		})
	case protocol.CmdSendDirectRequest:
		var req protocol.SendDirectRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		result, err := s.broker.SendDirect(ctx, req.Sender, req.Recipient, req.ConversationID, req.Payload)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdSendDirectResponse, protocol.SendDirectResponse{
			MessageID:      result.MessageID,
			ConversationID: result.ConversationID,
			Offset:         result.Offset,
			NextOffset:     result.NextOffset,
		})
	case protocol.CmdFetchInboxRequest:
		var req protocol.FetchInboxRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		result, err := s.broker.FetchInbox(req.Recipient, req.MaxRecords)
		if err != nil {
			return errorFromErr(err), nil
		}
		records := make([]protocol.DirectMessageRecord, 0, len(result.Records))
		for _, record := range result.Records {
			records = append(records, protocol.DirectMessageRecord{
				Offset:         record.Offset,
				MessageID:      record.Message.MessageID,
				ConversationID: record.Message.ConversationID,
				Sender:         record.Message.Sender,
				Recipient:      record.Message.Recipient,
				TimestampMS:    record.Message.TimestampMS,
				Payload:        record.Message.Payload,
			})
		}
		return okFrame(protocol.CmdFetchInboxResponse, protocol.FetchInboxResponse{
			Recipient:     result.Recipient,
			Records:       records,
			NextOffset:    result.NextOffset,
			HighWatermark: result.HighWatermark,
		})
	case protocol.CmdAckDirectRequest:
		var req protocol.AckDirectRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		if err := s.broker.AckDirect(ctx, req.Recipient, req.NextOffset); err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdAckDirectResponse, protocol.AckDirectResponse(req))
	case protocol.CmdJoinConsumerGroupRequest:
		var req protocol.JoinConsumerGroupRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		member, err := s.broker.JoinConsumerGroup(ctx, req.Group, req.MemberID, req.Topics, req.SessionTimeoutMS)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdJoinConsumerGroupResponse, protocol.JoinConsumerGroupResponse{Lease: leaseFromStore(member)})
	case protocol.CmdHeartbeatConsumerGroupRequest:
		var req protocol.HeartbeatConsumerGroupRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		member, err := s.broker.HeartbeatConsumerGroup(ctx, req.Group, req.MemberID, req.SessionTimeoutMS)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdHeartbeatConsumerGroupResponse, protocol.HeartbeatConsumerGroupResponse{Lease: leaseFromStore(member)})
	case protocol.CmdRebalanceConsumerGroupRequest:
		var req protocol.RebalanceConsumerGroupRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		assignment, err := s.broker.RebalanceConsumerGroup(ctx, req.Group)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdRebalanceConsumerGroupResponse, protocol.RebalanceConsumerGroupResponse{Assignment: assignmentToProtocol(assignment)})
	case protocol.CmdGetConsumerGroupAssignmentRequest:
		var req protocol.GetConsumerGroupAssignmentRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		assignment, err := s.broker.GetConsumerGroupAssignment(req.Group)
		if err != nil {
			return errorFromErr(err), nil
		}
		var out *protocol.ConsumerGroupAssignment
		if assignment != nil {
			converted := assignmentToProtocol(*assignment)
			out = &converted
		}
		return okFrame(protocol.CmdGetConsumerGroupAssignmentResponse, protocol.GetConsumerGroupAssignmentResponse{Assignment: out})
	case protocol.CmdFetchConsumerGroupRequest:
		var req protocol.FetchConsumerGroupRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		poll, err := s.broker.FetchConsumerGroup(req.Group, req.MemberID, req.Generation, req.Topic, req.Partition, req.Offset, req.MaxRecords)
		if err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdFetchConsumerGroupResponse, protocol.FetchConsumerGroupResponse{
			Group:         req.Group,
			MemberID:      req.MemberID,
			Generation:    req.Generation,
			Topic:         req.Topic,
			Partition:     req.Partition,
			Records:       fetchRecordsFromStore(poll.Records),
			NextOffset:    poll.NextOffset,
			HighWatermark: poll.HighWatermark,
		})
	case protocol.CmdCommitConsumerGroupOffsetRequest:
		var req protocol.CommitConsumerGroupOffsetRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		if err := s.broker.CommitConsumerGroupOffset(ctx, req.Group, req.MemberID, req.Generation, req.Topic, req.Partition, req.NextOffset); err != nil {
			return errorFromErr(err), nil
		}
		return okFrame(protocol.CmdCommitConsumerGroupOffsetResponse, protocol.CommitConsumerGroupOffsetResponse(req))
	case protocol.CmdFetchConsumerGroupBatchRequest:
		var req protocol.FetchConsumerGroupBatchRequest
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
		items := make([]protocol.FetchConsumerGroupBatchItemResponse, 0, len(req.Items))
		for _, item := range req.Items {
			poll, err := s.broker.FetchConsumerGroup(req.Group, req.MemberID, req.Generation, item.Topic, item.Partition, item.Offset, item.MaxRecords)
			if err != nil {
				return errorFromErr(err), nil
			}
			items = append(items, protocol.FetchConsumerGroupBatchItemResponse{
				Topic:         item.Topic,
				Partition:     item.Partition,
				Records:       fetchRecordsFromStore(poll.Records),
				NextOffset:    poll.NextOffset,
				HighWatermark: poll.HighWatermark,
			})
		}
		return okFrame(protocol.CmdFetchConsumerGroupBatchResponse, protocol.FetchConsumerGroupBatchResponse{
			Group:      req.Group,
			MemberID:   req.MemberID,
			Generation: req.Generation,
			Items:      items,
		})
	default:
		return errorFrame("unsupported_command", fmt.Sprintf("unsupported command %d", frame.Header.Command)), nil
	}
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
	if req.RetentionMaxBytes != nil {
		topic.RetentionMaxBytes = *req.RetentionMaxBytes
	}
	if req.CleanupPolicy != nil {
		topic.CleanupPolicy = store.TopicCleanupPolicy(*req.CleanupPolicy)
	}
	if req.MaxMessageBytes != nil {
		topic.MaxMessageBytes = *req.MaxMessageBytes
	}
	if req.MaxBatchBytes != nil {
		topic.MaxBatchBytes = *req.MaxBatchBytes
	}
	topic.RetentionMS = req.RetentionMS
	if req.RetryPolicy != nil {
		topic.RetryPolicy = store.TopicRetryPolicy{
			MaxAttempts:      req.RetryPolicy.MaxAttempts,
			BackoffInitialMS: req.RetryPolicy.BackoffInitialMS,
			BackoffMaxMS:     req.RetryPolicy.BackoffMaxMS,
		}
	}
	topic.DeadLetterTopic = req.DeadLetterTopic
	if req.DelayEnabled != nil {
		topic.DelayEnabled = *req.DelayEnabled
	}
	if req.CompactionEnabled != nil {
		topic.CompactionEnabled = *req.CompactionEnabled
	}
	topic.CompactionTombstoneRetentionMS = req.CompactionTombstoneRetentionMS
	return topic
}

func partitioningFromProtocol(mode protocol.ProducePartitioning, partition *uint32) PublishPartitioning {
	switch mode {
	case protocol.ProducePartitioningExplicit:
		if partition == nil {
			return PublishPartitioning{Mode: PartitionExplicit, Partition: 0}
		}
		return PublishPartitioning{Mode: PartitionExplicit, Partition: *partition}
	case protocol.ProducePartitioningKeyHash:
		return PublishPartitioning{Mode: PartitionKeyHash}
	case protocol.ProducePartitioningRoundRobin:
		return PublishPartitioning{Mode: PartitionRoundRobin}
	default:
		return PublishPartitioning{Mode: PartitionRoundRobin}
	}
}

//nolint:gocognit // Batch decoding validates mutually exclusive protocol shapes in one boundary helper.
func batchRecordsFromProtocol(req protocol.ProduceBatchRequest) ([]store.RecordAppend, error) {
	if len(req.Payloads) == 0 && len(req.Records) == 0 {
		return nil, errors.New("produce_batch requires payloads or records")
	}
	if len(req.Payloads) > 0 && len(req.Records) > 0 {
		return nil, errors.New("produce_batch must provide only one of payloads or records")
	}
	if len(req.Records) > 0 {
		out := make([]store.RecordAppend, 0, len(req.Records))
		for _, record := range req.Records {
			appendRecord := store.NewRecordAppend(record.Payload)
			appendRecord.Key = append([]byte(nil), record.Key...)
			if record.Tombstone {
				appendRecord.Attributes |= store.RecordAttributeTombstone
			}
			out = append(out, appendRecord)
		}
		return out, nil
	}
	out := make([]store.RecordAppend, 0, len(req.Payloads))
	for _, payload := range req.Payloads {
		out = append(out, store.NewRecordAppend(payload))
	}
	return out, nil
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
