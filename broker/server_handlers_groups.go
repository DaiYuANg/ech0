package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/transport"
)

func (s *TCPServer) handleSendDirectFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
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
}

func (s *TCPServer) handleFetchInboxFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchInboxRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.FetchInboxFor(ctx, req.Recipient, req.MaxRecords)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdFetchInboxResponse, protocol.FetchInboxResponse{
		Recipient:     result.Recipient,
		Records:       directMessagesToProtocol(result.Records),
		NextOffset:    result.NextOffset,
		HighWatermark: result.HighWatermark,
	})
}

func (s *TCPServer) handleAckDirectFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.AckDirectRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	if err := s.broker.AckDirect(ctx, req.Recipient, req.NextOffset); err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdAckDirectResponse, protocol.AckDirectResponse(req))
}

func (s *TCPServer) handleJoinConsumerGroupFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.JoinConsumerGroupRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	member, err := s.broker.JoinConsumerGroupWithOptions(ctx, req.Group, req.MemberID, req.Topics, ConsumerGroupLeaseOptions{
		SessionTimeoutMS:  req.SessionTimeoutMS,
		MaxPollIntervalMS: req.MaxPollIntervalMS,
	})
	if err != nil {
		return errorFromErr(err), nil
	}
	lease, err := leaseFromStore(member)
	if err != nil {
		return errorFrame("internal_error", err.Error()), nil
	}
	return okFrame(protocol.CmdJoinConsumerGroupResponse, protocol.JoinConsumerGroupResponse{Lease: lease})
}

func (s *TCPServer) handleHeartbeatConsumerGroupFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.HeartbeatConsumerGroupRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	member, err := s.broker.HeartbeatConsumerGroupWithOptions(ctx, req.Group, req.MemberID, req.SessionTimeoutMS, req.MaxPollIntervalMS)
	if err != nil {
		return errorFromErr(err), nil
	}
	lease, err := leaseFromStore(member)
	if err != nil {
		return errorFrame("internal_error", err.Error()), nil
	}
	return okFrame(protocol.CmdHeartbeatConsumerGroupResponse, protocol.HeartbeatConsumerGroupResponse{Lease: lease})
}

func (s *TCPServer) handleRebalanceConsumerGroupFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.RebalanceConsumerGroupRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	assignment, err := s.broker.RebalanceConsumerGroup(ctx, req.Group)
	if err != nil {
		return errorFromErr(err), nil
	}
	converted, err := assignmentToProtocol(assignment)
	if err != nil {
		return errorFrame("internal_error", err.Error()), nil
	}
	response := protocol.RebalanceConsumerGroupResponse{Assignment: converted}
	return okFrame(protocol.CmdRebalanceConsumerGroupResponse, response)
}

func (s *TCPServer) handleGetConsumerGroupAssignmentFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.GetConsumerGroupAssignmentRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	assignment, err := s.broker.GetConsumerGroupAssignmentFor(ctx, req.Group)
	if err != nil {
		return errorFromErr(err), nil
	}
	converted, ok, err := optionalAssignmentToProtocol(assignment)
	if err != nil {
		return errorFrame("internal_error", err.Error()), nil
	}
	var responseAssignment *protocol.ConsumerGroupAssignment
	if ok {
		responseAssignment = &converted
	}
	return okFrame(protocol.CmdGetConsumerGroupAssignmentResponse, protocol.GetConsumerGroupAssignmentResponse{
		Assignment: responseAssignment,
	})
}

func (s *TCPServer) handleFetchConsumerGroupFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchConsumerGroupRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	poll, err := s.broker.FetchConsumerGroupWithIsolation(ctx, req.Group, req.MemberID, req.Generation, req.Topic, req.Partition, req.Offset, req.MaxRecords, isolationFromProtocol(req.Isolation))
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
}

func (s *TCPServer) handleCommitConsumerGroupOffsetFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.CommitConsumerGroupOffsetRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	err := s.broker.CommitConsumerGroupOffsetWithMetadata(ctx, req.Group, req.MemberID, req.Generation, req.Topic, req.Partition, req.NextOffset, req.Metadata)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdCommitConsumerGroupOffsetResponse, protocol.CommitConsumerGroupOffsetResponse(req))
}

func (s *TCPServer) handleFetchConsumerGroupBatchFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchConsumerGroupBatchRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	items, err := s.fetchConsumerGroupBatchItems(ctx, req)
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdFetchConsumerGroupBatchResponse, protocol.FetchConsumerGroupBatchResponse{
		Group:      req.Group,
		MemberID:   req.MemberID,
		Generation: req.Generation,
		Items:      items,
	})
}

func (s *TCPServer) fetchConsumerGroupBatchItems(
	ctx context.Context,
	req protocol.FetchConsumerGroupBatchRequest,
) ([]protocol.FetchConsumerGroupBatchItemResponse, error) {
	items := collectionlist.NewListWithCapacity[protocol.FetchConsumerGroupBatchItemResponse](len(req.Items))
	for _, item := range req.Items {
		poll, err := s.broker.FetchConsumerGroupWithIsolation(ctx, req.Group, req.MemberID, req.Generation, item.Topic, item.Partition, item.Offset, item.MaxRecords, isolationFromProtocol(req.Isolation))
		if err != nil {
			return nil, err
		}
		items.Add(protocol.FetchConsumerGroupBatchItemResponse{
			Topic:         item.Topic,
			Partition:     item.Partition,
			Records:       fetchRecordsFromStore(poll.Records),
			NextOffset:    poll.NextOffset,
			HighWatermark: poll.HighWatermark,
		})
	}
	return items.Values(), nil
}
