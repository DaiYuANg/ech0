package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/transport"
)

func (s *TCPServer) handleListTopicsFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.ListTopicsRequest
	if len(frame.Body) > 0 {
		if err := decode(frame, &req); err != nil {
			return errorFrame("invalid_request", err.Error()), nil
		}
	}
	topics, err := s.broker.ListTopicsByPatternFor(ctx, req.Pattern)
	if err != nil {
		return errorFromErr(err), nil
	}
	out := collectionlist.NewListWithCapacity[protocol.TopicMetadata](len(topics))
	for i := range topics {
		topic := topics[i]
		out.Add(protocol.TopicMetadata{Topic: topic.Name, Partitions: topic.Partitions})
	}
	return okFrame(protocol.CmdListTopicsResponse, protocol.ListTopicsResponse{Topics: out.Values()})
}
