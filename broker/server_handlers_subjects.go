package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/transport"
)

func (s *TCPServer) handleFetchSubjectPatternFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	var req protocol.FetchSubjectPatternRequest
	if err := decode(frame, &req); err != nil {
		return errorFrame("invalid_request", err.Error()), nil
	}
	result, err := s.broker.FetchSubjectPatternWithIsolation(ctx, req.Consumer, req.Pattern, req.MaxRecords, isolationFromProtocol(req.Isolation))
	if err != nil {
		return errorFromErr(err), nil
	}
	return okFrame(protocol.CmdFetchSubjectPatternResponse, subjectPatternResultToProtocol(result))
}

func subjectPatternResultToProtocol(result SubjectPatternPollResult) protocol.FetchSubjectPatternResponse {
	items := collectionlist.NewListWithCapacity[protocol.FetchSubjectPatternItemResponse](len(result.Items))
	for index := range result.Items {
		item := &result.Items[index]
		items.Add(protocol.FetchSubjectPatternItemResponse{
			Topic:          item.Subject,
			Partition:      item.Partition,
			Records:        fetchRecordsFromStore(item.Poll.Records),
			NextOffset:     item.Poll.NextOffset,
			HighWatermark:  item.Poll.HighWatermark,
			LowWatermark:   item.Poll.LowWatermark,
			LogStartOffset: item.Poll.LogStartOffset,
		})
	}
	return protocol.FetchSubjectPatternResponse{Pattern: result.Pattern, Items: items.Values()}
}
