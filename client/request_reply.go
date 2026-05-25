package client

import (
	"context"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
)

type RequestReply struct {
	client     *Client
	instanceID string
}

type RequestOption func(*protocol.StartRequestRequest)

func (c *Client) RequestReply(instanceID string) *RequestReply {
	return &RequestReply{client: c, instanceID: instanceID}
}

func (r *RequestReply) Start(
	ctx context.Context,
	subject string,
	payload []byte,
	opts ...RequestOption,
) (protocol.StartRequestResponse, error) {
	req := protocol.StartRequestRequest{
		Subject:      subject,
		InstanceID:   r.instanceID,
		Partitioning: protocol.ProducePartitioningRoundRobin,
		Payload:      payload,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	var out protocol.StartRequestResponse
	err := r.client.RoundTrip(ctx, protocol.CmdStartRequestRequest, protocol.CmdStartRequestResponse, req, &out)
	return out, err
}

func (r *RequestReply) Request(
	ctx context.Context,
	subject string,
	payload []byte,
	opts ...RequestOption,
) (protocol.ReplyRecord, error) {
	started, err := r.Start(ctx, subject, payload, opts...)
	if err != nil {
		return protocol.ReplyRecord{}, err
	}
	reply, err := r.AwaitReply(ctx, started)
	return reply.Reply, err
}

func (r *RequestReply) AwaitReply(
	ctx context.Context,
	started protocol.StartRequestResponse,
) (protocol.AwaitReplyResponse, error) {
	req := protocol.AwaitReplyRequest{
		ReplyTo:       started.ReplyTo,
		CorrelationID: started.CorrelationID,
		ExpiresAtMS:   started.ExpiresAtMS,
	}
	var out protocol.AwaitReplyResponse
	err := r.client.RoundTrip(ctx, protocol.CmdAwaitReplyRequest, protocol.CmdAwaitReplyResponse, req, &out)
	return out, err
}

func (r *RequestReply) AwaitReplies(
	ctx context.Context,
	started protocol.StartRequestResponse,
	maxReplies int,
) (protocol.AwaitRepliesResponse, error) {
	req := protocol.AwaitRepliesRequest{
		ReplyTo:       started.ReplyTo,
		CorrelationID: started.CorrelationID,
		ExpiresAtMS:   started.ExpiresAtMS,
		MaxReplies:    maxReplies,
	}
	var out protocol.AwaitRepliesResponse
	err := r.client.RoundTrip(ctx, protocol.CmdAwaitRepliesRequest, protocol.CmdAwaitRepliesResponse, req, &out)
	return out, err
}

func (r *RequestReply) FetchRequests(
	ctx context.Context,
	consumer string,
	subject string,
	partition uint32,
	maxRecords int,
) (protocol.FetchRequestsResponse, error) {
	req := protocol.FetchRequestsRequest{Consumer: consumer, Subject: subject, Partition: partition, MaxRecords: maxRecords}
	var out protocol.FetchRequestsResponse
	err := r.client.RoundTrip(ctx, protocol.CmdFetchRequestsRequest, protocol.CmdFetchRequestsResponse, req, &out)
	return out, err
}

func (r *RequestReply) Reply(
	ctx context.Context,
	request protocol.RequestRecord,
	payload []byte,
) (protocol.ReplyResponse, error) {
	req := protocol.ReplyRequest{
		Subject:       request.Subject,
		ReplyTo:       request.ReplyTo,
		CorrelationID: request.CorrelationID,
		ExpiresAtMS:   request.ExpiresAtMS,
		ResponderID:   r.instanceID,
		Payload:       payload,
	}
	var out protocol.ReplyResponse
	err := r.client.RoundTrip(ctx, protocol.CmdReplyRequest, protocol.CmdReplyResponse, req, &out)
	return out, err
}

func (r *RequestReply) ReplyError(
	ctx context.Context,
	request protocol.RequestRecord,
	message string,
) (protocol.ReplyErrorResponse, error) {
	req := protocol.ReplyErrorRequest{
		Subject:       request.Subject,
		ReplyTo:       request.ReplyTo,
		CorrelationID: request.CorrelationID,
		ExpiresAtMS:   request.ExpiresAtMS,
		ResponderID:   r.instanceID,
		Error:         message,
	}
	var out protocol.ReplyErrorResponse
	err := r.client.RoundTrip(ctx, protocol.CmdReplyErrorRequest, protocol.CmdReplyErrorResponse, req, &out)
	return out, err
}

func RequestTimeout(timeout time.Duration) RequestOption {
	return func(req *protocol.StartRequestRequest) {
		if timeout > 0 {
			ms := durationMillis(timeout)
			req.TimeoutMS = &ms
		}
	}
}

func RequestPollInterval(interval time.Duration) RequestOption {
	return func(req *protocol.StartRequestRequest) {
		if interval > 0 {
			ms := durationMillis(interval)
			req.PollIntervalMS = &ms
		}
	}
}

func RequestPartition(partition uint32) RequestOption {
	return func(req *protocol.StartRequestRequest) {
		req.Partition = clonePartition(&partition)
		req.Partitioning = protocol.ProducePartitioningExplicit
	}
}

func RequestMode(mode protocol.RequestReplyMode) RequestOption {
	return func(req *protocol.StartRequestRequest) {
		req.ReplyMode = mode
	}
}

func RequestHeaders(headers ...protocol.MessageHeader) RequestOption {
	return func(req *protocol.StartRequestRequest) {
		req.Headers = cloneHeaders(headers)
	}
}
