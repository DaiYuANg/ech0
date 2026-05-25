package client

import (
	"context"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
)

type Admin struct {
	client *Client
}

type TopicOption func(*protocol.CreateTopicRequest)

func (c *Client) Admin() *Admin {
	return &Admin{client: c}
}

func (a *Admin) CreateTopic(ctx context.Context, topic string, opts ...TopicOption) (protocol.CreateTopicResponse, error) {
	req := protocol.CreateTopicRequest{Topic: topic, Partitions: 1}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	return a.CreateTopicRequest(ctx, req)
}

func (a *Admin) CreateTopicRequest(ctx context.Context, req protocol.CreateTopicRequest) (protocol.CreateTopicResponse, error) {
	var out protocol.CreateTopicResponse
	err := a.client.RoundTrip(ctx, protocol.CmdCreateTopicRequest, protocol.CmdCreateTopicResponse, req, &out)
	return out, err
}

func (a *Admin) ListTopics(ctx context.Context, pattern string) (protocol.ListTopicsResponse, error) {
	var out protocol.ListTopicsResponse
	err := a.client.RoundTrip(
		ctx,
		protocol.CmdListTopicsRequest,
		protocol.CmdListTopicsResponse,
		protocol.ListTopicsRequest{Pattern: pattern},
		&out,
	)
	return out, err
}

func TopicPartitions(partitions uint32) TopicOption {
	return func(req *protocol.CreateTopicRequest) {
		if partitions > 0 {
			req.Partitions = partitions
		}
	}
}

func TopicRetention(duration time.Duration) TopicOption {
	return func(req *protocol.CreateTopicRequest) {
		if duration > 0 {
			ms := durationMillis(duration)
			req.RetentionMS = &ms
		}
	}
}

func TopicCleanupPolicy(policy protocol.TopicCleanupPolicy) TopicOption {
	return func(req *protocol.CreateTopicRequest) {
		req.CleanupPolicy = &policy
	}
}

func TopicRetry(policy protocol.TopicRetryPolicy) TopicOption {
	return func(req *protocol.CreateTopicRequest) {
		req.RetryPolicy = &policy
	}
}

func TopicMessageTTL(duration time.Duration, action protocol.MessageExpiryAction) TopicOption {
	return func(req *protocol.CreateTopicRequest) {
		if duration > 0 {
			ms := durationMillis(duration)
			req.MessageTTLMS = &ms
			req.MessageExpiryAction = &action
		}
	}
}

func TopicOrdering(policy protocol.TopicOrderingPolicy) TopicOption {
	return func(req *protocol.CreateTopicRequest) {
		req.OrderingPolicy = &policy
	}
}

func TopicPriority(policy protocol.TopicPriorityPolicy) TopicOption {
	return func(req *protocol.CreateTopicRequest) {
		req.PriorityPolicy = &policy
	}
}
