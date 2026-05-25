package client

import (
	"context"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
)

type ConsumerGroup struct {
	client     *Client
	group      string
	memberID   string
	generation uint64
}

type JoinGroupOption func(*protocol.JoinConsumerGroupRequest)

func (c *Client) JoinConsumerGroup(
	ctx context.Context,
	group string,
	memberID string,
	topics []string,
	opts ...JoinGroupOption,
) (*ConsumerGroup, protocol.JoinConsumerGroupResponse, error) {
	req := protocol.JoinConsumerGroupRequest{Group: group, MemberID: memberID, Topics: topics}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	var out protocol.JoinConsumerGroupResponse
	err := c.RoundTrip(ctx, protocol.CmdJoinConsumerGroupRequest, protocol.CmdJoinConsumerGroupResponse, req, &out)
	if err != nil {
		return nil, out, err
	}
	return &ConsumerGroup{client: c, group: group, memberID: memberID}, out, nil
}

func (g *ConsumerGroup) Heartbeat(ctx context.Context) (protocol.HeartbeatConsumerGroupResponse, error) {
	req := protocol.HeartbeatConsumerGroupRequest{Group: g.group, MemberID: g.memberID}
	var out protocol.HeartbeatConsumerGroupResponse
	err := g.client.RoundTrip(ctx, protocol.CmdHeartbeatConsumerGroupRequest, protocol.CmdHeartbeatConsumerGroupResponse, req, &out)
	return out, err
}

func (g *ConsumerGroup) Rebalance(ctx context.Context) (protocol.RebalanceConsumerGroupResponse, error) {
	var out protocol.RebalanceConsumerGroupResponse
	err := g.client.RoundTrip(ctx, protocol.CmdRebalanceConsumerGroupRequest, protocol.CmdRebalanceConsumerGroupResponse, protocol.RebalanceConsumerGroupRequest{Group: g.group}, &out)
	if err == nil {
		g.generation = out.Assignment.Generation
	}
	return out, err
}

func (g *ConsumerGroup) Fetch(
	ctx context.Context,
	topic string,
	partition uint32,
	maxRecords int,
) (protocol.FetchConsumerGroupResponse, error) {
	req := protocol.FetchConsumerGroupRequest{
		Group:      g.group,
		MemberID:   g.memberID,
		Generation: g.generation,
		Topic:      topic,
		Partition:  partition,
		MaxRecords: maxRecords,
	}
	var out protocol.FetchConsumerGroupResponse
	err := g.client.RoundTrip(ctx, protocol.CmdFetchConsumerGroupRequest, protocol.CmdFetchConsumerGroupResponse, req, &out)
	return out, err
}

func (g *ConsumerGroup) Commit(
	ctx context.Context,
	topic string,
	partition uint32,
	nextOffset uint64,
	metadata string,
) (protocol.CommitConsumerGroupOffsetResponse, error) {
	req := protocol.CommitConsumerGroupOffsetRequest{
		Group:      g.group,
		MemberID:   g.memberID,
		Generation: g.generation,
		Topic:      topic,
		Partition:  partition,
		NextOffset: nextOffset,
		Metadata:   metadata,
	}
	var out protocol.CommitConsumerGroupOffsetResponse
	err := g.client.RoundTrip(ctx, protocol.CmdCommitConsumerGroupOffsetRequest, protocol.CmdCommitConsumerGroupOffsetResponse, req, &out)
	return out, err
}

func GroupSessionTimeout(timeout time.Duration) JoinGroupOption {
	return func(req *protocol.JoinConsumerGroupRequest) {
		req.SessionTimeoutMS = durationMillis(timeout)
	}
}

func GroupMaxPollInterval(interval time.Duration) JoinGroupOption {
	return func(req *protocol.JoinConsumerGroupRequest) {
		req.MaxPollIntervalMS = durationMillis(interval)
	}
}
