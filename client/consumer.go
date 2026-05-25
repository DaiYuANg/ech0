package client

import (
	"context"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
)

type Consumer struct {
	client     *Client
	consumer   string
	topic      string
	partition  uint32
	maxRecords int
	isolation  protocol.FetchIsolation
}

type ConsumerOption func(*Consumer)

type FetchOption func(*protocol.FetchRequest)

func (c *Client) Consumer(consumer, topic string, opts ...ConsumerOption) *Consumer {
	out := &Consumer{
		client:     c,
		consumer:   consumer,
		topic:      topic,
		maxRecords: 100,
		isolation:  protocol.FetchIsolationReadUncommitted,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(out)
		}
	}
	return out
}

func (c *Consumer) Fetch(ctx context.Context, opts ...FetchOption) (protocol.FetchResponse, error) {
	req := protocol.FetchRequest{
		Consumer:   c.consumer,
		Topic:      c.topic,
		Partition:  c.partition,
		MaxRecords: c.maxRecords,
		Isolation:  c.isolation,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	var out protocol.FetchResponse
	err := c.client.RoundTrip(ctx, protocol.CmdFetchRequest, protocol.CmdFetchResponse, req, &out)
	return out, err
}

func (c *Consumer) Commit(ctx context.Context, partition uint32, nextOffset uint64) (protocol.CommitOffsetResponse, error) {
	return c.CommitWithMetadata(ctx, partition, nextOffset, "")
}

func (c *Consumer) CommitWithMetadata(
	ctx context.Context,
	partition uint32,
	nextOffset uint64,
	metadata string,
) (protocol.CommitOffsetResponse, error) {
	req := protocol.CommitOffsetRequest{
		Consumer:   c.consumer,
		Topic:      c.topic,
		Partition:  partition,
		NextOffset: nextOffset,
		Metadata:   metadata,
	}
	var out protocol.CommitOffsetResponse
	err := c.client.RoundTrip(ctx, protocol.CmdCommitOffsetRequest, protocol.CmdCommitOffsetResponse, req, &out)
	return out, err
}

func (c *Consumer) Ack(ctx context.Context, record protocol.FetchRecord) (protocol.CommitOffsetResponse, error) {
	return c.Commit(ctx, c.partition, record.Offset+1)
}

func (c *Consumer) Nack(ctx context.Context, record protocol.FetchRecord, cause error) (protocol.NackResponse, error) {
	var lastError *string
	if cause != nil {
		message := cause.Error()
		lastError = &message
	}
	req := protocol.NackRequest{
		Consumer:  c.consumer,
		Topic:     c.topic,
		Partition: c.partition,
		Offset:    record.Offset,
		LastError: lastError,
	}
	var out protocol.NackResponse
	err := c.client.RoundTrip(ctx, protocol.CmdNackRequest, protocol.CmdNackResponse, req, &out)
	return out, err
}

func ConsumerPartition(partition uint32) ConsumerOption {
	return func(consumer *Consumer) {
		consumer.partition = partition
	}
}

func ConsumerFetchLimit(maxRecords int) ConsumerOption {
	return func(consumer *Consumer) {
		if maxRecords > 0 {
			consumer.maxRecords = maxRecords
		}
	}
}

func ConsumerReadCommitted() ConsumerOption {
	return func(consumer *Consumer) {
		consumer.isolation = protocol.FetchIsolationReadCommitted
	}
}

func FetchOffset(offset uint64) FetchOption {
	return func(req *protocol.FetchRequest) {
		req.Offset = &offset
	}
}

func FetchLimit(maxRecords int) FetchOption {
	return func(req *protocol.FetchRequest) {
		if maxRecords > 0 {
			req.MaxRecords = maxRecords
		}
	}
}

func FetchWait(minRecords int, maxWait time.Duration) FetchOption {
	return func(req *protocol.FetchRequest) {
		if minRecords > 0 {
			req.MinRecords = &minRecords
		}
		if maxWait > 0 {
			ms := durationMillis(maxWait)
			req.MaxWaitMS = &ms
		}
	}
}

func FetchReadCommitted() FetchOption {
	return func(req *protocol.FetchRequest) {
		req.Isolation = protocol.FetchIsolationReadCommitted
	}
}
