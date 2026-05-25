package client

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
)

type TxProducer struct {
	client          *Client
	transactionalID string
	identity        protocol.TransactionIdentity
	sequence        atomic.Uint64
}

type TxOption func(*protocol.TxBeginRequest)

func (c *Client) BeginTransaction(ctx context.Context, transactionalID string, opts ...TxOption) (*TxProducer, error) {
	req := protocol.TxBeginRequest{TransactionalID: transactionalID, TimeoutMS: durationMillis(30 * time.Second)}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	var out protocol.TxBeginResponse
	err := c.RoundTrip(ctx, protocol.CmdTxBeginRequest, protocol.CmdTxBeginResponse, req, &out)
	if err != nil {
		return nil, err
	}
	return &TxProducer{client: c, transactionalID: transactionalID, identity: out.Identity}, nil
}

func (p *TxProducer) TransactionalID() string {
	if p == nil {
		return ""
	}
	return p.transactionalID
}

func (p *TxProducer) Identity() protocol.TransactionIdentity {
	if p == nil {
		return protocol.TransactionIdentity{}
	}
	return p.identity
}

func (p *TxProducer) Publish(
	ctx context.Context,
	topic string,
	payload []byte,
	opts ...ProduceOption,
) (protocol.TxPublishResponse, error) {
	req := protocol.TxPublishRequest{
		Identity:     p.identity,
		Sequence:     p.sequence.Add(1) - 1,
		Topic:        topic,
		Partitioning: protocol.ProducePartitioningRoundRobin,
		Payload:      payload,
	}
	applyTxProduceOptions(&req, opts)
	var out protocol.TxPublishResponse
	err := p.client.RoundTrip(ctx, protocol.CmdTxPublishRequest, protocol.CmdTxPublishResponse, req, &out)
	return out, err
}

func (p *TxProducer) PublishBatch(
	ctx context.Context,
	topic string,
	records []protocol.ProduceBatchRecord,
	partition *uint32,
) (protocol.TxPublishBatchResponse, error) {
	count := nonNegativeIntToUint64(len(records))
	req := protocol.TxPublishBatchRequest{
		Identity:     p.identity,
		BaseSequence: p.sequence.Add(count) - count,
		Topic:        topic,
		Partition:    clonePartition(partition),
		Partitioning: protocol.ProducePartitioningRoundRobin,
		Records:      records,
	}
	var out protocol.TxPublishBatchResponse
	err := p.client.RoundTrip(ctx, protocol.CmdTxPublishBatchRequest, protocol.CmdTxPublishBatchResponse, req, &out)
	return out, err
}

func (p *TxProducer) CommitOffset(
	ctx context.Context,
	consumer string,
	topic string,
	partition uint32,
	nextOffset uint64,
	metadata string,
) (protocol.TxCommitOffsetResponse, error) {
	req := protocol.TxCommitOffsetRequest{
		Identity:   p.identity,
		Consumer:   consumer,
		Topic:      topic,
		Partition:  partition,
		NextOffset: nextOffset,
		Metadata:   metadata,
	}
	var out protocol.TxCommitOffsetResponse
	err := p.client.RoundTrip(ctx, protocol.CmdTxCommitOffsetRequest, protocol.CmdTxCommitOffsetResponse, req, &out)
	return out, err
}

func (p *TxProducer) Commit(ctx context.Context) (protocol.TxCommitResponse, error) {
	var out protocol.TxCommitResponse
	err := p.client.RoundTrip(ctx, protocol.CmdTxCommitRequest, protocol.CmdTxCommitResponse, protocol.TxCommitRequest{Identity: p.identity}, &out)
	return out, err
}

func (p *TxProducer) Abort(ctx context.Context) (protocol.TxAbortResponse, error) {
	var out protocol.TxAbortResponse
	err := p.client.RoundTrip(ctx, protocol.CmdTxAbortRequest, protocol.CmdTxAbortResponse, protocol.TxAbortRequest{Identity: p.identity}, &out)
	return out, err
}

func TxTimeout(timeout time.Duration) TxOption {
	return func(req *protocol.TxBeginRequest) {
		if timeout > 0 {
			req.TimeoutMS = durationMillis(timeout)
		}
	}
}

func applyTxProduceOptions(req *protocol.TxPublishRequest, opts []ProduceOption) {
	produceReq := protocol.ProduceRequest{Partitioning: req.Partitioning, Payload: req.Payload}
	for _, opt := range opts {
		if opt != nil {
			opt(&produceReq)
		}
	}
	req.Partition = produceReq.Partition
	req.Partitioning = produceReq.Partitioning
	req.RoutingKey = produceReq.RoutingKey
	req.Key = produceReq.Key
	req.Headers = produceReq.Headers
	req.Tombstone = produceReq.Tombstone
}
