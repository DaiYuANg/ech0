package client

import (
	"context"
	"sync/atomic"

	"github.com/lyonbrown4d/ech0/protocol"
)

type Producer struct {
	client       *Client
	topic        string
	partitioning protocol.ProducePartitioning
	partition    *uint32
	producerID   uint64
	epoch        uint64
	idempotent   bool
	sequence     atomic.Uint64
}

type ProducerOption func(*Producer)

type ProduceOption func(*protocol.ProduceRequest)

func (c *Client) Producer(topic string, opts ...ProducerOption) *Producer {
	producer := &Producer{
		client:       c,
		topic:        topic,
		partitioning: protocol.ProducePartitioningRoundRobin,
		producerID:   defaultProducerID(),
		idempotent:   true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(producer)
		}
	}
	return producer
}

func (p *Producer) Produce(ctx context.Context, payload []byte, opts ...ProduceOption) (protocol.ProduceResponse, error) {
	req := p.produceRequest(payload, opts)
	var out protocol.ProduceResponse
	err := p.client.RoundTrip(ctx, protocol.CmdProduceRequest, protocol.CmdProduceResponse, req, &out)
	return out, err
}

func (p *Producer) ProduceBatch(
	ctx context.Context,
	records []protocol.ProduceBatchRecord,
) (protocol.ProduceBatchResponse, error) {
	req := protocol.ProduceBatchRequest{
		Topic:        p.topic,
		Partition:    clonePartition(p.partition),
		Partitioning: p.partitioning,
		Records:      records,
		Idempotency:  p.idempotency(len(records)),
	}
	var out protocol.ProduceBatchResponse
	err := p.client.RoundTrip(ctx, protocol.CmdProduceBatchRequest, protocol.CmdProduceBatchResponse, req, &out)
	return out, err
}

func (p *Producer) ProducePayloads(ctx context.Context, payloads [][]byte) (protocol.ProduceBatchResponse, error) {
	req := protocol.ProduceBatchRequest{
		Topic:        p.topic,
		Partition:    clonePartition(p.partition),
		Partitioning: p.partitioning,
		Payloads:     payloads,
		Idempotency:  p.idempotency(len(payloads)),
	}
	var out protocol.ProduceBatchResponse
	err := p.client.RoundTrip(ctx, protocol.CmdProduceBatchRequest, protocol.CmdProduceBatchResponse, req, &out)
	return out, err
}

func (p *Producer) Fanout(ctx context.Context, payload []byte, opts ...ProduceOption) (protocol.ProduceFanoutResponse, error) {
	req := p.produceRequest(payload, opts)
	var out protocol.ProduceFanoutResponse
	err := p.client.RoundTrip(ctx, protocol.CmdProduceFanoutRequest, protocol.CmdProduceFanoutResponse, protocol.ProduceFanoutRequest{
		Topic:       req.Topic,
		RoutingKey:  req.RoutingKey,
		Key:         req.Key,
		Headers:     req.Headers,
		Tombstone:   req.Tombstone,
		ExpiresAtMS: req.ExpiresAtMS,
		Payload:     req.Payload,
	}, &out)
	return out, err
}

func (p *Producer) produceRequest(payload []byte, opts []ProduceOption) protocol.ProduceRequest {
	req := protocol.ProduceRequest{
		Topic:        p.topic,
		Partition:    clonePartition(p.partition),
		Partitioning: p.partitioning,
		Idempotency:  p.idempotency(1),
		Payload:      payload,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&req)
		}
	}
	return req
}

func (p *Producer) idempotency(records int) *protocol.ProduceIdempotency {
	if !p.idempotent || records <= 0 {
		return nil
	}
	count := nonNegativeIntToUint64(records)
	if count == 0 {
		return nil
	}
	next := p.sequence.Add(count)
	return &protocol.ProduceIdempotency{ProducerID: p.producerID, ProducerEpoch: p.epoch, BaseSequence: next - count}
}
