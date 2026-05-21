package ech0

import (
	"context"
	"errors"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type RuntimeHealth = internalbroker.RuntimeHealth
type RaftHealth = internalbroker.RaftHealth
type RaftGroupHealth = internalbroker.RaftGroupHealth
type DataShardHealth = internalbroker.DataShardHealth
type ClusterMetadata = internalbroker.ClusterMetadata
type ClusterRaftMetadata = internalbroker.ClusterRaftMetadata
type ClusterPeerMetadata = internalbroker.ClusterPeerMetadata
type TopicSummary = internalbroker.TopicSummary
type QuotaSummary = internalbroker.QuotaSummary
type StreamMetricsSnapshot = internalbroker.StreamMetricsSnapshot

func (b *Broker) RuntimeHealth() RuntimeHealth {
	if b == nil || b.broker == nil {
		return RuntimeHealth{Status: "closed"}
	}
	return b.broker.RuntimeHealth()
}

func (b *Broker) ClusterMetadata() ClusterMetadata {
	if b == nil || b.broker == nil {
		return ClusterMetadata{}
	}
	return b.broker.ClusterMetadata()
}

func (b *Broker) TopicSummaries(ctx context.Context) ([]TopicSummary, error) {
	if b == nil || b.broker == nil {
		return nil, oops.In("embedded").Code("broker_nil").Wrap(errors.New("broker is nil"))
	}
	out, err := b.broker.TopicSummariesFor(ctx)
	if err != nil {
		return nil, oops.In("embedded").Code("topic_summaries_failed").Wrapf(err, "list topic summaries")
	}
	return out, nil
}

func (b *Broker) QuotaSummary(ctx context.Context) (QuotaSummary, error) {
	if b == nil || b.broker == nil {
		return QuotaSummary{}, oops.In("embedded").Code("broker_nil").Wrap(errors.New("broker is nil"))
	}
	out, err := b.broker.QuotaSummaryFor(ctx)
	if err != nil {
		return QuotaSummary{}, oops.In("embedded").Code("quota_summary_failed").Wrapf(err, "load quota summary")
	}
	return out, nil
}

func (b *Broker) StreamMetrics(ctx context.Context) (StreamMetricsSnapshot, error) {
	if b == nil || b.broker == nil {
		return StreamMetricsSnapshot{}, oops.In("embedded").Code("broker_nil").Wrap(errors.New("broker is nil"))
	}
	out, err := b.broker.StreamMetricsSnapshotFor(ctx)
	if err != nil {
		return StreamMetricsSnapshot{}, oops.In("embedded").Code("stream_metrics_failed").Wrapf(err, "load stream metrics")
	}
	return out, nil
}
