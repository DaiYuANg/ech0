// Package admin provides role-focused administration helpers for embedded ech0 brokers.
package admin

import (
	"context"
	"errors"

	ech0 "github.com/lyonbrown4d/ech0"
	"github.com/samber/oops"
)

type Client struct {
	broker *ech0.Broker
}

type TopicOption = ech0.TopicOption
type TopicSummary = ech0.TopicSummary
type RuntimeHealth = ech0.RuntimeHealth
type ClusterMetadata = ech0.ClusterMetadata
type QuotaSummary = ech0.QuotaSummary
type StreamMetricsSnapshot = ech0.StreamMetricsSnapshot

func New(broker *ech0.Broker) (*Client, error) {
	if broker == nil {
		return nil, oops.In("admin").Code("broker_nil").Wrap(errors.New("broker is nil"))
	}
	return &Client{broker: broker}, nil
}

func (c *Client) CreateTopic(ctx context.Context, name string, opts ...TopicOption) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.broker.CreateTopic(ctx, name, opts...); err != nil {
		return wrap("admin_create_topic_failed", err, "create topic")
	}
	return nil
}

func (c *Client) Topics(ctx context.Context) ([]TopicSummary, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	out, err := c.broker.TopicSummaries(ctx)
	if err != nil {
		return nil, wrap("admin_topics_failed", err, "list topic summaries")
	}
	return out, nil
}

func (c *Client) Health() RuntimeHealth {
	if c == nil || c.broker == nil {
		return RuntimeHealth{Status: "closed"}
	}
	return c.broker.RuntimeHealth()
}

func (c *Client) Cluster() ClusterMetadata {
	if c == nil || c.broker == nil {
		return ClusterMetadata{}
	}
	return c.broker.ClusterMetadata()
}

func (c *Client) Quota(ctx context.Context) (QuotaSummary, error) {
	if err := c.validate(); err != nil {
		return QuotaSummary{}, err
	}
	out, err := c.broker.QuotaSummary(ctx)
	if err != nil {
		return QuotaSummary{}, wrap("admin_quota_failed", err, "load quota summary")
	}
	return out, nil
}

func (c *Client) StreamMetrics(ctx context.Context) (StreamMetricsSnapshot, error) {
	if err := c.validate(); err != nil {
		return StreamMetricsSnapshot{}, err
	}
	out, err := c.broker.StreamMetrics(ctx)
	if err != nil {
		return StreamMetricsSnapshot{}, wrap("admin_stream_metrics_failed", err, "load stream metrics")
	}
	return out, nil
}

func (c *Client) validate() error {
	if c == nil || c.broker == nil {
		return oops.In("admin").Code("client_nil").Wrap(errors.New("admin client is nil"))
	}
	return nil
}

func wrap(code string, err error, message string) error {
	if err == nil {
		return nil
	}
	return oops.In("admin").Code(code).Wrapf(err, "%s", message)
}
