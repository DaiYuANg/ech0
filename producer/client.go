// Package producer provides a role-focused async producer client for embedded ech0 brokers.
package producer

import (
	"context"
	"errors"
	"time"

	ech0 "github.com/lyonbrown4d/ech0"
	"github.com/samber/oops"
)

type Client struct {
	topic string
	inner *ech0.Producer
}

type Option = ech0.ProducerOption
type PublishOption = ech0.PublishOption
type Future = ech0.ProduceFuture
type Message = ech0.Message

func New(ctx context.Context, broker *ech0.Broker, topic string, opts ...Option) (*Client, error) {
	if broker == nil {
		return nil, oops.In("producer").Code("broker_nil").Wrap(errors.New("broker is nil"))
	}
	inner, err := broker.NewProducer(ctx, topic, opts...)
	if err != nil {
		return nil, wrap("producer_create_failed", err, "create producer")
	}
	return &Client{topic: topic, inner: inner}, nil
}

func (c *Client) Topic() string {
	if c == nil {
		return ""
	}
	return c.topic
}

func (c *Client) Send(ctx context.Context, payload []byte, opts ...PublishOption) (*Future, error) {
	if c == nil || c.inner == nil {
		return nil, oops.In("producer").Code("client_nil").Wrap(errors.New("producer client is nil"))
	}
	future, err := c.inner.Send(ctx, payload, opts...)
	if err != nil {
		return nil, wrap("producer_send_failed", err, "send producer message")
	}
	return future, nil
}

func (c *Client) Close(ctx context.Context) error {
	if c == nil || c.inner == nil {
		return nil
	}
	if err := c.inner.Close(ctx); err != nil {
		return wrap("producer_close_failed", err, "close producer")
	}
	return nil
}

func BatchSize(size int) Option {
	return ech0.ProducerBatchSize(size)
}

func Linger(duration time.Duration) Option {
	return ech0.ProducerLinger(duration)
}

func Buffer(size int) Option {
	return ech0.ProducerBuffer(size)
}

func InFlight(limit int) Option {
	return ech0.ProducerInFlight(limit)
}

func ID(id uint64) Option {
	return ech0.ProducerID(id)
}

func Epoch(epoch uint64) Option {
	return ech0.ProducerEpoch(epoch)
}

func DisableIdempotency() Option {
	return ech0.DisableProducerIdempotency()
}

func wrap(code string, err error, message string) error {
	if err == nil {
		return nil
	}
	return oops.In("producer").Code(code).Wrapf(err, "%s", message)
}
