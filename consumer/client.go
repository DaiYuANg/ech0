// Package consumer provides a role-focused consumer client for embedded ech0 brokers.
package consumer

import (
	"context"
	"errors"
	"time"

	ech0 "github.com/lyonbrown4d/ech0"
	"github.com/samber/oops"
)

type Client struct {
	broker   *ech0.Broker
	consumer string
	topic    string
}

type FetchOption = ech0.FetchOption
type Message = ech0.Message
type FetchResult = ech0.FetchResult
type CommittedOffset = ech0.CommittedOffset
type SeekResult = ech0.SeekResult
type PauseResult = ech0.PauseResult

func New(broker *ech0.Broker, consumer, topic string) (*Client, error) {
	if broker == nil {
		return nil, oops.In("consumer").Code("broker_nil").Wrap(errors.New("broker is nil"))
	}
	if consumer == "" {
		return nil, oops.In("consumer").Code("consumer_required").Wrap(errors.New("consumer is required"))
	}
	if topic == "" {
		return nil, oops.In("consumer").Code("topic_required").Wrap(errors.New("topic is required"))
	}
	return &Client{broker: broker, consumer: consumer, topic: topic}, nil
}

func (c *Client) Consumer() string {
	if c == nil {
		return ""
	}
	return c.consumer
}

func (c *Client) Topic() string {
	if c == nil {
		return ""
	}
	return c.topic
}

func (c *Client) Fetch(ctx context.Context, opts ...FetchOption) (FetchResult, error) {
	if err := c.validate(); err != nil {
		return FetchResult{}, err
	}
	out, err := c.broker.Fetch(ctx, c.consumer, c.topic, opts...)
	if err != nil {
		return FetchResult{}, wrap("consumer_fetch_failed", err, "fetch messages")
	}
	return out, nil
}

func (c *Client) Ack(ctx context.Context, msg Message) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.broker.Ack(ctx, c.consumer, msg); err != nil {
		return wrap("consumer_ack_failed", err, "ack message")
	}
	return nil
}

func (c *Client) AckWithMetadata(ctx context.Context, msg Message, metadata string) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.broker.AckWithMetadata(ctx, c.consumer, msg, metadata); err != nil {
		return wrap("consumer_ack_failed", err, "ack message with metadata")
	}
	return nil
}

func (c *Client) Commit(ctx context.Context, partition uint32, nextOffset uint64) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.broker.Commit(ctx, c.consumer, c.topic, partition, nextOffset); err != nil {
		return wrap("consumer_commit_failed", err, "commit offset")
	}
	return nil
}

func (c *Client) CommitWithMetadata(ctx context.Context, partition uint32, nextOffset uint64, metadata string) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.broker.CommitWithMetadata(ctx, c.consumer, c.topic, partition, nextOffset, metadata); err != nil {
		return wrap("consumer_commit_failed", err, "commit offset with metadata")
	}
	return nil
}

func (c *Client) Nack(ctx context.Context, msg Message, cause error) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.broker.Nack(ctx, c.consumer, msg, cause); err != nil {
		return wrap("consumer_nack_failed", err, "nack message")
	}
	return nil
}

func (c *Client) CommittedOffset(ctx context.Context, partition uint32) (*CommittedOffset, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	out, err := c.broker.CommittedOffset(ctx, c.consumer, c.topic, partition)
	if err != nil {
		return nil, wrap("consumer_committed_offset_failed", err, "load committed offset")
	}
	return out, nil
}

func (c *Client) SeekOffset(ctx context.Context, partition uint32, offset uint64) (SeekResult, error) {
	if err := c.validate(); err != nil {
		return SeekResult{}, err
	}
	out, err := c.broker.SeekOffset(ctx, c.consumer, c.topic, partition, offset)
	if err != nil {
		return SeekResult{}, wrap("consumer_seek_failed", err, "seek offset")
	}
	return out, nil
}

func (c *Client) SeekTimestamp(ctx context.Context, partition uint32, timestamp time.Time) (SeekResult, error) {
	if err := c.validate(); err != nil {
		return SeekResult{}, err
	}
	out, err := c.broker.SeekTimestamp(ctx, c.consumer, c.topic, partition, timestamp)
	if err != nil {
		return SeekResult{}, wrap("consumer_seek_failed", err, "seek timestamp")
	}
	return out, nil
}

func (c *Client) Pause(ctx context.Context, partition uint32) (PauseResult, error) {
	if err := c.validate(); err != nil {
		return PauseResult{}, err
	}
	out, err := c.broker.PauseConsumer(ctx, c.consumer, c.topic, partition)
	if err != nil {
		return PauseResult{}, wrap("consumer_pause_failed", err, "pause consumer")
	}
	return out, nil
}

func (c *Client) Resume(ctx context.Context, partition uint32) (PauseResult, error) {
	if err := c.validate(); err != nil {
		return PauseResult{}, err
	}
	out, err := c.broker.ResumeConsumer(ctx, c.consumer, c.topic, partition)
	if err != nil {
		return PauseResult{}, wrap("consumer_resume_failed", err, "resume consumer")
	}
	return out, nil
}

func (c *Client) validate() error {
	if c == nil || c.broker == nil {
		return oops.In("consumer").Code("client_nil").Wrap(errors.New("consumer client is nil"))
	}
	return nil
}

func wrap(code string, err error, message string) error {
	if err == nil {
		return nil
	}
	return oops.In("consumer").Code(code).Wrapf(err, "%s", message)
}
