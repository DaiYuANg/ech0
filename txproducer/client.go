// Package txproducer provides a role-focused transactional producer client for embedded ech0 brokers.
package txproducer

import (
	"context"
	"errors"
	"time"

	ech0 "github.com/lyonbrown4d/ech0"
	"github.com/samber/oops"
)

type Client struct {
	transactionalID string
	tx              *ech0.Transaction
}

type Option = ech0.TransactionOption
type PublishOption = ech0.PublishOption
type Message = ech0.Message
type ConsumerGroup = ech0.ConsumerGroup

func New(ctx context.Context, broker *ech0.Broker, transactionalID string, opts ...Option) (*Client, error) {
	if broker == nil {
		return nil, oops.In("txproducer").Code("broker_nil").Wrap(errors.New("broker is nil"))
	}
	tx, err := broker.BeginTransaction(ctx, transactionalID, opts...)
	if err != nil {
		return nil, wrap("txproducer_begin_failed", err, "begin transaction")
	}
	return &Client{transactionalID: transactionalID, tx: tx}, nil
}

func (c *Client) TransactionalID() string {
	if c == nil {
		return ""
	}
	return c.transactionalID
}

func (c *Client) Publish(ctx context.Context, topic string, payload []byte, opts ...PublishOption) (Message, error) {
	if err := c.validate(); err != nil {
		return Message{}, err
	}
	out, err := c.tx.Publish(ctx, topic, payload, opts...)
	if err != nil {
		return Message{}, wrap("txproducer_publish_failed", err, "publish transactional message")
	}
	return out, nil
}

func (c *Client) PublishBatch(ctx context.Context, topic string, payloads [][]byte, opts ...PublishOption) ([]Message, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	out, err := c.tx.PublishBatch(ctx, topic, payloads, opts...)
	if err != nil {
		return nil, wrap("txproducer_publish_batch_failed", err, "publish transactional message batch")
	}
	return out, nil
}

func (c *Client) CommitOffset(ctx context.Context, consumer string, msg Message) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.tx.CommitOffset(ctx, consumer, msg); err != nil {
		return wrap("txproducer_commit_offset_failed", err, "stage consumer offset")
	}
	return nil
}

func (c *Client) CommitOffsetWithMetadata(ctx context.Context, consumer string, msg Message, metadata string) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.tx.CommitOffsetWithMetadata(ctx, consumer, msg, metadata); err != nil {
		return wrap("txproducer_commit_offset_failed", err, "stage consumer offset with metadata")
	}
	return nil
}

func (c *Client) CommitConsumerGroupOffset(ctx context.Context, group *ConsumerGroup, msg Message) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.tx.CommitConsumerGroupOffset(ctx, group, msg); err != nil {
		return wrap("txproducer_group_offset_failed", err, "stage consumer group offset")
	}
	return nil
}

func (c *Client) Commit(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.tx.Commit(ctx); err != nil {
		return wrap("txproducer_commit_failed", err, "commit transaction")
	}
	return nil
}

func (c *Client) Abort(ctx context.Context) error {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.tx.Abort(ctx); err != nil {
		return wrap("txproducer_abort_failed", err, "abort transaction")
	}
	return nil
}

func (c *Client) validate() error {
	if c == nil || c.tx == nil {
		return oops.In("txproducer").Code("client_nil").Wrap(errors.New("transactional producer client is nil"))
	}
	return nil
}

func Timeout(timeout time.Duration) Option {
	return ech0.TransactionTimeout(timeout)
}

func wrap(code string, err error, message string) error {
	if err == nil {
		return nil
	}
	return oops.In("txproducer").Code(code).Wrapf(err, "%s", message)
}
