package ech0

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	internalbroker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
	"github.com/samber/oops"
)

type Transaction struct {
	broker   *Broker
	identity internalbroker.TransactionIdentity

	mu       sync.Mutex
	sequence uint64
	closed   bool
}

type TransactionOption func(*transactionOptions)

type transactionOptions struct {
	timeout time.Duration
}

func TransactionTimeout(timeout time.Duration) TransactionOption {
	return func(opts *transactionOptions) {
		if timeout > 0 {
			opts.timeout = timeout
		}
	}
}

func (b *Broker) BeginTransaction(ctx context.Context, transactionalID string, opts ...TransactionOption) (*Transaction, error) {
	txOpts := transactionOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&txOpts)
		}
	}
	result, err := b.broker.BeginTransaction(ctx, transactionalID, durationMillis(txOpts.timeout))
	if err != nil {
		return nil, oops.In("embedded").Code("transaction_begin_failed").Wrapf(err, "begin transaction")
	}
	return &Transaction{broker: b, identity: result.Identity}, nil
}

func (tx *Transaction) Publish(ctx context.Context, topic string, payload []byte, opts ...PublishOption) (Message, error) {
	if err := tx.ensureOpen(); err != nil {
		return Message{}, err
	}
	publishOpts := publishOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&publishOpts)
		}
	}
	record := store.NewRecordAppend(payload)
	record.Key = append([]byte(nil), publishOpts.key...)
	if publishOpts.tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	sequence := tx.nextSequence()
	result, err := tx.broker.broker.PublishTransactionalRecord(ctx, tx.identity, sequence, topic, publishPartitioning(publishOpts), record)
	if err != nil {
		return Message{}, oops.In("embedded").Code("transaction_publish_failed").With("topic", topic).Wrapf(err, "publish transaction message")
	}
	tx.advanceSequence(1)
	return messageFromRecord(topic, result.Partition, result.Record), nil
}

func (tx *Transaction) PublishBatch(ctx context.Context, topic string, payloads [][]byte, opts ...PublishOption) ([]Message, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, err
	}
	publishOpts := publishOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&publishOpts)
		}
	}
	records := make([]store.RecordAppend, 0, len(payloads))
	for _, payload := range payloads {
		record := store.NewRecordAppend(payload)
		record.Key = append([]byte(nil), publishOpts.key...)
		if publishOpts.tombstone {
			record.Attributes |= store.RecordAttributeTombstone
		}
		records = append(records, record)
	}
	sequence := tx.nextSequence()
	result, err := tx.broker.broker.PublishTransactionalBatch(ctx, tx.identity, sequence, topic, publishPartitioning(publishOpts), records)
	if err != nil {
		return nil, oops.In("embedded").Code("transaction_publish_batch_failed").With("topic", topic).Wrapf(err, "publish transaction message batch")
	}
	count, err := uint64FromInt(len(records))
	if err != nil {
		return nil, err
	}
	tx.advanceSequence(count)
	messages := make([]Message, 0, len(result.Records))
	for _, record := range result.Records {
		messages = append(messages, messageFromRecord(topic, result.Partition, record))
	}
	return messages, nil
}

func (tx *Transaction) CommitOffset(ctx context.Context, consumer string, msg Message) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	_, err := tx.broker.broker.CommitTransactionOffset(ctx, tx.identity, internalbroker.TransactionOffsetCommit{
		Consumer:   consumer,
		Topic:      msg.Topic,
		Partition:  msg.Partition,
		NextOffset: msg.NextOffset,
	})
	return oops.In("embedded").Code("transaction_offset_failed").With("consumer", consumer, "topic", msg.Topic).Wrapf(err, "stage transaction offset")
}

func (tx *Transaction) Commit(ctx context.Context) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	_, err := tx.broker.broker.CommitTransaction(ctx, tx.identity)
	if err == nil {
		tx.close()
	}
	return oops.In("embedded").Code("transaction_commit_failed").Wrapf(err, "commit transaction")
}

func (tx *Transaction) Abort(ctx context.Context) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	_, err := tx.broker.broker.AbortTransaction(ctx, tx.identity)
	if err == nil {
		tx.close()
	}
	return oops.In("embedded").Code("transaction_abort_failed").Wrapf(err, "abort transaction")
}

func (tx *Transaction) ensureOpen() error {
	if tx == nil || tx.broker == nil || tx.broker.broker == nil {
		return oops.In("embedded").Code("transaction_nil").Wrap(errors.New("transaction is nil"))
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return oops.In("embedded").Code("transaction_closed").Wrap(errors.New("transaction is closed"))
	}
	return nil
}

func (tx *Transaction) nextSequence() uint64 {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.sequence
}

func (tx *Transaction) advanceSequence(delta uint64) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.sequence += delta
}

func (tx *Transaction) close() {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.closed = true
}

func uint64FromInt(value int) (uint64, error) {
	if value < 0 {
		return 0, oops.In("embedded").Code("negative_int").Wrap(fmt.Errorf("negative value %d", value))
	}
	out, err := strconv.ParseUint(strconv.Itoa(value), 10, 64)
	if err != nil {
		return 0, oops.In("embedded").Code("int_convert_failed").Wrapf(err, "convert int")
	}
	return out, nil
}
