package ech0

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/cespare/xxhash/v2"
	"github.com/samber/oops"
)

type Producer struct {
	broker     *Broker
	topic      string
	partitions uint32
	opts       producerOptions

	ctx    context.Context
	cancel context.CancelFunc
	input  chan producerItem
	batch  chan producerBatch

	roundRobin atomic.Uint64
	wg         sync.WaitGroup
	sendMu     sync.Mutex
	sends      sync.WaitGroup
	closed     bool
	closeOnce  sync.Once
}

type producerItem struct {
	partition uint32
	record    store.RecordAppend
	future    *ProduceFuture
}

type producerBatch struct {
	partition uint32
	items     *collectionlist.List[producerItem]
}

func (b *Broker) NewProducer(ctx context.Context, topic string, opts ...ProducerOption) (*Producer, error) {
	if b == nil || b.broker == nil {
		return nil, producerError("broker_nil", "broker is nil")
	}
	if ctx == nil {
		return nil, producerError("producer_context_nil", "producer context is nil")
	}
	partitions, err := b.topicPartitions(ctx, topic)
	if err != nil {
		return nil, err
	}
	producerOpts := producerOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&producerOpts)
		}
	}
	producerOpts = normalizeProducerOptions(producerOpts)
	producerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p := &Producer{
		broker:     b,
		topic:      topic,
		partitions: partitions,
		opts:       producerOpts,
		ctx:        producerCtx,
		cancel:     cancel,
		input:      make(chan producerItem, producerOpts.buffer),
		batch:      make(chan producerBatch, producerOpts.inFlight*2),
	}
	p.start()
	return p, nil
}

func (b *Broker) topicPartitions(ctx context.Context, topic string) (uint32, error) {
	if ctx == nil {
		return 0, producerError("producer_context_nil", "producer context is nil")
	}
	select {
	case <-ctx.Done():
		return 0, producerWrap("producer_topic_context_done", ctx.Err(), "load producer topic")
	default:
	}
	topics, err := b.broker.ListTopics()
	if err != nil {
		return 0, producerWrap("producer_topic_list_failed", err, "list producer topics")
	}
	for index := range topics {
		item := &topics[index]
		if item.Name == topic {
			return item.Partitions, nil
		}
	}
	return 0, producerError("producer_topic_not_found", "topic %s not found", topic)
}

func (p *Producer) start() {
	p.wg.Go(p.runAccumulator)
	for range p.opts.inFlight {
		p.wg.Go(p.runSender)
	}
}

func (p *Producer) Send(ctx context.Context, payload []byte, opts ...PublishOption) (*ProduceFuture, error) {
	if ctx == nil {
		return nil, producerError("producer_context_nil", "producer context is nil")
	}
	item, err := p.newProducerItem(payload, opts...)
	if err != nil {
		return nil, err
	}
	if !p.beginSend() {
		return nil, producerError("producer_closed", "producer is closed")
	}
	defer p.sends.Done()
	select {
	case p.input <- item:
		return item.future, nil
	case <-ctx.Done():
		return nil, producerWrap("producer_send_context_done", ctx.Err(), "enqueue producer message")
	case <-p.ctx.Done():
		return nil, producerWrap("producer_send_closed", p.ctx.Err(), "enqueue producer message")
	}
}

func (p *Producer) Close(ctx context.Context) error {
	if ctx == nil {
		return producerError("producer_context_nil", "producer context is nil")
	}
	p.closeInput()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		<-done
		return producerWrap("producer_close_context_done", ctx.Err(), "close producer")
	}
}

func (p *Producer) beginSend() bool {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	if p.closed {
		return false
	}
	p.sends.Add(1)
	return true
}

func (p *Producer) closeInput() {
	p.closeOnce.Do(func() {
		p.sendMu.Lock()
		p.closed = true
		p.sendMu.Unlock()
		p.sends.Wait()
		close(p.input)
	})
}

func (p *Producer) newProducerItem(payload []byte, opts ...PublishOption) (producerItem, error) {
	publishOpts := publishOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&publishOpts)
		}
	}
	partition, err := p.selectPartition(publishOpts)
	if err != nil {
		return producerItem{}, err
	}
	record := store.NewRecordAppend(payload)
	record.Key = append([]byte(nil), publishOpts.key...)
	if publishOpts.tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	return producerItem{partition: partition, record: record, future: newProduceFuture()}, nil
}

func (p *Producer) selectPartition(opts publishOptions) (uint32, error) {
	if opts.partition != nil {
		if *opts.partition >= p.partitions {
			return 0, producerError("producer_partition_out_of_range", "partition %d out of range for topic %s", *opts.partition, p.topic)
		}
		return *opts.partition, nil
	}
	if len(opts.key) > 0 {
		return producerPartitionFromUint64(xxhash.Sum64(opts.key), p.partitions), nil
	}
	next := p.roundRobin.Add(1) - 1
	return producerPartitionFromUint64(next, p.partitions), nil
}

func producerPartitionFromUint64(value uint64, partitions uint32) uint32 {
	if partitions == 0 {
		return 0
	}
	partition := value % uint64(partitions)
	if partition > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(partition)
}

func producerError(code, format string, args ...any) error {
	return oops.In("producer").Code(code).Wrap(fmt.Errorf(format, args...))
}

func producerWrap(code string, err error, msg string) error {
	if err == nil {
		return nil
	}
	return oops.In("producer").Code(code).Wrapf(err, "%s", msg)
}

func producerBatchMismatchError(want, got int) error {
	return producerError("producer_batch_result_mismatch", "producer batch result length mismatch: got %d want %d", got, want)
}
