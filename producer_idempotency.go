package ech0

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
)

var fallbackProducerID atomic.Uint64

func newProducerID() uint64 {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err == nil {
		id := binary.BigEndian.Uint64(raw[:])
		if id != 0 {
			return id
		}
	}
	next := fallbackProducerID.Add(1)
	now := uint64(time.Now().UnixNano())
	id := now ^ next
	if id == 0 {
		return next
	}
	return id
}

func (p *Producer) nextBatchSequence(partition uint32, records int) (uint64, error) {
	count, err := producerRecordCount(records)
	if err != nil {
		return 0, err
	}
	p.sequenceMu.Lock()
	defer p.sequenceMu.Unlock()
	current := p.sequences.GetOrDefault(partition, 0)
	if current > math.MaxUint64-count {
		return 0, producerError("producer_sequence_overflow", "producer sequence overflows uint64")
	}
	p.sequences.Set(partition, current+count)
	return current, nil
}

func producerRecordCount(records int) (uint64, error) {
	if records <= 0 {
		return 0, producerError("producer_empty_batch", "producer batch requires at least one record")
	}
	out, err := strconv.ParseUint(strconv.Itoa(records), 10, 64)
	if err != nil {
		return 0, producerWrap("producer_record_count_convert_failed", err, "convert producer record count")
	}
	return out, nil
}

func (p *Producer) batchIdempotency(partition uint32, records int) (*internalbroker.ProduceIdempotency, bool, error) {
	if !p.opts.idempotent {
		return nil, false, nil
	}
	sequence, err := p.nextBatchSequence(partition, records)
	if err != nil {
		return nil, false, err
	}
	return &internalbroker.ProduceIdempotency{
		ProducerID:    p.opts.producerID,
		ProducerEpoch: p.opts.producerEpoch,
		BaseSequence:  sequence,
	}, true, nil
}
