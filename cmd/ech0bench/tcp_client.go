package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/transport"
)

const tcpBenchOperationTimeout = 30 * time.Second

type tcpBenchBroker struct {
	addr string
	pool chan net.Conn
}

func newTCPBenchBroker(cfg benchConfig) *tcpBenchBroker {
	producerSlots := int(cfg.producers) * int(cfg.producerInflight)
	poolSize := producerSlots + int(cfg.consumers) + 1
	if poolSize <= 0 {
		poolSize = 1
	}
	return &tcpBenchBroker{addr: cfg.brokerAddr, pool: make(chan net.Conn, poolSize)}
}

func (b *tcpBenchBroker) CreateTopic(ctx context.Context, name string, partitions uint32) error {
	var out protocol.CreateTopicResponse
	err := b.roundTrip(
		ctx,
		protocol.CmdCreateTopicRequest,
		protocol.CmdCreateTopicResponse,
		protocol.CreateTopicRequest{Topic: name, Partitions: partitions},
		&out,
	)
	return err
}

func (b *tcpBenchBroker) Publish(ctx context.Context, topic string, payload, key []byte) (benchMessage, error) {
	var out protocol.ProduceResponse
	err := b.roundTrip(
		ctx,
		protocol.CmdProduceRequest,
		protocol.CmdProduceResponse,
		protocol.ProduceRequest{
			Topic:        topic,
			Partitioning: protocol.ProducePartitioningKeyHash,
			Key:          key,
			Payload:      payload,
		},
		&out,
	)
	if err != nil {
		return benchMessage{}, err
	}
	return benchMessage{Payload: payload, NextOffset: out.NextOffset}, nil
}

func (b *tcpBenchBroker) PublishBatch(ctx context.Context, topic string, partition uint32, records []benchPublishRecord) ([]benchMessage, error) {
	protocolRecords := make([]protocol.ProduceBatchRecord, 0, len(records))
	for _, record := range records {
		protocolRecords = append(protocolRecords, protocol.ProduceBatchRecord{
			Key:     record.Key,
			Payload: record.Payload,
		})
	}
	var out protocol.ProduceBatchResponse
	err := b.roundTrip(
		ctx,
		protocol.CmdProduceBatchRequest,
		protocol.CmdProduceBatchResponse,
		protocol.ProduceBatchRequest{
			Topic:        topic,
			Partition:    &partition,
			Partitioning: protocol.ProducePartitioningExplicit,
			Records:      protocolRecords,
		},
		&out,
	)
	if err != nil {
		return nil, err
	}
	messages := make([]benchMessage, 0, out.Appended)
	for idx := range out.Appended {
		messages = append(messages, benchMessage{
			Payload:    records[idx].Payload,
			NextOffset: out.BaseOffset + uint64(idx) + 1,
		})
	}
	return messages, nil
}

func (b *tcpBenchBroker) Fetch(ctx context.Context, consumer, topic string, partition uint32, offset *uint64, maxRecords int) (benchFetchResult, error) {
	var out protocol.FetchResponse
	err := b.roundTrip(
		ctx,
		protocol.CmdFetchRequest,
		protocol.CmdFetchResponse,
		protocol.FetchRequest{Consumer: consumer, Topic: topic, Partition: partition, Offset: offset, MaxRecords: maxRecords},
		&out,
	)
	if err != nil {
		return benchFetchResult{}, err
	}
	messages := make([]benchMessage, 0, len(out.Records))
	for _, record := range out.Records {
		messages = append(messages, benchMessage{Payload: record.Payload, NextOffset: record.Offset + 1})
	}
	return benchFetchResult{Messages: messages, NextOffset: out.NextOffset}, nil
}

func (b *tcpBenchBroker) Commit(ctx context.Context, consumer, topic string, partition uint32, nextOffset uint64) error {
	var out protocol.CommitOffsetResponse
	return b.roundTrip(
		ctx,
		protocol.CmdCommitOffsetRequest,
		protocol.CmdCommitOffsetResponse,
		protocol.CommitOffsetRequest{Consumer: consumer, Topic: topic, Partition: partition, NextOffset: nextOffset},
		&out,
	)
}

func (b *tcpBenchBroker) Close(context.Context) error {
	var result error
	for {
		select {
		case conn := <-b.pool:
			result = errors.Join(result, conn.Close())
		default:
			return result
		}
	}
}

func (b *tcpBenchBroker) roundTrip(ctx context.Context, requestCommand, responseCommand uint16, request, response any) error {
	frame, err := newTCPBenchFrame(requestCommand, request)
	if err != nil {
		return err
	}
	conn, err := b.borrow(ctx)
	if err != nil {
		return err
	}
	release := false
	defer func() {
		if release {
			b.release(conn)
			return
		}
		closeTCPBenchConn(conn)
	}()

	deadlineErr := setTCPBenchDeadline(ctx, conn)
	if deadlineErr != nil {
		return deadlineErr
	}
	writeErr := transport.WriteFrame(conn, frame)
	if writeErr != nil {
		return fmt.Errorf("write tcp benchmark frame: %w", writeErr)
	}
	out, err := transport.ReadFrame(conn)
	if err != nil {
		return fmt.Errorf("read tcp benchmark frame: %w", err)
	}
	if err := decodeTCPBenchResponse(out, responseCommand, response); err != nil {
		release = shouldReleaseTCPBenchResponse(out)
		return err
	}
	release = true
	return nil
}

func newTCPBenchFrame(command uint16, request any) (transport.Frame, error) {
	body, err := protocol.EncodeBody(command, request)
	if err != nil {
		return transport.Frame{}, fmt.Errorf("encode tcp benchmark request: %w", err)
	}
	frame, err := transport.NewFrame(protocol.Version, command, body)
	if err != nil {
		return transport.Frame{}, fmt.Errorf("create tcp benchmark frame: %w", err)
	}
	return frame, nil
}

func decodeTCPBenchResponse(frame transport.Frame, expectedCommand uint16, response any) error {
	if frame.Header.Status == transport.StatusError || frame.Header.Command == protocol.CmdErrorResponse {
		return decodeTCPBenchError(frame)
	}
	if frame.Header.Command != expectedCommand {
		return fmt.Errorf("unexpected tcp benchmark response command: got %d want %d", frame.Header.Command, expectedCommand)
	}
	if response == nil {
		return nil
	}
	if err := protocol.DecodeBody(frame.Header.Command, frame.Body, response); err != nil {
		return fmt.Errorf("decode tcp benchmark response: %w", err)
	}
	return nil
}

func shouldReleaseTCPBenchResponse(frame transport.Frame) bool {
	return frame.Header.Status == transport.StatusError || frame.Header.Command == protocol.CmdErrorResponse
}

func (b *tcpBenchBroker) borrow(ctx context.Context) (net.Conn, error) {
	select {
	case conn := <-b.pool:
		return conn, nil
	default:
		return b.dial(ctx)
	}
}

func (b *tcpBenchBroker) dial(ctx context.Context) (net.Conn, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", b.addr)
	if err != nil {
		return nil, fmt.Errorf("dial tcp benchmark broker %s: %w", b.addr, err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			closeTCPBenchConn(conn)
			return nil, fmt.Errorf("set tcp benchmark no delay: %w", err)
		}
	}
	return conn, nil
}

func (b *tcpBenchBroker) release(conn net.Conn) {
	select {
	case b.pool <- conn:
	default:
		closeTCPBenchConn(conn)
	}
}

func closeTCPBenchConn(conn net.Conn) {
	if err := conn.Close(); err != nil {
		return
	}
}

func setTCPBenchDeadline(ctx context.Context, conn net.Conn) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(tcpBenchOperationTimeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set tcp benchmark deadline: %w", err)
	}
	return nil
}

func decodeTCPBenchError(frame transport.Frame) error {
	var out protocol.ErrorResponse
	if err := protocol.DecodeBody(protocol.CmdErrorResponse, frame.Body, &out); err != nil {
		return fmt.Errorf("decode tcp benchmark error response: %w", err)
	}
	return fmt.Errorf("tcp broker error %s: %s", out.Code, out.Message)
}
