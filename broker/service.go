//nolint:revive // Broker service methods and raft command registration share the same runtime boundary.
package broker

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/queue"
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/arcgolabs/eventx"
)

type Option func(*Broker)

type Broker struct {
	cfg    Config
	log    store.MessageLogStore
	meta   metadataStore
	queue  *queue.Runtime
	direct *direct.Runtime
	router *partitionRouter
	events eventx.BusRuntime
	logger *slog.Logger

	raftMu sync.RWMutex
	raft   *raftNode
}

type ProduceResult struct {
	Partition uint32
	Record    store.Record
}

type ProduceBatchResult struct {
	Partition uint32
	Records   []store.Record
}

type metadataStore interface {
	store.OffsetStore
	store.TopicCatalogStore
	store.ConsumerGroupStore
	store.BrokerStateStore
}

func New(cfg Config, opts ...Option) (*Broker, error) {
	normalizeConfig(&cfg)
	st := store.NewMemoryStore()
	return NewWithStores(cfg, st, st, opts...)
}

func NewWithStores(cfg Config, logStore store.MessageLogStore, metaStore metadataStore, opts ...Option) (*Broker, error) {
	normalizeConfig(&cfg)
	if logStore == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "log store is required")
	}
	if metaStore == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "metadata store is required")
	}
	b := &Broker{
		cfg:    cfg,
		log:    logStore,
		meta:   metaStore,
		router: newPartitionRouter(),
		events: eventx.New(),
		logger: slog.Default(),
	}
	b.queue = queue.New(logStore, metaStore)
	b.direct = direct.New(logStore, metaStore)
	for _, opt := range opts {
		opt(b)
	}
	if b.events == nil {
		b.events = eventx.New()
	}
	if b.logger == nil {
		b.logger = slog.Default()
	}
	return b, nil
}

func WithLogger(logger *slog.Logger) Option {
	return func(b *Broker) {
		if logger != nil {
			b.logger = logger
		}
	}
}

func WithEventBus(events eventx.BusRuntime) Option {
	return func(b *Broker) {
		if events != nil {
			b.events = events
		}
	}
}

func (b *Broker) Config() Config {
	return b.cfg
}

func (b *Broker) Events() eventx.BusRuntime {
	return b.events
}

func (b *Broker) Start(ctx context.Context) error {
	if err := b.meta.SaveBrokerState(store.BrokerState{
		NodeID: fmt.Sprintf("node-%d", b.cfg.Broker.NodeID),
		Epoch:  1,
	}); err != nil {
		return wrapBrokerStore(err, "save broker state")
	}
	if b.cfg.Raft.Enabled {
		node, err := startRaft(ctx, b)
		if err != nil {
			return err
		}
		b.raftMu.Lock()
		b.raft = node
		b.raftMu.Unlock()
	}
	return nil
}

func (b *Broker) Stop(ctx context.Context) error {
	_ = ctx
	b.raftMu.Lock()
	node := b.raft
	b.raft = nil
	b.raftMu.Unlock()
	if node != nil {
		if err := node.Close(); err != nil {
			return err
		}
	}
	if b.events != nil {
		return wrapBroker("event_bus_close_failed", b.events.Close(), "close event bus")
	}
	return nil
}

func (b *Broker) RuntimeHealth() RuntimeHealth {
	health := RuntimeHealth{
		Status:      "ok",
		RuntimeMode: "standalone",
	}
	b.raftMu.RLock()
	node := b.raft
	b.raftMu.RUnlock()
	if b.cfg.Raft.Enabled {
		health.RuntimeMode = "raft"
		if node == nil {
			health.Status = "degraded"
			health.Raft = &RaftHealth{NodeID: b.cfg.Broker.NodeID, KnownNodes: len(b.cfg.Raft.Cluster)}
			return health
		}
		health.Raft = node.Health()
		if health.Raft.LeaderID == 0 {
			health.Status = "degraded"
		}
	}
	return health
}

type RuntimeHealth struct {
	Status      string      `json:"status"`
	RuntimeMode string      `json:"runtime_mode"`
	Raft        *RaftHealth `json:"raft,omitempty"`
}

type RaftHealth struct {
	NodeID        uint64 `json:"node_id"`
	KnownNodes    int    `json:"known_nodes"`
	LeaderID      uint64 `json:"leader_id,omitempty"`
	LocalIsLeader bool   `json:"local_is_leader"`
}

func (b *Broker) CreateTopic(ctx context.Context, topic store.TopicConfig) (store.TopicConfig, error) {
	return proposeOrApply(ctx, b, raftCommandCreateTopic, topic, b.applyCreateTopic)
}

func (b *Broker) Publish(ctx context.Context, topic string, partitioning PublishPartitioning, key []byte, tombstone bool, payload []byte) (ProduceResult, error) {
	req := produceCommand{
		Topic:        topic,
		Partitioning: partitioning,
		Key:          append([]byte(nil), key...),
		Tombstone:    tombstone,
		Payload:      append([]byte(nil), payload...),
	}
	return proposeOrApply(ctx, b, raftCommandProduce, req, b.applyProduce)
}

func (b *Broker) PublishBatch(ctx context.Context, topic string, partitioning PublishPartitioning, records []store.RecordAppend) (ProduceBatchResult, error) {
	copied := make([]store.RecordAppend, 0, len(records))
	for _, record := range records {
		copied = append(copied, cloneAppend(record))
	}
	req := produceBatchCommand{Topic: topic, Partitioning: partitioning, Records: copied}
	return proposeOrApply(ctx, b, raftCommandProduceBatch, req, b.applyProduceBatch)
}

func (b *Broker) Fetch(consumer, topic string, partition uint32, offset *uint64, maxRecords int) (store.PollResult, error) {
	if maxRecords <= 0 || maxRecords > b.cfg.Broker.MaxFetchRecords {
		maxRecords = b.cfg.Broker.MaxFetchRecords
	}
	poll, err := b.queue.Fetch(consumer, topic, partition, offset, maxRecords)
	if err != nil {
		return store.PollResult{}, wrapBroker("queue_fetch_failed", err, "fetch records")
	}
	return poll, nil
}

func (b *Broker) CommitOffset(ctx context.Context, consumer, topic string, partition uint32, nextOffset uint64) error {
	req := commitOffsetCommand{Consumer: consumer, Topic: topic, Partition: partition, NextOffset: nextOffset}
	_, err := proposeOrApply(ctx, b, raftCommandCommitOffset, req, b.applyCommitOffset)
	return err
}

func (b *Broker) ListTopics() ([]store.TopicConfig, error) {
	topics, err := b.queue.ListTopics()
	if err != nil {
		return nil, wrapBroker("list_topics_failed", err, "list topics")
	}
	out := make([]store.TopicConfig, 0, len(topics))
	for i := range topics {
		topic := topics[i]
		if !isInternalTopicName(topic.Name) {
			out = append(out, topic)
		}
	}
	return out, nil
}

func (b *Broker) SendDirect(ctx context.Context, sender, recipient string, conversationID *string, payload []byte) (direct.SendResult, error) {
	req := directCommand{Sender: sender, Recipient: recipient, ConversationID: conversationID, Payload: append([]byte(nil), payload...)}
	return proposeOrApply(ctx, b, raftCommandDirectSend, req, b.applyDirectSend)
}

func (b *Broker) FetchInbox(recipient string, maxRecords int) (direct.FetchInboxResult, error) {
	if maxRecords <= 0 || maxRecords > b.cfg.Broker.MaxFetchRecords {
		maxRecords = b.cfg.Broker.MaxFetchRecords
	}
	inbox, err := b.direct.FetchInbox(recipient, maxRecords)
	if err != nil {
		return direct.FetchInboxResult{}, wrapBroker("fetch_inbox_failed", err, "fetch inbox")
	}
	return inbox, nil
}

func (b *Broker) AckDirect(ctx context.Context, recipient string, nextOffset uint64) error {
	req := ackDirectCommand{Recipient: recipient, NextOffset: nextOffset}
	_, err := proposeOrApply(ctx, b, raftCommandDirectAck, req, b.applyDirectAck)
	return err
}

func (b *Broker) JoinConsumerGroup(ctx context.Context, group, memberID string, topics []string, sessionTimeoutMS uint64) (store.ConsumerGroupMember, error) {
	req := joinGroupCommand{Group: group, MemberID: memberID, Topics: append([]string(nil), topics...), SessionTimeoutMS: sessionTimeoutMS}
	return proposeOrApply(ctx, b, raftCommandJoinGroup, req, b.applyJoinGroup)
}

func (b *Broker) HeartbeatConsumerGroup(ctx context.Context, group, memberID string, sessionTimeoutMS *uint64) (store.ConsumerGroupMember, error) {
	req := heartbeatGroupCommand{Group: group, MemberID: memberID, SessionTimeoutMS: sessionTimeoutMS}
	return proposeOrApply(ctx, b, raftCommandHeartbeatGroup, req, b.applyHeartbeatGroup)
}

func (b *Broker) RebalanceConsumerGroup(ctx context.Context, group string) (store.ConsumerGroupAssignment, error) {
	req := rebalanceGroupCommand{Group: group}
	return proposeOrApply(ctx, b, raftCommandRebalanceGroup, req, b.applyRebalanceGroup)
}

func (b *Broker) GetConsumerGroupAssignment(group string) (*store.ConsumerGroupAssignment, error) {
	assignment, err := b.meta.LoadGroupAssignment(group)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group assignment")
	}
	return assignment, nil
}

func (b *Broker) FetchConsumerGroup(group, memberID string, generation uint64, topic string, partition uint32, offset *uint64, maxRecords int) (store.PollResult, error) {
	_ = memberID
	_ = generation
	return b.Fetch(groupConsumer(group), topic, partition, offset, maxRecords)
}

func (b *Broker) CommitConsumerGroupOffset(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32, nextOffset uint64) error {
	_ = memberID
	_ = generation
	return b.CommitOffset(ctx, groupConsumer(group), topic, partition, nextOffset)
}

func (b *Broker) applyCreateTopic(ctx context.Context, topic store.TopicConfig) (store.TopicConfig, error) {
	normalizeTopicPolicies(&topic)
	if isInternalTopicName(topic.Name) {
		return store.TopicConfig{}, brokerStoreError(store.CodeInvalidArgument, "topic name %s is reserved for internal broker use", topic.Name)
	}
	if err := b.queue.CreateTopic(topic); err != nil {
		return store.TopicConfig{}, wrapBroker("create_topic_failed", err, "create topic")
	}
	b.publishEvent(ctx, TopicCreatedEvent{Topic: topic.Name, Partitions: topic.Partitions})
	return topic, nil
}

func (b *Broker) applyProduce(ctx context.Context, req produceCommand) (ProduceResult, error) {
	topic, err := b.meta.LoadTopicConfig(req.Topic)
	if err != nil {
		return ProduceResult{}, wrapBrokerStore(err, "load topic config")
	}
	if topic == nil {
		return ProduceResult{}, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", req.Topic)
	}
	partition, err := b.router.selectPartition(*topic, req.Partitioning, req.Key)
	if err != nil {
		return ProduceResult{}, err
	}
	appendRecord := store.NewRecordAppend(req.Payload)
	appendRecord.Key = append([]byte(nil), req.Key...)
	if req.Tombstone {
		appendRecord.Attributes |= store.RecordAttributeTombstone
	}
	record, err := b.queue.PublishRecord(req.Topic, partition, appendRecord)
	if err != nil {
		return ProduceResult{}, wrapBroker("publish_record_failed", err, "publish record")
	}
	b.publishEvent(ctx, RecordProducedEvent{
		Topic:      req.Topic,
		Partition:  partition,
		Offset:     record.Offset,
		NextOffset: record.Offset + 1,
	})
	return ProduceResult{Partition: partition, Record: record}, nil
}

func (b *Broker) applyProduceBatch(ctx context.Context, req produceBatchCommand) (ProduceBatchResult, error) {
	topic, err := b.meta.LoadTopicConfig(req.Topic)
	if err != nil {
		return ProduceBatchResult{}, wrapBrokerStore(err, "load topic config")
	}
	if topic == nil {
		return ProduceBatchResult{}, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", req.Topic)
	}
	totalBytes := 0
	for _, record := range req.Records {
		totalBytes += len(record.Payload)
	}
	if totalBytes > b.cfg.Broker.MaxBatchPayloadBytes {
		return ProduceBatchResult{}, brokerStoreError(store.CodeInvalidArgument, "batch payload size %d exceeds limit %d", totalBytes, b.cfg.Broker.MaxBatchPayloadBytes)
	}
	partition, err := b.router.selectPartition(*topic, req.Partitioning, firstRecordKey(req.Records))
	if err != nil {
		return ProduceBatchResult{}, err
	}
	records, err := b.queue.PublishBatchRecords(req.Topic, partition, req.Records)
	if err != nil {
		return ProduceBatchResult{}, wrapBroker("publish_batch_failed", err, "publish record batch")
	}
	for _, record := range records {
		b.publishEvent(ctx, RecordProducedEvent{Topic: req.Topic, Partition: partition, Offset: record.Offset, NextOffset: record.Offset + 1})
	}
	return ProduceBatchResult{Partition: partition, Records: records}, nil
}

func (b *Broker) applyCommitOffset(ctx context.Context, req commitOffsetCommand) (struct{}, error) {
	_ = ctx
	return struct{}{}, wrapBroker("commit_offset_failed", b.queue.Ack(req.Consumer, req.Topic, req.Partition, req.NextOffset), "commit offset")
}

func (b *Broker) applyDirectSend(ctx context.Context, req directCommand) (direct.SendResult, error) {
	result, err := b.direct.Send(req.Sender, req.Recipient, req.ConversationID, req.Payload)
	if err != nil {
		return direct.SendResult{}, wrapBroker("direct_send_failed", err, "send direct message")
	}
	b.publishEvent(ctx, DirectMessageSentEvent{Sender: req.Sender, Recipient: req.Recipient, Offset: result.Offset})
	return result, nil
}

func (b *Broker) applyDirectAck(ctx context.Context, req ackDirectCommand) (struct{}, error) {
	_ = ctx
	return struct{}{}, wrapBroker("direct_ack_failed", b.direct.AckInbox(req.Recipient, req.NextOffset), "ack direct inbox")
}

func (b *Broker) applyJoinGroup(ctx context.Context, req joinGroupCommand) (store.ConsumerGroupMember, error) {
	_ = ctx
	if req.Group == "" || req.MemberID == "" {
		return store.ConsumerGroupMember{}, brokerStoreError(store.CodeInvalidArgument, "group and member_id are required")
	}
	now := store.NowMS()
	sessionTimeout := req.SessionTimeoutMS
	if sessionTimeout == 0 {
		sessionTimeout = 30000
	}
	member := store.ConsumerGroupMember{
		Group:            req.Group,
		MemberID:         req.MemberID,
		Topics:           append([]string(nil), req.Topics...),
		SessionTimeoutMS: sessionTimeout,
		JoinedAtMS:       now,
		LastHeartbeatMS:  now,
	}
	if err := b.meta.SaveGroupMember(member); err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "save group member")
	}
	return member, nil
}

func (b *Broker) applyHeartbeatGroup(ctx context.Context, req heartbeatGroupCommand) (store.ConsumerGroupMember, error) {
	_ = ctx
	member, err := b.meta.LoadGroupMember(req.Group, req.MemberID)
	if err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "load group member")
	}
	if member == nil {
		return store.ConsumerGroupMember{}, brokerStoreError(store.CodeInvalidArgument, "group member %s/%s not found", req.Group, req.MemberID)
	}
	if req.SessionTimeoutMS != nil && *req.SessionTimeoutMS > 0 {
		member.SessionTimeoutMS = *req.SessionTimeoutMS
	}
	member.LastHeartbeatMS = store.NowMS()
	if err := b.meta.SaveGroupMember(*member); err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "save group member heartbeat")
	}
	return *member, nil
}

func (b *Broker) applyRebalanceGroup(ctx context.Context, req rebalanceGroupCommand) (store.ConsumerGroupAssignment, error) {
	_ = ctx
	now := store.NowMS()
	if _, err := b.meta.DeleteExpiredGroupMembers(now); err != nil {
		return store.ConsumerGroupAssignment{}, wrapBrokerStore(err, "delete expired group members")
	}
	members, err := b.meta.ListGroupMembers(req.Group)
	if err != nil {
		return store.ConsumerGroupAssignment{}, wrapBrokerStore(err, "list group members")
	}
	active := collectionlist.FilterList(
		collectionlist.NewList(members...),
		func(_ int, member store.ConsumerGroupMember) bool {
			return !member.ExpiredAt(now)
		},
	).Sort(func(left, right store.ConsumerGroupMember) int {
		return cmp.Compare(left.MemberID, right.MemberID)
	}).Values()
	previous, err := b.meta.LoadGroupAssignment(req.Group)
	if err != nil {
		return store.ConsumerGroupAssignment{}, wrapBrokerStore(err, "load group assignment")
	}
	generation := uint64(1)
	if previous != nil {
		generation = previous.Generation + 1
	}
	assignments := make([]store.GroupPartitionAssignment, 0)
	if len(active) > 0 {
		partitions, err := b.groupPartitions(active)
		if err != nil {
			return store.ConsumerGroupAssignment{}, err
		}
		for i, tp := range partitions {
			owner := active[i%len(active)]
			assignments = append(assignments, store.GroupPartitionAssignment{
				MemberID:  owner.MemberID,
				Topic:     tp.Topic,
				Partition: tp.Partition,
			})
		}
	}
	assignment := store.ConsumerGroupAssignment{
		Group:       req.Group,
		Generation:  generation,
		Assignments: assignments,
		UpdatedAtMS: now,
	}
	if err := b.meta.SaveGroupAssignment(assignment); err != nil {
		return store.ConsumerGroupAssignment{}, wrapBrokerStore(err, "save group assignment")
	}
	return assignment, nil
}

func (b *Broker) groupPartitions(members []store.ConsumerGroupMember) ([]store.TopicPartition, error) {
	wanted := collectionset.NewSet[string]()
	for _, member := range members {
		wanted.Add(member.Topics...)
	}
	topics, err := b.meta.ListTopics()
	if err != nil {
		return nil, wrapBrokerStore(err, "list topics for group partitions")
	}
	out := make([]store.TopicPartition, 0)
	for i := range topics {
		topic := topics[i]
		if !wanted.Contains(topic.Name) {
			continue
		}
		for partition := range topic.Partitions {
			out = append(out, store.NewTopicPartition(topic.Name, partition))
		}
	}
	return collectionlist.NewList(out...).
		Sort(func(left, right store.TopicPartition) int {
			if left.Topic == right.Topic {
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).
		Values(), nil
}

func (b *Broker) applyRaftCommand(data []byte) (any, error) {
	var cmd raftCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, wrapBroker("raft_command_decode_failed", err, "decode raft command")
	}
	ctx := context.Background()
	handler, ok := b.raftCommandHandlers().Get(cmd.Type)
	if !ok {
		return nil, brokerStoreError(store.CodeCodec, "unknown raft command %s", cmd.Type)
	}
	return handler(ctx, cmd.Payload)
}

type raftCommandHandler func(context.Context, json.RawMessage) (any, error)

func (b *Broker) raftCommandHandlers() *collectionmapping.Map[string, raftCommandHandler] {
	handlers := collectionmapping.NewMap[string, raftCommandHandler]()
	handlers.Set(raftCommandCreateTopic, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyCreateTopic, "decode create topic command")
	})
	handlers.Set(raftCommandProduce, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyProduce, "decode produce command")
	})
	handlers.Set(raftCommandProduceBatch, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyProduceBatch, "decode produce batch command")
	})
	handlers.Set(raftCommandCommitOffset, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyCommitOffset, "decode commit offset command")
	})
	handlers.Set(raftCommandDirectSend, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyDirectSend, "decode direct send command")
	})
	handlers.Set(raftCommandDirectAck, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyDirectAck, "decode direct ack command")
	})
	handlers.Set(raftCommandJoinGroup, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyJoinGroup, "decode join group command")
	})
	handlers.Set(raftCommandHeartbeatGroup, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyHeartbeatGroup, "decode heartbeat group command")
	})
	handlers.Set(raftCommandRebalanceGroup, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyRebalanceGroup, "decode rebalance group command")
	})
	handlers.Set(raftCommandNack, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyNack, "decode nack command")
	})
	handlers.Set(raftCommandProcessRetry, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyProcessRetryBatch, "decode process retry command")
	})
	handlers.Set(raftCommandScheduleDelay, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyScheduleDelay, "decode schedule delay command")
	})
	handlers.Set(raftCommandProcessDelay, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, b.applyProcessDelayPartition, "decode process delay command")
	})
	return handlers
}

func decodeRaftCommand[R any, T any](ctx context.Context, payload json.RawMessage, apply func(context.Context, R) (T, error), label string) (any, error) {
	var req R
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, wrapBroker("raft_payload_decode_failed", err, "%s", label)
	}
	return apply(ctx, req)
}

func proposeOrApply[T any, R any](ctx context.Context, b *Broker, commandType string, req R, apply func(context.Context, R) (T, error)) (T, error) {
	b.raftMu.RLock()
	node := b.raft
	b.raftMu.RUnlock()
	if !b.cfg.Raft.Enabled || node == nil {
		return apply(ctx, req)
	}
	var zero T
	value, err := node.Apply(ctx, commandType, req)
	if err != nil {
		return zero, err
	}
	typed, ok := value.(T)
	if ok {
		return typed, nil
	}
	marshaled, err := json.Marshal(value)
	if err != nil {
		return zero, wrapBroker("raft_result_encode_failed", err, "encode raft result")
	}
	if err := json.Unmarshal(marshaled, &typed); err != nil {
		return zero, wrapBroker("raft_result_decode_failed", err, "decode raft result")
	}
	return typed, nil
}

func normalizeTopicPolicies(topic *store.TopicConfig) {
	if topic.Name == "" {
		return
	}
	defaults := store.NewTopicConfig(topic.Name)
	if topic.Partitions == 0 {
		topic.Partitions = defaults.Partitions
	}
	if topic.SegmentMaxBytes == 0 {
		topic.SegmentMaxBytes = defaults.SegmentMaxBytes
	}
	if topic.IndexIntervalBytes == 0 {
		topic.IndexIntervalBytes = defaults.IndexIntervalBytes
	}
	if topic.RetentionMaxBytes == 0 {
		topic.RetentionMaxBytes = defaults.RetentionMaxBytes
	}
	if topic.CleanupPolicy == "" {
		topic.CleanupPolicy = defaults.CleanupPolicy
	}
	if topic.MaxMessageBytes == 0 {
		topic.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if topic.MaxBatchBytes == 0 {
		topic.MaxBatchBytes = defaults.MaxBatchBytes
	}
	if topic.RetryPolicy.MaxAttempts == 0 {
		topic.RetryPolicy = defaults.RetryPolicy
	}
}

func groupConsumer(group string) string {
	return "__group_offset/" + group
}

func firstRecordKey(records []store.RecordAppend) []byte {
	for _, record := range records {
		if len(record.Key) > 0 {
			return record.Key
		}
	}
	return nil
}

func cloneAppend(record store.RecordAppend) store.RecordAppend {
	out := store.RecordAppend{
		Attributes: record.Attributes,
		Payload:    append([]byte(nil), record.Payload...),
		Key:        append([]byte(nil), record.Key...),
	}
	if record.TimestampMS != nil {
		v := *record.TimestampMS
		out.TimestampMS = &v
	}
	if len(record.Headers) > 0 {
		out.Headers = make([]store.RecordHeader, len(record.Headers))
		for i, header := range record.Headers {
			out.Headers[i] = store.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)}
		}
	}
	return out
}

func fetchRecordsFromStore(records []store.Record) []protocol.FetchRecord {
	out := make([]protocol.FetchRecord, 0, len(records))
	for _, record := range records {
		out = append(out, protocol.FetchRecord{
			Offset:      record.Offset,
			TimestampMS: record.TimestampMS,
			Key:         append([]byte(nil), record.Key...),
			Tombstone:   record.IsTombstone(),
			Payload:     append([]byte(nil), record.Payload...),
		})
	}
	return out
}

func (b *Broker) publishEvent(ctx context.Context, event eventx.Event) {
	if b.events == nil {
		return
	}
	if err := b.events.Publish(ctx, event); err != nil && b.logger != nil {
		b.logger.Warn("publish event failed", "event", event.Name(), "error", err)
	}
}

type produceCommand struct {
	Topic        string
	Partitioning PublishPartitioning
	Key          []byte
	Tombstone    bool
	Payload      []byte
}

type produceBatchCommand struct {
	Topic        string
	Partitioning PublishPartitioning
	Records      []store.RecordAppend
}

type commitOffsetCommand struct {
	Consumer   string
	Topic      string
	Partition  uint32
	NextOffset uint64
}

type directCommand struct {
	Sender         string
	Recipient      string
	ConversationID *string
	Payload        []byte
}

type ackDirectCommand struct {
	Recipient  string
	NextOffset uint64
}

type joinGroupCommand struct {
	Group            string
	MemberID         string
	Topics           []string
	SessionTimeoutMS uint64
}

type heartbeatGroupCommand struct {
	Group            string
	MemberID         string
	SessionTimeoutMS *uint64
}

type rebalanceGroupCommand struct {
	Group string
}
