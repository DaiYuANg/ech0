package broker

import (
	"context"
	"fmt"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
	"golang.org/x/time/rate"
)

type configuredQuotaLimiter struct {
	cfg      QuotaConfig
	limiters *collectionmapping.ShardedConcurrentMap[string, *rate.Limiter]
}

func newConfiguredQuotaLimiter(cfg QuotaConfig) QuotaLimiter {
	if quotaConfigEmpty(cfg) {
		return UnlimitedQuotaLimiter{}
	}
	return &configuredQuotaLimiter{
		cfg:      cfg,
		limiters: collectionmapping.NewShardedConcurrentMap[string, *rate.Limiter](64, collectionmapping.HashString),
	}
}

func quotaConfigEmpty(cfg QuotaConfig) bool {
	return cfg.MaxTopics == 0 &&
		cfg.MaxPartitions == 0 &&
		cfg.MaxMessageBytes == 0 &&
		cfg.MaxBatchBytes == 0 &&
		cfg.MaxConnections == 0 &&
		cfg.ProduceRateLimitPerSecond == 0 &&
		cfg.ConsumeRateLimitPerSecond == 0 &&
		cfg.RequestRateLimitPerSecond == 0 &&
		cfg.MaxInflightRequests == 0 &&
		cfg.MaxStorageBytes == 0
}

func (l *configuredQuotaLimiter) CheckQuota(_ context.Context, req QuotaRequest) error {
	if l == nil {
		return nil
	}
	if err := l.checkStatic(req); err != nil {
		return err
	}
	return l.checkRate(req)
}

func quotaExceeded(format string, args ...any) error {
	return brokerStoreError(store.CodeUnavailable, format, args...)
}

func (b *Broker) populateTopicQuotaUsage(identity Identity, req *QuotaRequest) error {
	if req == nil || !topicQuotaUsageNeeded(b.cfg.Governance.Quota) {
		return nil
	}
	topics, err := b.queue.ListTopics()
	if err != nil {
		return wrapBroker("quota_topic_usage_failed", err, "load topic quota usage")
	}
	for index := range topics {
		topic := &topics[index]
		if nameInScope(identity, "topic", topic.Name) && !isInternalTopicName(visibleTopicName(identity, topic.Name)) {
			req.CurrentTopics++
			req.CurrentPartitions += int(topic.Partitions)
		}
	}
	return nil
}

func topicQuotaUsageNeeded(cfg QuotaConfig) bool {
	return cfg.MaxTopics > 0 || cfg.MaxPartitions > 0
}

func (b *Broker) populateStorageQuotaUsage(identity Identity, req *QuotaRequest) error {
	if req == nil || !storageQuotaUsageNeeded(b.cfg.Governance.Quota) {
		return nil
	}
	usage, err := b.storageUsageForScope(identity)
	if err != nil {
		return err
	}
	req.CurrentStorageBytes = usage
	return nil
}

func (b *Broker) storageUsageForScope(identity Identity) (uint64, error) {
	if b == nil || b.queue == nil {
		return 0, nil
	}
	topics, err := b.queue.ListTopics()
	if err != nil {
		return 0, wrapBroker("quota_storage_topics_failed", err, "load topics for storage quota usage")
	}
	var total uint64
	for index := range topics {
		topic := topics[index]
		if !topicStorageInScope(identity, topic.Name) {
			continue
		}
		usage, usageErr := b.queue.StorageUsage(topic.Name)
		if usageErr != nil {
			return 0, wrapBroker("quota_storage_usage_failed", usageErr, "load storage quota usage")
		}
		total += usage
	}
	return total, nil
}

func topicStorageInScope(identity Identity, topic string) bool {
	if nameInScope(identity, "topic", topic) {
		return true
	}
	_, source, ok := splitAuxTopicName(topic)
	return ok && nameInScope(identity, "topic", source)
}

func (l *configuredQuotaLimiter) checkStatic(req QuotaRequest) error {
	switch req.Action {
	case QuotaActionCreateTopic:
		return l.checkCreateTopicQuota(req)
	case QuotaActionProduce:
		return l.checkProduceQuota(req)
	case QuotaActionConnect:
		return l.checkConnectionQuota(req)
	case QuotaActionInflight:
		return l.checkInflightQuota(req)
	case QuotaActionConsume, QuotaActionRequest:
		return nil
	}
	return nil
}

func (l *configuredQuotaLimiter) checkCreateTopicQuota(req QuotaRequest) error {
	if l.cfg.MaxTopics > 0 && req.CurrentTopics+1 > l.cfg.MaxTopics {
		return quotaExceeded("topic count %d exceeds max_topics %d", req.CurrentTopics+1, l.cfg.MaxTopics)
	}
	nextPartitions := req.CurrentPartitions + int(req.Partitions)
	if l.cfg.MaxPartitions > 0 && nextPartitions > l.cfg.MaxPartitions {
		return quotaExceeded("partition count %d exceeds max_partitions %d", nextPartitions, l.cfg.MaxPartitions)
	}
	return nil
}

func (l *configuredQuotaLimiter) checkProduceQuota(req QuotaRequest) error {
	if l.cfg.MaxMessageBytes > 0 && req.Records <= 1 && req.Bytes > l.cfg.MaxMessageBytes {
		return quotaExceeded("message bytes %d exceeds max_message_bytes %d", req.Bytes, l.cfg.MaxMessageBytes)
	}
	if l.cfg.MaxBatchBytes > 0 && req.Records > 1 && req.Bytes > l.cfg.MaxBatchBytes {
		return quotaExceeded("batch bytes %d exceeds max_batch_bytes %d", req.Bytes, l.cfg.MaxBatchBytes)
	}
	if l.cfg.MaxStorageBytes > 0 && req.CurrentStorageBytes+req.AdditionalStorageBytes > l.cfg.MaxStorageBytes {
		return quotaExceeded("storage bytes %d exceeds max_storage_bytes %d", req.CurrentStorageBytes+req.AdditionalStorageBytes, l.cfg.MaxStorageBytes)
	}
	return nil
}

func (l *configuredQuotaLimiter) checkConnectionQuota(req QuotaRequest) error {
	if l.cfg.MaxConnections > 0 && req.CurrentConnections+1 > l.cfg.MaxConnections {
		return quotaExceeded("connection count %d exceeds max_connections %d", req.CurrentConnections+1, l.cfg.MaxConnections)
	}
	return nil
}

func (l *configuredQuotaLimiter) checkInflightQuota(req QuotaRequest) error {
	if l.cfg.MaxInflightRequests > 0 && req.CurrentInflightRequests+1 > l.cfg.MaxInflightRequests {
		return quotaExceeded("in-flight request count %d exceeds max_inflight_requests %d", req.CurrentInflightRequests+1, l.cfg.MaxInflightRequests)
	}
	return nil
}

func (l *configuredQuotaLimiter) checkRate(req QuotaRequest) error {
	limit := l.rateLimit(req.Action)
	if limit <= 0 {
		return nil
	}
	key := quotaRateKey(req)
	burst := max(int(limit), 1)
	limiter, _ := l.limiters.GetOrStore(key, rate.NewLimiter(rate.Limit(limit), burst))
	if !limiter.AllowN(time.Now(), quotaTokens(req)) {
		return quotaExceeded("%s rate limit exceeded for %s", req.Action, req.Identity.Principal)
	}
	return nil
}

func (l *configuredQuotaLimiter) rateLimit(action QuotaAction) float64 {
	switch action {
	case QuotaActionCreateTopic:
		return 0
	case QuotaActionProduce:
		return l.cfg.ProduceRateLimitPerSecond
	case QuotaActionConsume:
		return l.cfg.ConsumeRateLimitPerSecond
	case QuotaActionRequest:
		return l.cfg.RequestRateLimitPerSecond
	case QuotaActionConnect, QuotaActionInflight:
		return 0
	default:
		return 0
	}
}

func quotaTokens(req QuotaRequest) int {
	if req.Records > 0 {
		return req.Records
	}
	return 1
}

func quotaRateKey(req QuotaRequest) string {
	identity := normalizeIdentity(req.Identity)
	return fmt.Sprintf("%s/%s/%s/%s", identity.Tenant, identity.Namespace, identity.Principal, req.Action)
}

func storageQuotaUsageNeeded(cfg QuotaConfig) bool {
	return cfg.MaxStorageBytes > 0
}
