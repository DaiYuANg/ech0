package broker

import "github.com/lyonbrown4d/ech0/store"

func (b *Broker) applyTenantTopicDefaults(identity Identity, topic store.TopicConfig) store.TopicConfig {
	defaults, ok := b.tenantTopicDefaults(identity)
	if !ok {
		return topic
	}
	base := store.NewTopicConfig(topic.Name)
	topic = applyTenantRetentionDefaults(defaults, topic, base)
	topic = applyTenantDeliveryDefaults(defaults, topic)
	topic = applyTenantMessageExpiryDefaults(defaults, topic)
	topic.RetryPolicy = applyTenantRetryDefaults(defaults.RetryPolicy, topic.RetryPolicy, base.RetryPolicy)
	return topic
}

func applyTenantRetentionDefaults(defaults TenantDefaultsConfig, topic, base store.TopicConfig) store.TopicConfig {
	if defaults.RetentionMaxBytes > 0 && topicDefaultUint64(topic.RetentionMaxBytes, base.RetentionMaxBytes) {
		topic.RetentionMaxBytes = defaults.RetentionMaxBytes
	}
	if defaults.RetentionMS != nil && topic.RetentionMS == nil {
		value := *defaults.RetentionMS
		topic.RetentionMS = &value
	}
	return topic
}

func applyTenantDeliveryDefaults(defaults TenantDefaultsConfig, topic store.TopicConfig) store.TopicConfig {
	if defaults.DeadLetterTopic != "" && topic.DeadLetterTopic == nil {
		value := defaults.DeadLetterTopic
		topic.DeadLetterTopic = &value
	}
	if defaults.DelayEnabled != nil && !topic.DelayEnabled {
		topic.DelayEnabled = *defaults.DelayEnabled
	}
	return topic
}

func applyTenantMessageExpiryDefaults(defaults TenantDefaultsConfig, topic store.TopicConfig) store.TopicConfig {
	if defaults.MessageTTLMS != nil && topic.MessageTTLMS == nil {
		value := *defaults.MessageTTLMS
		topic.MessageTTLMS = &value
	}
	if defaults.MessageExpiryAction != "" && topic.MessageExpiryAction == "" {
		topic.MessageExpiryAction = defaults.MessageExpiryAction
	}
	return topic
}

func (b *Broker) tenantTopicDefaults(identity Identity) (TenantDefaultsConfig, bool) {
	if b == nil {
		return TenantDefaultsConfig{}, false
	}
	identity = normalizeIdentity(identity)
	best := TenantDefaultsConfig{}
	bestScore := -1
	for _, defaults := range b.cfg.Governance.TenantDefaults {
		score, ok := tenantDefaultsMatch(identity, defaults)
		if ok && score >= bestScore {
			best = defaults
			bestScore = score
		}
	}
	return best, bestScore >= 0
}

func tenantDefaultsMatch(identity Identity, defaults TenantDefaultsConfig) (int, bool) {
	score := 0
	if defaults.Tenant != "" {
		if defaults.Tenant != identity.Tenant {
			return 0, false
		}
		score += 2
	}
	if defaults.Namespace != "" {
		if defaults.Namespace != identity.Namespace {
			return 0, false
		}
		score++
	}
	return score, true
}

func applyTenantRetryDefaults(defaults, current, base store.TopicRetryPolicy) store.TopicRetryPolicy {
	if defaults.MaxAttempts > 0 && topicDefaultUint32(current.MaxAttempts, base.MaxAttempts) {
		current.MaxAttempts = defaults.MaxAttempts
	}
	if defaults.BackoffInitialMS > 0 && topicDefaultUint64(current.BackoffInitialMS, base.BackoffInitialMS) {
		current.BackoffInitialMS = defaults.BackoffInitialMS
	}
	if defaults.BackoffMaxMS > 0 && topicDefaultUint64(current.BackoffMaxMS, base.BackoffMaxMS) {
		current.BackoffMaxMS = defaults.BackoffMaxMS
	}
	return current
}

func topicDefaultUint64(value, base uint64) bool {
	return value == 0 || value == base
}

func topicDefaultUint32(value, base uint32) bool {
	return value == 0 || value == base
}
