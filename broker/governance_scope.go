package broker

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) scopedTopicConfig(ctx context.Context, topic store.TopicConfig) (Identity, store.TopicConfig) {
	identity := b.identity(ctx)
	topic = b.applyTenantTopicDefaults(identity, topic)
	topic.Name = scopedTopicName(identity, topic.Name)
	if topic.DeadLetterTopic != nil {
		value := scopedTopicName(identity, *topic.DeadLetterTopic)
		topic.DeadLetterTopic = &value
	}
	return identity, topic
}

func (b *Broker) visibleTopicConfig(identity Identity, topic store.TopicConfig) store.TopicConfig {
	topic.Name = visibleTopicName(identity, topic.Name)
	if topic.DeadLetterTopic != nil {
		value := visibleTopicName(identity, *topic.DeadLetterTopic)
		topic.DeadLetterTopic = &value
	}
	return topic
}

func scopedTopicName(identity Identity, name string) string {
	identity = normalizeIdentity(identity)
	name = strings.TrimSpace(name)
	if defaultScope(identity) {
		return name
	}
	prefix, source, ok := splitAuxTopicName(name)
	if ok {
		return prefix + "." + scopedName(identity, "topic", source)
	}
	return scopedName(identity, "topic", name)
}

func visibleTopicName(identity Identity, name string) string {
	identity = normalizeIdentity(identity)
	name = strings.TrimSpace(name)
	if defaultScope(identity) {
		return name
	}
	prefix, source, ok := splitAuxTopicName(name)
	if ok {
		return prefix + "." + visibleName(identity, "topic", source)
	}
	return visibleName(identity, "topic", name)
}

func splitAuxTopicName(name string) (string, string, bool) {
	for _, prefix := range []string{internalRetryTopicPrefix, internalDelayTopicPrefix, internalDLQTopicPrefix} {
		source, ok := strings.CutPrefix(name, prefix+".")
		if ok {
			return prefix, source, true
		}
	}
	return "", "", false
}

func scopedName(identity Identity, kind, name string) string {
	identity = normalizeIdentity(identity)
	name = strings.TrimSpace(name)
	if defaultScope(identity) {
		return name
	}
	return scopedKeyPrefix + hexComponent(identity.Tenant) + "/" + hexComponent(identity.Namespace) + "/" + kind + "/" + name
}

func visibleName(identity Identity, kind, name string) string {
	identity = normalizeIdentity(identity)
	name = strings.TrimSpace(name)
	if defaultScope(identity) {
		return name
	}
	prefix := scopedKeyPrefix + hexComponent(identity.Tenant) + "/" + hexComponent(identity.Namespace) + "/" + kind + "/"
	return strings.TrimPrefix(name, prefix)
}

func nameInScope(identity Identity, kind, name string) bool {
	identity = normalizeIdentity(identity)
	name = strings.TrimSpace(name)
	if defaultScope(identity) {
		return !strings.HasPrefix(name, scopedKeyPrefix)
	}
	prefix := scopedKeyPrefix + hexComponent(identity.Tenant) + "/" + hexComponent(identity.Namespace) + "/" + kind + "/"
	return strings.HasPrefix(name, prefix)
}

func defaultScope(identity Identity) bool {
	return identity.Tenant == DefaultTenant && identity.Namespace == DefaultNamespace
}

func hexComponent(value string) string {
	return hex.EncodeToString([]byte(value))
}

func nonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
