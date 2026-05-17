package broker

import (
	"context"
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

type quotaUsageTracker struct {
	mu          sync.Mutex
	connections *collectionmapping.Map[string, int64]
	inflight    *collectionmapping.Map[string, int64]
}

func newQuotaUsageTracker() *quotaUsageTracker {
	return &quotaUsageTracker{
		connections: collectionmapping.NewMap[string, int64](),
		inflight:    collectionmapping.NewMap[string, int64](),
	}
}

func (t *quotaUsageTracker) acquireConnection(ctx context.Context, b *Broker, identity Identity) error {
	if t == nil || b == nil {
		return nil
	}
	key := quotaIdentityKey(identity)
	t.mu.Lock()
	defer t.mu.Unlock()
	current := t.connections.GetOrDefault(key, 0)
	if err := b.checkQuota(ctx, QuotaRequest{
		Identity:           identity,
		Action:             QuotaActionConnect,
		CurrentConnections: current,
	}); err != nil {
		return err
	}
	t.connections.Set(key, current+1)
	return nil
}

func (t *quotaUsageTracker) releaseConnection(identity Identity) {
	if t == nil {
		return
	}
	key := quotaIdentityKey(identity)
	t.mu.Lock()
	defer t.mu.Unlock()
	current := t.connections.GetOrDefault(key, 0)
	if current <= 1 {
		t.connections.Delete(key)
		return
	}
	t.connections.Set(key, current-1)
}

func (t *quotaUsageTracker) beginInflight(ctx context.Context, b *Broker, identity Identity) (func(), error) {
	if t == nil || b == nil {
		return func() {}, nil
	}
	key := quotaIdentityKey(identity)
	t.mu.Lock()
	current := t.inflight.GetOrDefault(key, 0)
	if err := b.checkQuota(ctx, QuotaRequest{
		Identity:                identity,
		Action:                  QuotaActionInflight,
		CurrentInflightRequests: current,
	}); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	t.inflight.Set(key, current+1)
	t.mu.Unlock()
	return func() {
		t.finishInflight(identity)
	}, nil
}

func (t *quotaUsageTracker) finishInflight(identity Identity) {
	if t == nil {
		return
	}
	key := quotaIdentityKey(identity)
	t.mu.Lock()
	defer t.mu.Unlock()
	current := t.inflight.GetOrDefault(key, 0)
	if current <= 1 {
		t.inflight.Delete(key)
		return
	}
	t.inflight.Set(key, current-1)
}

func (t *quotaUsageTracker) connectionCount(identity Identity) int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.connections.GetOrDefault(quotaIdentityKey(identity), 0)
}

func (t *quotaUsageTracker) inflightCount(identity Identity) int64 {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inflight.GetOrDefault(quotaIdentityKey(identity), 0)
}

func quotaIdentityKey(identity Identity) string {
	identity = normalizeIdentity(identity)
	return identity.Tenant + "/" + identity.Namespace + "/" + identity.Principal
}
