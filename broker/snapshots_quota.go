package broker

import "context"

func (b *Broker) QuotaSummary() (QuotaSummary, error) {
	return b.QuotaSummaryFor(context.Background())
}

func (b *Broker) QuotaSummaryFor(ctx context.Context) (QuotaSummary, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionDescribe, ACLResource{Type: ACLResourceNamespace, Tenant: identity.Tenant, Namespace: identity.Namespace, Name: identity.Namespace}); err != nil {
		return QuotaSummary{}, err
	}
	req := QuotaRequest{Identity: identity}
	if err := b.populateTopicQuotaUsage(identity, &req); err != nil {
		return QuotaSummary{}, err
	}
	if err := b.populateStorageQuotaUsage(identity, &req); err != nil {
		return QuotaSummary{}, err
	}
	if b.quotaUsage != nil {
		req.CurrentConnections = b.quotaUsage.connectionCount(identity)
		req.CurrentInflightRequests = b.quotaUsage.inflightCount(identity)
	}
	return QuotaSummary{
		Identity:                identity,
		Limits:                  b.cfg.Governance.Quota,
		CurrentTopics:           req.CurrentTopics,
		CurrentPartitions:       req.CurrentPartitions,
		CurrentStorageBytes:     req.CurrentStorageBytes,
		CurrentConnections:      req.CurrentConnections,
		CurrentInflightRequests: req.CurrentInflightRequests,
	}, nil
}
