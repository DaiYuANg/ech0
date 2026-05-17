package broker

import "context"

func (s *TCPServer) acquireInflightRequest(ctx context.Context) (func(), error) {
	if s == nil || s.broker == nil || s.broker.quotaUsage == nil {
		return func() {}, nil
	}
	return s.broker.quotaUsage.beginInflight(ctx, s.broker, identityFromContext(ctx))
}
