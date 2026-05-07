package broker

import (
	"context"

	"github.com/samber/oops"
)

type healthOutput struct {
	Body RuntimeHealth `json:"body"`
}

type topicsOutput struct {
	Body struct {
		Topics []TopicSummary `json:"topics"`
	} `json:"body"`
}

type metricsOutput struct {
	Body struct {
		Snapshot MetricsSnapshot `json:"snapshot"`
		Runtime  string          `json:"runtime"`
	} `json:"body"`
}

func (s *AdminServer) apiHealth(ctx context.Context, _ *struct{}) (*healthOutput, error) {
	_ = ctx
	out := &healthOutput{}
	out.Body = s.broker.RuntimeHealth()
	return out, nil
}

func (s *AdminServer) apiTopics(ctx context.Context, _ *struct{}) (*topicsOutput, error) {
	_ = ctx
	topics, err := s.broker.TopicSummaries()
	if err != nil {
		return nil, wrapBroker("list_topics_failed", err, "list topics")
	}
	out := &topicsOutput{}
	out.Body.Topics = topics
	return out, nil
}

func (s *AdminServer) apiMetrics(ctx context.Context, _ *struct{}) (*metricsOutput, error) {
	if err := s.metrics.RefreshStream(ctx, s.broker); err != nil {
		return nil, oops.In("admin").Code("list_topics_failed").Wrapf(err, "build metrics")
	}
	out := &metricsOutput{}
	out.Body.Snapshot = s.metrics.Snapshot()
	out.Body.Runtime = s.broker.RuntimeHealth().RuntimeMode
	return out, nil
}
