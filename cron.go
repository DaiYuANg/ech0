package ech0

import (
	"context"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type CronOption func(*cronOptions)

type cronOptions struct {
	name        string
	partition   uint32
	withSeconds bool
	headers     []internalbroker.CronMessageHeader
}

func CronName(name string) CronOption {
	return func(opts *cronOptions) {
		opts.name = name
	}
}

func CronPartition(partition uint32) CronOption {
	return func(opts *cronOptions) {
		opts.partition = partition
	}
}

func CronWithSeconds() CronOption {
	return func(opts *cronOptions) {
		opts.withSeconds = true
	}
}

func CronHeader(key, value string) CronOption {
	return func(opts *cronOptions) {
		if key != "" {
			opts.headers = append(opts.headers, internalbroker.CronMessageHeader{Key: key, Value: value})
		}
	}
}

func (b *Broker) ScheduleCron(ctx context.Context, topic, cron string, payload []byte, opts ...CronOption) error {
	cronOpts := cronOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&cronOpts)
		}
	}
	if b == nil || b.scheduled == nil {
		return oops.In("embedded").Code("schedule_cron_failed").With("topic", topic).New("scheduled runtime is not available")
	}
	err := b.scheduled.ScheduleCronMessage(ctx, internalbroker.CronMessageConfig{
		Name:        cronOpts.name,
		Topic:       topic,
		Partition:   cronOpts.partition,
		Cron:        cron,
		WithSeconds: cronOpts.withSeconds,
		Payload:     string(payload),
		Headers:     cronOpts.headers,
	})
	return oops.In("embedded").Code("schedule_cron_failed").With("topic", topic).Wrapf(err, "schedule cron message")
}
