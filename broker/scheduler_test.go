package broker_test

import (
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
)

func TestScheduledRuntimeRejectsInvalidCronSchedule(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.CronSchedules = []broker.CronMessageConfig{{Topic: "orders", Cron: "not-a-cron"}}
	b := newTestBroker(t)
	_, err := broker.NewScheduledRuntime(cfg, b, nil)
	if err == nil {
		t.Fatal("expected invalid cron schedule to fail")
	}
}
