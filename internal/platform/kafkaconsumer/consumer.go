package kafkaconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	recordsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_records_total", Help: "Records handled by this consumer.",
	})
	eventAge = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_event_age_seconds",
		Help:    "Time from event creation to handling (a proxy for consumer lag).",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 300},
	})
)

// Handler processes a batch. It must be idempotent: batches can be delivered
// more than once. Return an error only for transient failures (the batch is
// retried); skip poison records yourself and return nil.
type Handler func(ctx context.Context, recs []*kgo.Record) error

type Consumer struct {
	client *kgo.Client
	log    *slog.Logger
}

func New(brokers []string, group, topic string, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client, log: log.With("group", group, "topic", topic)}, nil
}

func (c *Consumer) Close() {
	c.client.Close()
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Run consumes until ctx is cancelled. Offsets are committed only after the
// handler has succeeded (at-least-once).
func (c *Consumer) Run(ctx context.Context, handle Handler) {
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if ctx.Err() == nil {
				c.log.Error("kafka fetch error", "partition", partition, "error", err)
			}
		})

		var recs []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			continue
		}

		for ctx.Err() == nil {
			if err := handle(ctx, recs); err != nil {
				c.log.Error("handling batch failed, retrying", "error", err)
				sleep(ctx, 2*time.Second)
				continue
			}
			break
		}
		if ctx.Err() != nil {
			return
		}

		for _, r := range recs {
			eventAge.Observe(time.Since(r.Timestamp).Seconds())
		}
		recordsTotal.Add(float64(len(recs)))

		if err := c.client.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
			c.log.Error("commit offsets failed (batch will be redelivered)", "error", err)
		}
	}
}
