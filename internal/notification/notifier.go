package notification

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	dlqTopic        = "payment.events.dlq"
	maxSendAttempts = 3
)

// Notifier turns payment events into notifications.
type Notifier struct {
	store  *Store
	sender Sender
	kafka  *kgo.Client // producer, used for the dead-letter topic
	log    *slog.Logger
}

func NewNotifier(store *Store, sender Sender, kafka *kgo.Client, log *slog.Logger) *Notifier {
	return &Notifier{store: store, sender: sender, kafka: kafka, log: log}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Handle processes a batch. It is idempotent, so redelivery is harmless.
func (n *Notifier) Handle(ctx context.Context, recs []*kgo.Record) error {
	for _, r := range recs {
		if err := n.handleOne(ctx, r); err != nil {
			return err // transient: the whole batch is retried
		}
	}
	return nil
}

func (n *Notifier) handleOne(ctx context.Context, r *kgo.Record) error {
	var ev paymentEvent
	if err := json.Unmarshal(r.Value, &ev); err != nil {
		return n.deadLetter(ctx, r, "undecodable event: "+err.Error())
	}
	subject, body, ok := render(ev)
	if !ok {
		return nil // not an event we notify about
	}
	if _, err := uuid.Parse(ev.PaymentID); err != nil {
		return n.deadLetter(ctx, r, "invalid payment_id")
	}
	if _, err := uuid.Parse(ev.UserID); err != nil {
		return n.deadLetter(ctx, r, "invalid user_id")
	}

	status, err := n.store.Claim(ctx, ev.PaymentID, ev.UserID, ev.Type)
	if err != nil {
		return err
	}
	if status != StatusPending {
		return nil // already sent, or already dead-lettered
	}

	var sendErr error
	for attempt := 1; attempt <= maxSendAttempts; attempt++ {
		sendErr = n.sender.Send(ctx, ev.UserID, subject, body)
		if sendErr == nil {
			return n.store.MarkSent(ctx, ev.PaymentID, ev.Type, attempt)
		}
		n.log.Warn("send failed", "payment_id", ev.PaymentID, "attempt", attempt, "error", sendErr)
		if attempt < maxSendAttempts {
			sleep(ctx, time.Duration(attempt)*time.Second) // 1s, then 2s
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	// Out of attempts: park the event for a human, then remember we did.
	if err := n.deadLetter(ctx, r, "send failed: "+sendErr.Error()); err != nil {
		return err
	}
	return n.store.MarkFailed(ctx, ev.PaymentID, ev.Type, maxSendAttempts, sendErr.Error())
}

func (n *Notifier) deadLetter(ctx context.Context, r *kgo.Record, reason string) error {
	rec := &kgo.Record{
		Topic: dlqTopic,
		Key:   r.Key,
		Value: r.Value,
		Headers: []kgo.RecordHeader{
			{Key: "error", Value: []byte(reason)},
			{Key: "source_topic", Value: []byte(r.Topic)},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
		},
	}
	if err := n.kafka.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("publish to dead-letter topic: %w", err)
	}
	n.log.Error("event dead-lettered", "reason", reason, "partition", r.Partition, "offset", r.Offset)
	return nil
}
