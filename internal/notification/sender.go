package notification

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
)

// Sender delivers a notification to a user (email, SMS, push...).
type Sender interface {
	Send(ctx context.Context, userID, subject, body string) error
}

// LogSender is a stand-in provider: it logs the message instead of sending it.
// failPercent lets us simulate provider outages while testing retries.
type LogSender struct {
	log         *slog.Logger
	failPercent int
}

func NewLogSender(log *slog.Logger, failPercent int) *LogSender {
	return &LogSender{log: log, failPercent: failPercent}
}

func (s *LogSender) Send(_ context.Context, userID, subject, body string) error {
	if s.failPercent > 0 && rand.IntN(100) < s.failPercent {
		return errors.New("simulated provider outage")
	}
	s.log.Info("notification sent", "user_id", userID, "subject", subject, "body", body)
	return nil
}

type paymentEvent struct {
	Type        string `json:"type"`
	PaymentID   string `json:"payment_id"`
	UserID      string `json:"user_id"`
	AmountPaise int64  `json:"amount_paise"`
	Reason      string `json:"reason"`
}

// render builds the message for an event. ok=false means "not worth notifying".
func render(ev paymentEvent) (subject, body string, ok bool) {
	amount := fmt.Sprintf("Rs %d.%02d", ev.AmountPaise/100, ev.AmountPaise%100)
	switch ev.Type {
	case "payment.completed":
		return "Payment sent", fmt.Sprintf("Your payment of %s was successful.", amount), true
	case "payment.rejected":
		return "Payment declined", fmt.Sprintf("Your payment of %s was declined (%s).", amount, ev.Reason), true
	case "payment.failed":
		return "Payment failed", fmt.Sprintf("Your payment of %s failed (%s).", amount, ev.Reason), true
	default:
		return "", "", false
	}
}
