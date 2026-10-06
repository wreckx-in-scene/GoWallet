package notification

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	StatusPending = "PENDING"
	StatusSent    = "SENT"
	StatusFailed  = "FAILED"
)

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Claim records that we are handling (paymentID, kind) and returns its status.
// Only PENDING means "still to do"; SENT and FAILED mean it was already handled.
func (s *Store) Claim(ctx context.Context, paymentID, userID, kind string) (string, error) {
	_, err := s.db.Exec(ctx, `
		INSERT INTO notifications (payment_id, user_id, kind)
		VALUES ($1::uuid, $2::uuid, $3)
		ON CONFLICT (payment_id, kind) DO NOTHING`, paymentID, userID, kind)
	if err != nil {
		return "", fmt.Errorf("claim notification: %w", err)
	}
	var status string
	err = s.db.QueryRow(ctx,
		`SELECT status FROM notifications WHERE payment_id = $1::uuid AND kind = $2`,
		paymentID, kind).Scan(&status)
	if err != nil {
		return "", fmt.Errorf("read notification status: %w", err)
	}
	return status, nil
}

func (s *Store) MarkSent(ctx context.Context, paymentID, kind string, attempts int) error {
	_, err := s.db.Exec(ctx, `
		UPDATE notifications SET status = 'SENT', attempts = $3, sent_at = now()
		WHERE payment_id = $1::uuid AND kind = $2`, paymentID, kind, attempts)
	if err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	return nil
}

func (s *Store) MarkFailed(ctx context.Context, paymentID, kind string, attempts int, lastErr string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE notifications SET status = 'FAILED', attempts = $3, last_error = $4
		WHERE payment_id = $1::uuid AND kind = $2`, paymentID, kind, attempts, lastErr)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}
