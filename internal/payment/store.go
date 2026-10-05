package payment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const outboxTopic = "payment.events"

const (
	StatusPending       = "PENDING"
	StatusFraudApproved = "FRAUD_APPROVED"
	StatusCompleted     = "COMPLETED"
	StatusRejected      = "REJECTED"
	StatusFailed        = "FAILED"
)

var (
	ErrNotFound            = errors.New("payment not found")
	ErrIdempotencyConflict = errors.New("idempotency key already used with different details")
)

type Payment struct {
	ID             string
	UserID         string
	IdempotencyKey string
	RequestHash    string
	FromWalletID   string
	ToWalletID     string
	AmountPaise    int64
	Status         string
	FailureReason  string
	CreatedAt      time.Time
}

// Event is what notification (and others) consume from payment.events.
type Event struct {
	Type         string `json:"type"`
	PaymentID    string `json:"payment_id"`
	UserID       string `json:"user_id"`
	FromWalletID string `json:"from_wallet_id"`
	ToWalletID   string `json:"to_wallet_id"`
	AmountPaise  int64  `json:"amount_paise"`
	Reason       string `json:"reason,omitempty"`
}

const cols = `id::text, user_id::text, idempotency_key, request_hash,
	from_wallet_id::text, to_wallet_id::text, amount_paise, status, failure_reason, created_at`

func scan(row pgx.Row) (Payment, error) {
	var p Payment
	err := row.Scan(&p.ID, &p.UserID, &p.IdempotencyKey, &p.RequestHash,
		&p.FromWalletID, &p.ToWalletID, &p.AmountPaise, &p.Status, &p.FailureReason, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Payment{}, ErrNotFound
	}
	if err != nil {
		return Payment{}, fmt.Errorf("load payment: %w", err)
	}
	return p, nil
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) Get(ctx context.Context, id string) (Payment, error) {
	return scan(s.db.QueryRow(ctx, `SELECT `+cols+` FROM payments WHERE id = $1::uuid`, id))
}

func (s *Store) GetForUser(ctx context.Context, userID, id string) (Payment, error) {
	return scan(s.db.QueryRow(ctx,
		`SELECT `+cols+` FROM payments WHERE id = $1::uuid AND user_id = $2::uuid`, id, userID))
}

// Insert creates the payment, or returns the existing one for the same
// (user, idempotency key). Same key with different details is a conflict.
func (s *Store) Insert(ctx context.Context, p Payment) (Payment, error) {
	tag, err := s.db.Exec(ctx, `
		INSERT INTO payments (id, user_id, idempotency_key, request_hash, from_wallet_id, to_wallet_id, amount_paise)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5::uuid, $6::uuid, $7)
		ON CONFLICT (user_id, idempotency_key) DO NOTHING`,
		p.ID, p.UserID, p.IdempotencyKey, p.RequestHash, p.FromWalletID, p.ToWalletID, p.AmountPaise)
	if err != nil {
		return Payment{}, fmt.Errorf("insert payment: %w", err)
	}
	existing, err := scan(s.db.QueryRow(ctx,
		`SELECT `+cols+` FROM payments WHERE user_id = $1::uuid AND idempotency_key = $2`,
		p.UserID, p.IdempotencyKey))
	if err != nil {
		return Payment{}, err
	}
	if tag.RowsAffected() == 0 && existing.RequestHash != p.RequestHash {
		return Payment{}, ErrIdempotencyConflict
	}
	return existing, nil
}

// Transition moves a payment from `from` to `to` only if it is still in `from`.
// It reports false when someone else already moved it. If ev is not nil, the
// event is written to the outbox in the same transaction.
func (s *Store) Transition(ctx context.Context, id, from, to, reason string, ev *Event) (bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE payments SET status = $3, failure_reason = $4, updated_at = now()
		WHERE id = $1::uuid AND status = $2`, id, from, to, reason)
	if err != nil {
		return false, fmt.Errorf("update status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if ev != nil {
		if _, err := tx.Exec(ctx,
			`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
			outboxTopic, id, ev); err != nil {
			return false, fmt.Errorf("write outbox: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}

// StuckIDs lists unfinished payments that have not moved for a while.
func (s *Store) StuckIDs(ctx context.Context, olderThan time.Duration, limit int) ([]string, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id::text FROM payments
		WHERE status IN ('PENDING', 'FRAUD_APPROVED')
		  AND updated_at < now() - make_interval(secs => $1)
		ORDER BY updated_at
		LIMIT $2`, olderThan.Seconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("find stuck payments: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan stuck payment: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
