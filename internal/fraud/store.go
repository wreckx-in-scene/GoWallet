package fraud

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ReasonAmountLimit = "amount_limit_exceeded"
	ReasonVelocity    = "velocity_limit_exceeded"
)

type Rules struct {
	MaxAmountPaise int64
	MaxPerMinute   int
}

type Decision struct {
	Approved bool
	Reason   string
}

type Store struct {
	db    *pgxpool.Pool
	rules Rules
}

func NewStore(db *pgxpool.Pool, rules Rules) *Store {
	return &Store{db: db, rules: rules}
}

// Check returns the decision for a payment. Asking again about the same
// payment returns the stored decision and does not count twice.
func (s *Store) Check(ctx context.Context, paymentID, userID string, amount int64) (Decision, error) {
	if d, found, err := s.stored(ctx, paymentID); err != nil || found {
		return d, err
	}

	d := Decision{Approved: true}
	if amount > s.rules.MaxAmountPaise {
		d = Decision{Approved: false, Reason: ReasonAmountLimit}
	} else {
		var recent int
		err := s.db.QueryRow(ctx, `
			SELECT count(*) FROM fraud_checks
			WHERE user_id = $1::uuid AND created_at > now() - interval '1 minute'`,
			userID).Scan(&recent)
		if err != nil {
			return Decision{}, fmt.Errorf("count recent payments: %w", err)
		}
		if recent >= s.rules.MaxPerMinute {
			d = Decision{Approved: false, Reason: ReasonVelocity}
		}
	}

	_, err := s.db.Exec(ctx, `
		INSERT INTO fraud_checks (payment_id, user_id, amount_paise, approved, reason)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5)
		ON CONFLICT (payment_id) DO NOTHING`,
		paymentID, userID, amount, d.Approved, d.Reason)
	if err != nil {
		return Decision{}, fmt.Errorf("record decision: %w", err)
	}

	// Return what is stored: if a duplicate request won the insert, its decision wins.
	d, _, err = s.stored(ctx, paymentID)
	return d, err
}

func (s *Store) stored(ctx context.Context, paymentID string) (Decision, bool, error) {
	var d Decision
	err := s.db.QueryRow(ctx,
		`SELECT approved, reason FROM fraud_checks WHERE payment_id = $1::uuid`,
		paymentID).Scan(&d.Approved, &d.Reason)
	if errors.Is(err, pgx.ErrNoRows) {
		return Decision{}, false, nil
	}
	if err != nil {
		return Decision{}, false, fmt.Errorf("load decision: %w", err)
	}
	return d, true, nil
}
