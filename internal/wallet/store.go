package wallet

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrInvalidTransfer     = errors.New("invalid transfer")
	ErrIdempotencyConflict = errors.New("transfer id already used with different details")
)

// Store is the wallet service's data access layer.
type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// CreateUserWallet returns the user's wallet id, creating the wallet if it
// does not exist yet. Safe to call repeatedly (idempotent).
func (s *Store) CreateUserWallet(ctx context.Context, userID string) (string, error) {
	var id string
	err := s.db.QueryRow(ctx, `
		INSERT INTO wallets (user_id) VALUES ($1::uuid)
		ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id
		RETURNING id::text`, userID).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("create user wallet: %w", err)
	}
	return id, nil
}

// CreateSystemWallet creates a wallet with no owner, used as the source of top-ups.
func (s *Store) CreateSystemWallet(ctx context.Context) (string, error) {
	var id string
	err := s.db.QueryRow(ctx,
		`INSERT INTO wallets (kind) VALUES ('SYSTEM') RETURNING id::text`).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("create system wallet: %w", err)
	}
	return id, nil
}

// Balance returns a wallet's balance in paise.
func (s *Store) Balance(ctx context.Context, walletID string) (int64, error) {
	var bal int64
	err := s.db.QueryRow(ctx,
		`SELECT balance FROM wallets WHERE id = $1::uuid`, walletID).Scan(&bal)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrWalletNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("get balance: %w", err)
	}
	return bal, nil
}
