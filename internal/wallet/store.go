package wallet

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

const outboxTopic = "wallet.events"

// TransferParams describes one money movement. ID is the payment id and
// doubles as the idempotency key. Amount is in paise.
type TransferParams struct {
	ID     string
	FromID string
	ToID   string
	Amount int64
}

// walletEvent is what the ledger will later consume from Kafka (one per wallet leg).
type walletEvent struct {
	Type           string `json:"type"`
	TransferID     string `json:"transfer_id"`
	WalletID       string `json:"wallet_id"`
	CounterpartyID string `json:"counterparty_id"`
	Amount         int64  `json:"amount"`
	BalanceAfter   int64  `json:"balance_after"`
}

func normalizeIDs(ids ...string) ([]string, error) {
	out := make([]string, len(ids))
	for i, s := range ids {
		u, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("%w: bad id %q", ErrInvalidTransfer, s)
		}
		out[i] = u.String()
	}
	return out, nil
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

// Transfer moves Amount from one wallet to another atomically.
// Calling it again with the same ID and details is a safe no-op.
func (s *Store) Transfer(ctx context.Context, p TransferParams) error {
	ids, err := normalizeIDs(p.ID, p.FromID, p.ToID)
	if err != nil {
		return err
	}
	p.ID, p.FromID, p.ToID = ids[0], ids[1], ids[2]

	if p.Amount <= 0 || p.FromID == p.ToID {
		return ErrInvalidTransfer
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transfer: %w", err)
	}
	defer tx.Rollback(ctx) // no-op once Commit has succeeded

	// 2. Lock both wallets, always the smaller id first (no deadlocks).
	first, second := p.FromID, p.ToID
	if first > second {
		first, second = second, first
	}
	type locked struct {
		balance int64
		kind    string
	}
	wallets := make(map[string]locked, 2)
	for _, id := range []string{first, second} {
		var w locked
		err := tx.QueryRow(ctx,
			`SELECT balance, kind FROM wallets WHERE id = $1::uuid FOR UPDATE`, id).
			Scan(&w.balance, &w.kind)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrWalletNotFound
		}
		if err != nil {
			return fmt.Errorf("lock wallet: %w", err)
		}
		wallets[id] = w
	}

	// 1. Idempotency gate: the primary key on transfers.id decides.
	tag, err := tx.Exec(ctx, `
		INSERT INTO transfers (id, from_wallet_id, to_wallet_id, amount)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4)
		ON CONFLICT (id) DO NOTHING`,
		p.ID, p.FromID, p.ToID, p.Amount)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign_key_violation
			return ErrWalletNotFound
		}
		return fmt.Errorf("insert transfer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Already applied. Same details => success, different => caller bug.
		var from, to string
		var amount int64
		err := tx.QueryRow(ctx, `
			SELECT from_wallet_id::text, to_wallet_id::text, amount
			FROM transfers WHERE id = $1::uuid`, p.ID).Scan(&from, &to, &amount)
		if err != nil {
			return fmt.Errorf("load existing transfer: %w", err)
		}
		if from != p.FromID || to != p.ToID || amount != p.Amount {
			return ErrIdempotencyConflict
		}
		return nil
	}

	// 3. Balance check, now that nobody else can change it.
	from := wallets[p.FromID]
	if from.kind != "SYSTEM" && from.balance < p.Amount {
		return ErrInsufficientFunds
	}

	// 4. Move the money.
	if _, err := tx.Exec(ctx,
		`UPDATE wallets SET balance = balance - $2 WHERE id = $1::uuid`,
		p.FromID, p.Amount); err != nil {
		return fmt.Errorf("debit: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE wallets SET balance = balance + $2 WHERE id = $1::uuid`,
		p.ToID, p.Amount); err != nil {
		return fmt.Errorf("credit: %w", err)
	}

	// 5. Outbox: one event per wallet leg, in the SAME transaction.
	events := []walletEvent{
		{"wallet.debited", p.ID, p.FromID, p.ToID, p.Amount, from.balance - p.Amount},
		{"wallet.credited", p.ID, p.ToID, p.FromID, p.Amount, wallets[p.ToID].balance + p.Amount},
	}
	for _, ev := range events {
		if _, err := tx.Exec(ctx,
			`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
			outboxTopic, ev.WalletID, ev); err != nil {
			return fmt.Errorf("write outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transfer: %w", err)
	}
	return nil
}
