package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type walletEvent struct {
	Type           string `json:"type"`
	TransferID     string `json:"transfer_id"`
	WalletID       string `json:"wallet_id"`
	CounterpartyID string `json:"counterparty_id"`
	Amount         int64  `json:"amount"`
	BalanceAfter   int64  `json:"balance_after"`
}

type entry struct {
	transferID     string
	walletID       string
	counterpartyID string
	direction      string
	amount         int64
	balanceAfter   int64
}

// Writer stores wallet events as ledger entries. Handle is idempotent.
type Writer struct {
	db  *pgxpool.Pool
	log *slog.Logger
}

func NewWriter(db *pgxpool.Pool, log *slog.Logger) *Writer {
	return &Writer{db: db, log: log}
}

func (w *Writer) Handle(ctx context.Context, recs []*kgo.Record) error {
	var entries []entry
	for _, r := range recs {
		e, err := parseEvent(r.Value)
		if err != nil {
			w.log.Error("skipping bad wallet event", "partition", r.Partition, "offset", r.Offset, "error", err)
			continue
		}
		entries = append(entries, e)
	}
	return w.store(ctx, entries)
}

func parseEvent(value []byte) (entry, error) {
	var ev walletEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return entry{}, fmt.Errorf("decode: %w", err)
	}
	var dir string
	switch ev.Type {
	case "wallet.debited":
		dir = "DEBIT"
	case "wallet.credited":
		dir = "CREDIT"
	default:
		return entry{}, fmt.Errorf("unknown event type %q", ev.Type)
	}
	for _, id := range []string{ev.TransferID, ev.WalletID, ev.CounterpartyID} {
		if _, err := uuid.Parse(id); err != nil {
			return entry{}, fmt.Errorf("bad id %q", id)
		}
	}
	if ev.Amount <= 0 {
		return entry{}, fmt.Errorf("non-positive amount %d", ev.Amount)
	}
	return entry{
		transferID: ev.TransferID, walletID: ev.WalletID, counterpartyID: ev.CounterpartyID,
		direction: dir, amount: ev.Amount, balanceAfter: ev.BalanceAfter,
	}, nil
}

// Consumer reads wallet.events and writes ledger entries. Delivery is
// at-least-once; the unique (transfer_id, wallet_id) key makes replays harmless.

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Run consumes until ctx is cancelled.

func (w *Writer) store(ctx context.Context, entries []entry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, e := range entries {
		_, err := tx.Exec(ctx, `
			INSERT INTO ledger_entries (transfer_id, wallet_id, counterparty_id, direction, amount_paise, balance_after)
			VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6)
			ON CONFLICT (transfer_id, wallet_id) DO NOTHING`,
			e.transferID, e.walletID, e.counterpartyID, e.direction, e.amount, e.balanceAfter)
		if err != nil {
			return fmt.Errorf("insert entry: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
