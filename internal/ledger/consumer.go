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
type Consumer struct {
	db     *pgxpool.Pool
	client *kgo.Client
	log    *slog.Logger
}

func NewConsumer(db *pgxpool.Pool, brokers []string, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("ledger"),
		kgo.ConsumeTopics("wallet.events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{db: db, client: client, log: log}, nil
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

// Run consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if ctx.Err() == nil {
				c.log.Error("kafka fetch error", "topic", topic, "partition", partition, "error", err)
			}
		})

		var recs []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
		if len(recs) == 0 {
			continue
		}

		var entries []entry
		for _, r := range recs {
			e, err := parseEvent(r.Value)
			if err != nil {
				c.log.Error("skipping bad wallet event", "partition", r.Partition, "offset", r.Offset, "error", err)
				continue
			}
			entries = append(entries, e)
		}

		// Never commit an offset for data we have not saved: retry until stored.
		for ctx.Err() == nil {
			if err := c.store(ctx, entries); err != nil {
				c.log.Error("store ledger entries failed, retrying", "error", err)
				sleep(ctx, 2*time.Second)
				continue
			}
			break
		}
		if ctx.Err() != nil {
			return
		}

		if err := c.client.CommitRecords(ctx, recs...); err != nil && ctx.Err() == nil {
			c.log.Error("commit offsets failed (events will be redelivered, inserts are idempotent)", "error", err)
		}
	}
}

func (c *Consumer) store(ctx context.Context, entries []entry) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := c.db.Begin(ctx)
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
