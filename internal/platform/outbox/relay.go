package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	batchSize      = 100
	pollInterval   = 500 * time.Millisecond
	produceTimeout = 10 * time.Second
)

// Relay moves rows from a service's outbox table into Kafka.
type Relay struct {
	db     *pgxpool.Pool
	client *kgo.Client
	log    *slog.Logger
}

func NewRelay(db *pgxpool.Pool, client *kgo.Client, log *slog.Logger) *Relay {
	return &Relay{db: db, client: client, log: log}
}

// Run publishes pending outbox rows until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		n, err := r.publishBatch(ctx)
		if err != nil && ctx.Err() == nil {
			r.log.Error("outbox relay failed", "error", err)
		}
		if n == batchSize {
			continue // a full batch means there is probably more waiting
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// publishBatch sends up to batchSize unpublished rows to Kafka and marks them
// published. A row is marked only AFTER Kafka has confirmed it.
func (r *Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("select pending: %w", err)
	}
	defer rows.Close()

	var ids []int64
	var recs []*kgo.Record
	for rows.Next() {
		var id int64
		var topic, key string
		var payload []byte
		if err := rows.Scan(&id, &topic, &key, &payload); err != nil {
			return 0, fmt.Errorf("scan outbox row: %w", err)
		}
		ids = append(ids, id)
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(key), Value: payload})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read outbox rows: %w", err)
	}
	if len(recs) == 0 {
		return 0, nil
	}

	pctx, cancel := context.WithTimeout(ctx, produceTimeout)
	defer cancel()
	if err := r.client.ProduceSync(pctx, recs...).FirstErr(); err != nil {
		return 0, fmt.Errorf("produce to kafka: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(recs), nil
}
