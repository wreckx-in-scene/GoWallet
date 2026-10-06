package ledger

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
)

// Reconciler checks that the ledger agrees with the wallet service.
// It only looks at data older than 30s and re-checks mismatches once,
// so events still in flight do not raise false alarms.

var (
	driftWallets = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ledger_reconciliation_drift_wallets", Help: "Wallets whose ledger sum differs from the wallet balance.",
	})
	unmatchedTransfers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ledger_unmatched_transfers", Help: "Transfers whose debit and credit legs do not balance.",
	})
	lastReconcile = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ledger_last_reconciliation_timestamp_seconds", Help: "Unix time of the last finished reconciliation.",
	})
)

type Reconciler struct {
	db     *pgxpool.Pool
	wallet walletv1.WalletServiceClient
	log    *slog.Logger
	every  time.Duration
}

func NewReconciler(db *pgxpool.Pool, w walletv1.WalletServiceClient, log *slog.Logger, every time.Duration) *Reconciler {
	return &Reconciler{db: db, wallet: w, log: log, every: every}
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := r.RunOnce(ctx); err != nil && ctx.Err() == nil {
			r.log.Error("reconciliation failed", "error", err)
		}
	}
}

func (r *Reconciler) RunOnce(ctx context.Context) error {
	unmatched, err := r.queryIDs(ctx, `
		SELECT transfer_id::text FROM ledger_entries
		GROUP BY transfer_id
		HAVING SUM(CASE direction WHEN 'DEBIT' THEN amount_paise ELSE -amount_paise END) <> 0
		   AND MAX(created_at) < now() - interval '30 seconds'`)
	if err != nil {
		return err
	}
	for _, id := range unmatched {
		r.log.Error("reconciliation: transfer legs do not balance", "transfer_id", id)
	}

	wallets, err := r.queryIDs(ctx, `
		SELECT wallet_id::text FROM ledger_entries
		GROUP BY wallet_id
		HAVING MAX(created_at) < now() - interval '30 seconds'`)
	if err != nil {
		return err
	}

	drift := 0
	for _, id := range wallets {
		ledgerSum, balance, err := r.compare(ctx, id)
		if err != nil {
			r.log.Warn("reconciliation: could not compare wallet", "wallet_id", id, "error", err)
			continue
		}
		if ledgerSum == balance {
			continue
		}
		sleep(ctx, 5*time.Second) // maybe an event is in flight: look once more
		ledgerSum, balance, err = r.compare(ctx, id)
		if err != nil || ledgerSum == balance {
			continue
		}
		drift++
		r.log.Error("reconciliation drift", "wallet_id", id, "ledger", ledgerSum, "wallet", balance, "diff", balance-ledgerSum)
	}

	driftWallets.Set(float64(drift))
	unmatchedTransfers.Set(float64(len(unmatched)))
	lastReconcile.SetToCurrentTime()

	r.log.Info("reconciliation finished",
		"wallets_checked", len(wallets), "drift", drift, "unmatched_transfers", len(unmatched))
	return nil
}

func (r *Reconciler) compare(ctx context.Context, walletID string) (ledgerSum, balance int64, err error) {
	err = r.db.QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_paise ELSE -amount_paise END), 0)::bigint
		FROM ledger_entries WHERE wallet_id = $1::uuid`, walletID).Scan(&ledgerSum)
	if err != nil {
		return 0, 0, fmt.Errorf("sum ledger: %w", err)
	}

	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := r.wallet.GetBalance(cctx, &walletv1.GetBalanceRequest{WalletId: walletID})
	if err != nil {
		return 0, 0, fmt.Errorf("get wallet balance: %w", err)
	}
	return ledgerSum, resp.GetBalancePaise(), nil
}

func (r *Reconciler) queryIDs(ctx context.Context, query string) ([]string, error) {
	rows, err := r.db.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("reconciliation query: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
