package wallet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
)

// fundedWallet creates a user wallet and tops it up from the system wallet.
func fundedWallet(t *testing.T, s *Store, sys string, amount int64) string {
	t.Helper()
	ctx := context.Background()
	id, err := s.CreateUserWallet(ctx, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if amount > 0 {
		err := s.Transfer(ctx, TransferParams{ID: uuid.NewString(), FromID: sys, ToID: id, Amount: amount})
		if err != nil {
			t.Fatal(err)
		}
	}
	return id
}

func newSystemWallet(t *testing.T, s *Store) string {
	t.Helper()
	id, err := s.CreateSystemWallet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustBalance(t *testing.T, s *Store, id string) int64 {
	t.Helper()
	b, err := s.Balance(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func countRows(t *testing.T, s *Store, query string, arg string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(context.Background(), query, arg).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Every transfer moves money between wallets, so all balances must sum to 0
// (system wallets go negative by exactly what users hold).
func assertBooksBalance(t *testing.T, s *Store) {
	t.Helper()
	var sum int64
	err := s.db.QueryRow(context.Background(),
		`SELECT COALESCE(SUM(balance), 0)::bigint FROM wallets`).Scan(&sum)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 0 {
		t.Fatalf("books out of balance: sum of all wallets = %d, want 0", sum)
	}
}

func TestTransferMovesMoney(t *testing.T) {
	s := newTestStore(t)
	sys := newSystemWallet(t, s)
	a := fundedWallet(t, s, sys, 1000)
	b := fundedWallet(t, s, sys, 0)

	id := uuid.NewString()
	err := s.Transfer(context.Background(), TransferParams{ID: id, FromID: a, ToID: b, Amount: 300})
	if err != nil {
		t.Fatal(err)
	}

	if got := mustBalance(t, s, a); got != 700 {
		t.Fatalf("a = %d, want 700", got)
	}
	if got := mustBalance(t, s, b); got != 300 {
		t.Fatalf("b = %d, want 300", got)
	}
	if n := countRows(t, s, `SELECT count(*) FROM outbox WHERE payload->>'transfer_id' = $1`, id); n != 2 {
		t.Fatalf("outbox rows = %d, want 2", n)
	}
	assertBooksBalance(t, s)
}

func TestTransferInsufficientFundsRollsBack(t *testing.T) {
	s := newTestStore(t)
	sys := newSystemWallet(t, s)
	a := fundedWallet(t, s, sys, 100)
	b := fundedWallet(t, s, sys, 0)

	id := uuid.NewString()
	err := s.Transfer(context.Background(), TransferParams{ID: id, FromID: a, ToID: b, Amount: 200})
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	if got := mustBalance(t, s, a); got != 100 {
		t.Fatalf("a = %d, want 100 (unchanged)", got)
	}
	// the transfers row must have been rolled back too
	if n := countRows(t, s, `SELECT count(*) FROM transfers WHERE id = $1::uuid`, id); n != 0 {
		t.Fatalf("transfer row survived a failed transfer")
	}
}

func TestTransferIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	sys := newSystemWallet(t, s)
	a := fundedWallet(t, s, sys, 1000)
	b := fundedWallet(t, s, sys, 0)

	p := TransferParams{ID: uuid.NewString(), FromID: a, ToID: b, Amount: 300}
	for i := 0; i < 3; i++ { // a client retrying the same payment
		if err := s.Transfer(ctx, p); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if got := mustBalance(t, s, a); got != 700 {
		t.Fatalf("a = %d, want 700 (money moved more than once?)", got)
	}
	if n := countRows(t, s, `SELECT count(*) FROM outbox WHERE payload->>'transfer_id' = $1`, p.ID); n != 2 {
		t.Fatalf("outbox rows = %d, want 2", n)
	}

	p.Amount = 999 // same id, different details = caller bug
	if err := s.Transfer(ctx, p); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestNoDoubleSpend(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	sys := newSystemWallet(t, s)
	a := fundedWallet(t, s, sys, 100)
	b := fundedWallet(t, s, sys, 0)

	const attempts = 200 // 200 people try to spend the same 100 paise
	var ok, insufficient atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Transfer(ctx, TransferParams{ID: uuid.NewString(), FromID: a, ToID: b, Amount: 1})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrInsufficientFunds):
				insufficient.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if ok.Load() != 100 || insufficient.Load() != 100 {
		t.Fatalf("ok=%d insufficient=%d, want 100 and 100", ok.Load(), insufficient.Load())
	}
	if got := mustBalance(t, s, a); got != 0 {
		t.Fatalf("a = %d, want 0", got)
	}
	if got := mustBalance(t, s, b); got != 100 {
		t.Fatalf("b = %d, want 100", got)
	}
	assertBooksBalance(t, s)
}

func TestOpposingTransfersDoNotDeadlock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	sys := newSystemWallet(t, s)
	a := fundedWallet(t, s, sys, 10_000)
	b := fundedWallet(t, s, sys, 10_000)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		from, to := a, b
		if i%2 == 1 {
			from, to = b, a // half go a->b, half go b->a
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Transfer(ctx, TransferParams{ID: uuid.NewString(), FromID: from, ToID: to, Amount: 1})
			if err != nil {
				t.Errorf("transfer failed: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := mustBalance(t, s, a); got != 10_000 {
		t.Fatalf("a = %d, want 10000", got)
	}
	if got := mustBalance(t, s, b); got != 10_000 {
		t.Fatalf("b = %d, want 10000", got)
	}
	assertBooksBalance(t, s)
}
