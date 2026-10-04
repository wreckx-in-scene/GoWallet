package wallet

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/wreckx-in-scene/GoWallet/internal/platform/postgres"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("WALLET_DB_URL")
	if url == "" {
		t.Skip("WALLET_DB_URL not set")
	}
	pool, err := postgres.NewPool(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return NewStore(pool)
}

func TestCreateUserWalletIsIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	user := uuid.NewString()

	a, err := s.CreateUserWallet(ctx, user)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.CreateUserWallet(ctx, user)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("expected the same wallet, got %s and %s", a, b)
	}
}
