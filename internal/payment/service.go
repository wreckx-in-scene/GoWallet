package payment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	fraudv1 "github.com/wreckx-in-scene/GoWallet/gen/fraud/v1"
	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	callTimeout   = 3 * time.Second  // per downstream call
	pendingExpiry = 5 * time.Minute  // a PENDING payment (no money moved) older than this fails
	stuckAfter    = 30 * time.Second // recovery picks up payments idle this long
)

var ErrInvalid = errors.New("invalid payment request")

type Service struct {
	store  *Store
	wallet walletv1.WalletServiceClient
	fraud  fraudv1.FraudServiceClient
	log    *slog.Logger
}

func NewService(store *Store, w walletv1.WalletServiceClient, f fraudv1.FraudServiceClient, log *slog.Logger) *Service {
	return &Service{store: store, wallet: w, fraud: f, log: log}
}

func requestHash(to string, amount int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", to, amount)))
	return hex.EncodeToString(sum[:])
}

// Create is idempotent on (userID, key). It drives the payment as far as it can.
func (s *Service) Create(ctx context.Context, userID, key, toWalletID string, amount int64) (Payment, error) {
	uid, err := uuid.Parse(userID)
	if err != nil {
		return Payment{}, fmt.Errorf("%w: user_id must be a UUID", ErrInvalid)
	}
	to, err := uuid.Parse(toWalletID)
	if err != nil {
		return Payment{}, fmt.Errorf("%w: to_wallet_id must be a UUID", ErrInvalid)
	}
	if key == "" || len(key) > 128 {
		return Payment{}, fmt.Errorf("%w: idempotency_key must be 1-128 characters", ErrInvalid)
	}
	if amount <= 0 {
		return Payment{}, fmt.Errorf("%w: amount_paise must be positive", ErrInvalid)
	}

	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	w, err := s.wallet.CreateWallet(cctx, &walletv1.CreateWalletRequest{UserId: uid.String()})
	if err != nil {
		return Payment{}, fmt.Errorf("resolve sender wallet: %w", err)
	}
	if w.GetWalletId() == to.String() {
		return Payment{}, fmt.Errorf("%w: cannot pay your own wallet", ErrInvalid)
	}

	p, err := s.store.Insert(ctx, Payment{
		ID:             uuid.NewString(),
		UserID:         uid.String(),
		IdempotencyKey: key,
		RequestHash:    requestHash(to.String(), amount),
		FromWalletID:   w.GetWalletId(),
		ToWalletID:     to.String(),
		AmountPaise:    amount,
	})
	if err != nil {
		return Payment{}, err
	}
	return s.Advance(ctx, p)
}

func (s *Service) Get(ctx context.Context, userID, paymentID string) (Payment, error) {
	return s.store.GetForUser(ctx, userID, paymentID)
}

// Advance drives a payment forward. Safe to call repeatedly and concurrently:
// fraud check and wallet transfer are idempotent, status moves are guarded.
func (s *Service) Advance(ctx context.Context, p Payment) (Payment, error) {
	for i := 0; i < 3; i++ { // PENDING -> FRAUD_APPROVED -> final needs at most 2 steps
		var err error
		switch p.Status {
		case StatusPending:
			p, err = s.stepFraud(ctx, p)
		case StatusFraudApproved:
			p, err = s.stepTransfer(ctx, p)
		default:
			return p, nil // COMPLETED, REJECTED or FAILED
		}
		if err != nil {
			return p, err
		}
	}
	return p, nil
}

func (s *Service) stepFraud(ctx context.Context, p Payment) (Payment, error) {
	if time.Since(p.CreatedAt) > pendingExpiry {
		return s.finish(ctx, p, StatusPending, StatusFailed, "expired", "payment.failed")
	}

	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	res, err := s.fraud.Check(cctx, &fraudv1.CheckRequest{
		PaymentId: p.ID, UserId: p.UserID, AmountPaise: p.AmountPaise,
	})
	if err != nil {
		return p, fmt.Errorf("fraud check: %w", err) // fail closed: stays PENDING
	}
	if !res.GetApproved() {
		return s.finish(ctx, p, StatusPending, StatusRejected, res.GetReason(), "payment.rejected")
	}
	return s.move(ctx, p, StatusPending, StatusFraudApproved, "", nil)
}

func (s *Service) stepTransfer(ctx context.Context, p Payment) (Payment, error) {
	cctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	_, err := s.wallet.Transfer(cctx, &walletv1.TransferRequest{
		TransferId:   p.ID,
		FromWalletId: p.FromWalletID,
		ToWalletId:   p.ToWalletID,
		AmountPaise:  p.AmountPaise,
	})
	if err == nil {
		return s.finish(ctx, p, StatusFraudApproved, StatusCompleted, "", "payment.completed")
	}

	switch status.Code(err) {
	case codes.FailedPrecondition:
		return s.finish(ctx, p, StatusFraudApproved, StatusFailed, "insufficient_funds", "payment.failed")
	case codes.NotFound:
		return s.finish(ctx, p, StatusFraudApproved, StatusFailed, "wallet_not_found", "payment.failed")
	case codes.InvalidArgument:
		return s.finish(ctx, p, StatusFraudApproved, StatusFailed, "invalid_transfer", "payment.failed")
	default:
		// Unknown outcome (timeout, unavailable...). The transfer may or may not have
		// happened, so stay FRAUD_APPROVED and retry later with the same transfer id.
		return p, fmt.Errorf("wallet transfer: %w", err)
	}
}

func (s *Service) finish(ctx context.Context, p Payment, from, to, reason, eventType string) (Payment, error) {
	ev := &Event{
		Type: eventType, PaymentID: p.ID, UserID: p.UserID,
		FromWalletID: p.FromWalletID, ToWalletID: p.ToWalletID,
		AmountPaise: p.AmountPaise, Reason: reason,
	}
	return s.move(ctx, p, from, to, reason, ev)
}

func (s *Service) move(ctx context.Context, p Payment, from, to, reason string, ev *Event) (Payment, error) {
	moved, err := s.store.Transition(ctx, p.ID, from, to, reason, ev)
	if err != nil {
		return p, err
	}
	if !moved {
		return s.store.Get(ctx, p.ID) // someone else advanced it first: take their result
	}
	p.Status, p.FailureReason = to, reason
	return p, nil
}

// RunRecovery resumes payments that got stuck (crash, downstream outage).
func (s *Service) RunRecovery(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		ids, err := s.store.StuckIDs(ctx, stuckAfter, 20)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Error("recovery: list stuck payments", "error", err)
			}
			continue
		}
		for _, id := range ids {
			p, err := s.store.Get(ctx, id)
			if err != nil {
				continue
			}
			if _, err := s.Advance(ctx, p); err != nil && ctx.Err() == nil {
				s.log.Warn("recovery: could not advance payment", "payment_id", id, "error", err)
			}
		}
	}
}
