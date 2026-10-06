package wallet

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	walletv1 "github.com/wreckx-in-scene/GoWallet/gen/wallet/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var transferDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "wallet_transfer_duration_seconds",
	Help:    "Duration of wallet transfers by outcome.",
	Buckets: prometheus.DefBuckets,
}, []string{"outcome"})

func outcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrInsufficientFunds):
		return "insufficient_funds"
	case errors.Is(err, ErrInvalidTransfer), errors.Is(err, ErrWalletNotFound), errors.Is(err, ErrIdempotencyConflict):
		return "rejected"
	default:
		return "error"
	}
}

// handler implements the generated WalletServiceServer interface.
type handler struct {
	walletv1.UnimplementedWalletServiceServer
	store *Store
	log   *slog.Logger
}

func (h *handler) CreateWallet(ctx context.Context, req *walletv1.CreateWalletRequest) (*walletv1.CreateWalletResponse, error) {
	if _, err := uuid.Parse(req.GetUserId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	id, err := h.store.CreateUserWallet(ctx, req.GetUserId())
	if err != nil {
		return nil, h.toStatus("CreateWallet", err)
	}
	return &walletv1.CreateWalletResponse{WalletId: id}, nil
}

func (h *handler) GetBalance(ctx context.Context, req *walletv1.GetBalanceRequest) (*walletv1.GetBalanceResponse, error) {
	if _, err := uuid.Parse(req.GetWalletId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "wallet_id must be a UUID")
	}
	bal, err := h.store.Balance(ctx, req.GetWalletId())
	if err != nil {
		return nil, h.toStatus("GetBalance", err)
	}
	return &walletv1.GetBalanceResponse{BalancePaise: bal}, nil
}

func (h *handler) Transfer(ctx context.Context, req *walletv1.TransferRequest) (*walletv1.TransferResponse, error) {
	start := time.Now()
	err := h.store.Transfer(ctx, TransferParams{
		ID:     req.GetTransferId(),
		FromID: req.GetFromWalletId(),
		ToID:   req.GetToWalletId(),
		Amount: req.GetAmountPaise(),
	})
	transferDuration.WithLabelValues(outcome(err)).Observe(time.Since(start).Seconds())
	if err != nil {
		return nil, h.toStatus("Transfer", err)
	}

	return &walletv1.TransferResponse{}, nil
}

// toStatus turns store errors into gRPC status codes. Business outcomes get
// specific codes; anything unexpected is logged here and hidden from the caller.
func (h *handler) toStatus(op string, err error) error {
	switch {
	case errors.Is(err, ErrInvalidTransfer):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrWalletNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrInsufficientFunds):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.FromContextError(err).Err()
	default:
		h.log.Error("wallet operation failed", "op", op, "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}

func NewHandler(store *Store, log *slog.Logger) walletv1.WalletServiceServer {
	return &handler{store: store, log: log}
}
