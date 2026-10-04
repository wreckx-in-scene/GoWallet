package fraud

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	fraudv1 "github.com/wreckx-in-scene/GoWallet/gen/fraud/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handler struct {
	fraudv1.UnimplementedFraudServiceServer
	store *Store
	log   *slog.Logger
}

func (h *handler) Check(ctx context.Context, req *fraudv1.CheckRequest) (*fraudv1.CheckResponse, error) {
	if _, err := uuid.Parse(req.GetPaymentId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "payment_id must be a UUID")
	}
	if _, err := uuid.Parse(req.GetUserId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	if req.GetAmountPaise() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_paise must be positive")
	}

	d, err := h.store.Check(ctx, req.GetPaymentId(), req.GetUserId(), req.GetAmountPaise())
	if err != nil {
		h.log.Error("fraud check failed", "error", err)
		return nil, status.Error(codes.Internal, "internal error")
	}
	return &fraudv1.CheckResponse{Approved: d.Approved, Reason: d.Reason}, nil
}
