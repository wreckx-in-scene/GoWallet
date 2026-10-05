package payment

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	paymentv1 "github.com/wreckx-in-scene/GoWallet/gen/payment/v1"
)

type handler struct {
	paymentv1.UnimplementedPaymentServiceServer
	svc *Service
	log *slog.Logger
}

func NewHandler(svc *Service, log *slog.Logger) paymentv1.PaymentServiceServer {
	return &handler{svc: svc, log: log}
}

func (h *handler) CreatePayment(ctx context.Context, req *paymentv1.CreatePaymentRequest) (*paymentv1.CreatePaymentResponse, error) {
	// Keep working even if the caller disconnects: a payment must never be left half-way.
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	p, err := h.svc.Create(work, req.GetUserId(), req.GetIdempotencyKey(), req.GetToWalletId(), req.GetAmountPaise())
	if err != nil {
		return nil, h.toStatus("CreatePayment", err)
	}
	return &paymentv1.CreatePaymentResponse{Payment: toProto(p)}, nil
}

func (h *handler) GetPayment(ctx context.Context, req *paymentv1.GetPaymentRequest) (*paymentv1.GetPaymentResponse, error) {
	if _, err := uuid.Parse(req.GetUserId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	if _, err := uuid.Parse(req.GetPaymentId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "payment_id must be a UUID")
	}
	p, err := h.svc.Get(ctx, req.GetUserId(), req.GetPaymentId())
	if err != nil {
		return nil, h.toStatus("GetPayment", err)
	}
	return &paymentv1.GetPaymentResponse{Payment: toProto(p)}, nil
}

func toProto(p Payment) *paymentv1.Payment {
	return &paymentv1.Payment{
		PaymentId:     p.ID,
		Status:        p.Status,
		FailureReason: p.FailureReason,
		FromWalletId:  p.FromWalletID,
		ToWalletId:    p.ToWalletID,
		AmountPaise:   p.AmountPaise,
	}
}

// Business outcomes (COMPLETED/REJECTED/FAILED) are normal responses. Errors
// here mean "not finished": the caller retries with the same idempotency key.
func (h *handler) toStatus(op string, err error) error {
	switch {
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		h.log.Warn("payment not finished", "op", op, "error", err)
		return status.Error(codes.Unavailable, "payment not finished, retry with the same idempotency key")
	}
}
