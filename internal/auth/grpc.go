package auth

import (
	"context"
	"errors"
	"log/slog"

	authv1 "github.com/wreckx-in-scene/GoWallet/gen/auth/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handler struct {
	authv1.UnimplementedAuthServiceServer
	svc *Service
	log *slog.Logger
}

func NewHandler(svc *Service, log *slog.Logger) authv1.AuthServiceServer {
	return &handler{svc: svc, log: log}
}

func (h *handler) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	id, err := h.svc.Register(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		return nil, h.toStatus("Register", err)
	}
	return &authv1.RegisterResponse{UserId: id}, nil
}

func (h *handler) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.TokenPair, error) {
	p, err := h.svc.Login(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		return nil, h.toStatus("Login", err)
	}
	return toProto(p), nil
}

func (h *handler) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.TokenPair, error) {
	p, err := h.svc.Refresh(ctx, req.GetRefreshToken())
	if err != nil {
		return nil, h.toStatus("Refresh", err)
	}
	return toProto(p), nil
}

func (h *handler) Logout(ctx context.Context, req *authv1.LogoutRequest) (*authv1.LogoutResponse, error) {
	if err := h.svc.Logout(ctx, req.GetRefreshToken()); err != nil {
		return nil, h.toStatus("Logout", err)
	}
	return &authv1.LogoutResponse{}, nil
}

func (h *handler) GetPublicKey(context.Context, *authv1.GetPublicKeyRequest) (*authv1.GetPublicKeyResponse, error) {
	id, key := h.svc.PublicKey()
	return &authv1.GetPublicKeyResponse{KeyId: id, PublicKey: key}, nil
}

func toProto(p TokenPair) *authv1.TokenPair {
	return &authv1.TokenPair{
		AccessToken:      p.Access,
		RefreshToken:     p.Refresh,
		ExpiresInSeconds: p.ExpiresInSeconds,
	}
}

func (h *handler) toStatus(op string, err error) error {
	switch {
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrEmailTaken):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrBadCredentials), errors.Is(err, ErrInvalidToken):
		return status.Error(codes.Unauthenticated, err.Error())
	default:
		h.log.Error("auth operation failed", "op", op, "error", err)
		return status.Error(codes.Internal, "internal error")
	}
}
