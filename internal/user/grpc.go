package user

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	userv1 "github.com/wreckx-in-scene/GoWallet/gen/user/v1"
)

type handler struct {
	userv1.UnimplementedUserServiceServer
	store *Store
	log   *slog.Logger
}

func NewHandler(store *Store, log *slog.Logger) userv1.UserServiceServer {
	return &handler{store: store, log: log}
}

func (h *handler) GetProfile(ctx context.Context, req *userv1.GetProfileRequest) (*userv1.GetProfileResponse, error) {
	if _, err := uuid.Parse(req.GetUserId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	p, err := h.store.Get(ctx, req.GetUserId())
	if err != nil {
		return nil, h.toStatus("GetProfile", err)
	}
	return &userv1.GetProfileResponse{Profile: toProto(p)}, nil
}

func (h *handler) UpdateProfile(ctx context.Context, req *userv1.UpdateProfileRequest) (*userv1.UpdateProfileResponse, error) {
	if _, err := uuid.Parse(req.GetUserId()); err != nil {
		return nil, status.Error(codes.InvalidArgument, "user_id must be a UUID")
	}
	name := strings.TrimSpace(req.GetFullName())
	if utf8.RuneCountInString(name) > 100 {
		return nil, status.Error(codes.InvalidArgument, "full_name must be at most 100 characters")
	}
	p, err := h.store.UpdateName(ctx, req.GetUserId(), name)
	if err != nil {
		return nil, h.toStatus("UpdateProfile", err)
	}
	return &userv1.UpdateProfileResponse{Profile: toProto(p)}, nil
}

func toProto(p Profile) *userv1.Profile {
	return &userv1.Profile{UserId: p.UserID, Email: p.Email, FullName: p.FullName}
}

func (h *handler) toStatus(op string, err error) error {
	if errors.Is(err, ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	h.log.Error("user operation failed", "op", op, "error", err)
	return status.Error(codes.Internal, "internal error")
}
