package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/mail"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalid        = errors.New("invalid request")
	ErrEmailTaken     = errors.New("email already registered")
	ErrBadCredentials = errors.New("invalid email or password")
	ErrInvalidToken   = errors.New("invalid or expired refresh token")
)

type TokenPair struct {
	Access           string
	Refresh          string
	ExpiresInSeconds int64
}

type Service struct {
	store      *Store
	signer     *signer
	accessTTL  time.Duration
	refreshTTL time.Duration
	log        *slog.Logger
}

func NewService(store *Store, keyFile string, accessTTL, refreshTTL time.Duration, log *slog.Logger) (*Service, error) {
	sg, err := loadOrCreateKey(keyFile)
	if err != nil {
		return nil, err
	}
	log.Info("signing key ready", "key_id", sg.keyID)
	return &Service{store: store, signer: sg, accessTTL: accessTTL, refreshTTL: refreshTTL, log: log}, nil
}

func normalizeEmail(e string) (string, error) {
	e = strings.ToLower(strings.TrimSpace(e))
	addr, err := mail.ParseAddress(e)
	if err != nil || addr.Address != e {
		return "", fmt.Errorf("%w: invalid email", ErrInvalid)
	}
	return e, nil
}

func (s *Service) Register(ctx context.Context, email, password string) (string, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		return "", err
	}
	if len(password) < 8 || len(password) > 128 {
		return "", fmt.Errorf("%w: password must be 8-128 characters", ErrInvalid)
	}
	hash, err := hashPassword(password)
	if err != nil {
		return "", err
	}
	id := uuid.NewString()
	if err := s.store.CreateUser(ctx, id, email, hash); err != nil {
		return "", err
	}
	return id, nil
}

func (s *Service) Login(ctx context.Context, email, password string) (TokenPair, error) {
	email, err := normalizeEmail(email)
	if err != nil {
		return TokenPair{}, ErrBadCredentials
	}
	u, err := s.store.UserByEmail(ctx, email)
	if errors.Is(err, ErrNotFound) {
		verifyPassword(password, dummyHash()) // keep timing similar
		return TokenPair{}, ErrBadCredentials
	}
	if err != nil {
		return TokenPair{}, err
	}
	if !verifyPassword(password, u.PasswordHash) {
		return TokenPair{}, ErrBadCredentials
	}

	refresh, hash, err := newRefreshToken()
	if err != nil {
		return TokenPair{}, err
	}
	if err := s.store.InsertRefresh(ctx, u.ID, uuid.NewString(), hash, time.Now().Add(s.refreshTTL)); err != nil {
		return TokenPair{}, err
	}
	return s.pair(u.ID, refresh)
}

func (s *Service) Refresh(ctx context.Context, token string) (TokenPair, error) {
	newToken, newHash, err := newRefreshToken()
	if err != nil {
		return TokenPair{}, err
	}
	userID, err := s.store.RotateRefresh(ctx, hashToken(token), newHash, time.Now().Add(s.refreshTTL))
	switch {
	case errors.Is(err, ErrTokenReuse):
		s.log.Warn("refresh token reuse detected, session revoked")
		return TokenPair{}, ErrInvalidToken
	case err != nil:
		return TokenPair{}, err
	}
	return s.pair(userID, newToken)
}

func (s *Service) Logout(ctx context.Context, token string) error {
	return s.store.RevokeFamily(ctx, hashToken(token))
}

func (s *Service) PublicKey() (keyID string, key []byte) {
	return s.signer.keyID, []byte(s.signer.pub)
}

func (s *Service) pair(userID, refresh string) (TokenPair, error) {
	access, err := s.signer.accessToken(userID, s.accessTTL)
	if err != nil {
		return TokenPair{}, fmt.Errorf("sign access token: %w", err)
	}
	return TokenPair{Access: access, Refresh: refresh, ExpiresInSeconds: int64(s.accessTTL.Seconds())}, nil
}
