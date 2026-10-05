package user

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("profile not found")

type Profile struct {
	UserID   string
	Email    string
	FullName string
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// CreateFromEvent is idempotent: replaying user.created changes nothing.
func (s *Store) CreateFromEvent(ctx context.Context, userID, email string) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO profiles (user_id, email) VALUES ($1::uuid, $2)
		ON CONFLICT (user_id) DO NOTHING`, userID, email)
	if err != nil {
		return fmt.Errorf("create profile: %w", err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, userID string) (Profile, error) {
	var p Profile
	err := s.db.QueryRow(ctx,
		`SELECT user_id::text, email, full_name FROM profiles WHERE user_id = $1::uuid`, userID).
		Scan(&p.UserID, &p.Email, &p.FullName)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, fmt.Errorf("get profile: %w", err)
	}
	return p, nil
}

func (s *Store) UpdateName(ctx context.Context, userID, fullName string) (Profile, error) {
	var p Profile
	err := s.db.QueryRow(ctx, `
		UPDATE profiles SET full_name = $2, updated_at = now()
		WHERE user_id = $1::uuid
		RETURNING user_id::text, email, full_name`, userID, fullName).
		Scan(&p.UserID, &p.Email, &p.FullName)
	if errors.Is(err, pgx.ErrNoRows) {
		return Profile{}, ErrNotFound
	}
	if err != nil {
		return Profile{}, fmt.Errorf("update profile: %w", err)
	}
	return p, nil
}
