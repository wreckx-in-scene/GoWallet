package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const outboxTopic = "user.events"

var (
	ErrNotFound   = errors.New("not found")
	ErrTokenReuse = errors.New("refresh token reuse detected")
)

type User struct {
	ID           string
	Email        string
	PasswordHash string
}

type userCreated struct {
	Type   string `json:"type"`
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// CreateUser inserts the user and the user.created outbox event in one transaction.
func (s *Store) CreateUser(ctx context.Context, id, email, passwordHash string) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx,
		`INSERT INTO users (id, email, password_hash) VALUES ($1::uuid, $2, $3)`,
		id, email, passwordHash)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
			return ErrEmailTaken
		}
		return fmt.Errorf("insert user: %w", err)
	}

	ev := userCreated{Type: "user.created", UserID: id, Email: email}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		outboxTopic, id, ev); err != nil {
		return fmt.Errorf("write outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (s *Store) UserByEmail(ctx context.Context, email string) (User, error) {
	var u User
	err := s.db.QueryRow(ctx,
		`SELECT id::text, email, password_hash FROM users WHERE email = $1`, email).
		Scan(&u.ID, &u.Email, &u.PasswordHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	if err != nil {
		return User{}, fmt.Errorf("load user: %w", err)
	}
	return u, nil
}

func (s *Store) InsertRefresh(ctx context.Context, userID, familyID, tokenHash string, expires time.Time) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO refresh_tokens (id, user_id, family_id, token_hash, expires_at)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5)`,
		uuid.NewString(), userID, familyID, tokenHash, expires)
	if err != nil {
		return fmt.Errorf("insert refresh token: %w", err)
	}
	return nil
}

// RotateRefresh consumes the old token and stores the new one in the same family.
// Presenting a token that was already used or revoked revokes the whole family.
func (s *Store) RotateRefresh(ctx context.Context, oldHash, newHash string, newExpires time.Time) (string, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var id, userID, familyID string
	var expiresAt time.Time
	var usedAt, revokedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT id::text, user_id::text, family_id::text, expires_at, used_at, revoked_at
		FROM refresh_tokens WHERE token_hash = $1 FOR UPDATE`, oldHash).
		Scan(&id, &userID, &familyID, &expiresAt, &usedAt, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrInvalidToken
	}
	if err != nil {
		return "", fmt.Errorf("load refresh token: %w", err)
	}

	if usedAt != nil || revokedAt != nil {
		// An old token is being presented again: assume it was stolen.
		if _, err := tx.Exec(ctx, `
			UPDATE refresh_tokens SET revoked_at = now()
			WHERE family_id = $1::uuid AND revoked_at IS NULL`, familyID); err != nil {
			return "", fmt.Errorf("revoke family: %w", err)
		}
		if err := tx.Commit(ctx); err != nil { // commit the revocation
			return "", fmt.Errorf("commit: %w", err)
		}
		return "", ErrTokenReuse
	}
	if time.Now().After(expiresAt) {
		return "", ErrInvalidToken
	}

	if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET used_at = now() WHERE id = $1::uuid`, id); err != nil {
		return "", fmt.Errorf("mark used: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO refresh_tokens (id, user_id, family_id, token_hash, expires_at)
		VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5)`,
		uuid.NewString(), userID, familyID, newHash, newExpires); err != nil {
		return "", fmt.Errorf("insert rotated token: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit: %w", err)
	}
	return userID, nil
}

// RevokeFamily ends the session the given refresh token belongs to (logout).
func (s *Store) RevokeFamily(ctx context.Context, tokenHash string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE refresh_tokens SET revoked_at = now()
		WHERE revoked_at IS NULL
		  AND family_id = (SELECT family_id FROM refresh_tokens WHERE token_hash = $1)`, tokenHash)
	if err != nil {
		return fmt.Errorf("revoke family: %w", err)
	}
	return nil
}
