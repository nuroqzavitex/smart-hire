package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/auth/internal/domain"
)

// UserRepo implements app.UserRepo.
type UserRepo struct{ pool *pgxpool.Pool }

// NewUserRepo returns a UserRepo over pool.
func NewUserRepo(pool *pgxpool.Pool) *UserRepo { return &UserRepo{pool: pool} }

// Create inserts u; a duplicate email yields apperr.ErrConflict.
func (r *UserRepo) Create(ctx context.Context, u domain.User) error {
	err := queries(ctx, r.pool).CreateUser(ctx, sqlc.CreateUserParams{
		ID: u.ID, Email: u.Email, PasswordHash: u.PasswordHash, Role: string(u.Role),
		Status: string(u.Status), CreatedAt: u.CreatedAt, UpdatedAt: u.UpdatedAt,
	})
	if isUniqueViolation(err) {
		return apperr.New(apperr.ErrConflict, "email already registered")
	}
	if err != nil {
		return fmt.Errorf("insert user: %w", err)
	}
	return nil
}

// GetByEmail finds a user by (case-insensitive) email.
func (r *UserRepo) GetByEmail(ctx context.Context, email string) (domain.User, error) {
	row, err := queries(ctx, r.pool).GetUserByEmail(ctx, email)
	if err != nil {
		return domain.User{}, fmt.Errorf("get user by email: %w", mapNotFound(err, "user not found"))
	}
	return toUser(row), nil
}

// GetByID finds a user by ID.
func (r *UserRepo) GetByID(ctx context.Context, id uuid.UUID) (domain.User, error) {
	row, err := queries(ctx, r.pool).GetUserByID(ctx, id)
	if err != nil {
		return domain.User{}, fmt.Errorf("get user by id: %w", mapNotFound(err, "user not found"))
	}
	return toUser(row), nil
}

func toUser(r sqlc.User) domain.User {
	return domain.User{
		ID: r.ID, Email: r.Email, PasswordHash: r.PasswordHash, Role: authz.Role(r.Role),
		Status: domain.UserStatus(r.Status), CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}
