package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/postgres/sqlc"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

var (
	_ app.CVRepo        = (*CVRepo)(nil)
	_ app.CandidateRepo = (*CandidateRepo)(nil)
	_ app.TxManager     = (*TxManager)(nil)
	_ app.OutboxWriter  = (*OutboxWriter)(nil)
	_ app.Idempotency   = (*Idempotency)(nil)
)

// CVRepo implements app.CVRepo.
type CVRepo struct{ pool *pgxpool.Pool }

// NewCVRepo returns a CVRepo over pool.
func NewCVRepo(pool *pgxpool.Pool) *CVRepo { return &CVRepo{pool: pool} }

// Create inserts cv.
func (r *CVRepo) Create(ctx context.Context, cv domain.CV) error {
	err := queries(ctx, r.pool).InsertCV(ctx, sqlc.InsertCVParams{
		ID: cv.ID, CandidateID: cv.CandidateID, ObjectKey: cv.ObjectKey, FileName: cv.FileName,
		Status: string(cv.Status), AttemptID: optText(cv.AttemptID), ParsedData: cv.ParsedData,
		ParseError: toText(cv.ParseError), SizeBytes: toInt8(cv.SizeBytes), ConsentAt: cv.ConsentAt,
		ParsedAt: cv.ParsedAt, Version: cv.Version, CreatedAt: cv.CreatedAt, UpdatedAt: cv.UpdatedAt,
	})
	if err != nil {
		return fmt.Errorf("insert cv: %w", err)
	}
	return nil
}

// Get returns domain.ErrCVNotFound when the CV does not exist.
func (r *CVRepo) Get(ctx context.Context, id uuid.UUID) (domain.CV, error) {
	row, err := queries(ctx, r.pool).GetCV(ctx, id)
	if err != nil {
		return domain.CV{}, mapCVNotFound(err, "get cv")
	}
	return toCV(row), nil
}

// GetForUpdate is Get with a row lock (SELECT ... FOR UPDATE); call it inside a tx.
func (r *CVRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (domain.CV, error) {
	row, err := queries(ctx, r.pool).GetCVForUpdate(ctx, id)
	if err != nil {
		return domain.CV{}, mapCVNotFound(err, "get cv for update")
	}
	return toCV(row), nil
}

// Update writes every mutable field of cv.
func (r *CVRepo) Update(ctx context.Context, cv domain.CV) error {
	n, err := queries(ctx, r.pool).UpdateCV(ctx, sqlc.UpdateCVParams{
		ID: cv.ID, Status: string(cv.Status), AttemptID: optText(cv.AttemptID), ParsedData: cv.ParsedData,
		ParseError: toText(cv.ParseError), SizeBytes: toInt8(cv.SizeBytes), ParsedAt: cv.ParsedAt,
		Version: cv.Version, UpdatedAt: cv.UpdatedAt,
	})
	if err != nil {
		return fmt.Errorf("update cv: %w", err)
	}
	if n == 0 {
		return domain.ErrCVNotFound
	}
	return nil
}

// ListByCandidate returns up to limit CVs of candidateID after the cursor,
// ordered by created_at DESC, id DESC.
func (r *CVRepo) ListByCandidate(ctx context.Context, candidateID uuid.UUID, after *app.Cursor, limit int) ([]domain.CV, error) {
	var afterID *uuid.UUID
	var afterAt *time.Time
	if after != nil {
		afterID, afterAt = &after.ID, &after.At
	}
	rows, err := queries(ctx, r.pool).ListCVsByCandidate(ctx, sqlc.ListCVsByCandidateParams{
		CandidateID: candidateID, AfterID: afterID, AfterAt: afterAt,
		Lim: int32(limit), //nolint:gosec // limit is capped by the app layer
	})
	if err != nil {
		return nil, fmt.Errorf("list cvs: %w", err)
	}
	out := make([]domain.CV, len(rows))
	for i, row := range rows {
		out[i] = toCV(row)
	}
	return out, nil
}

func mapCVNotFound(err error, op string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrCVNotFound
	}
	return fmt.Errorf("%s: %w", op, err)
}

func toCV(r sqlc.Cv) domain.CV {
	cv := domain.CV{
		ID: r.ID, CandidateID: r.CandidateID, ObjectKey: r.ObjectKey, FileName: r.FileName,
		Status: domain.Status(r.Status), AttemptID: r.AttemptID.String, ParsedData: r.ParsedData,
		ParseError: fromText(r.ParseError), ConsentAt: r.ConsentAt, ParsedAt: r.ParsedAt,
		Version: r.Version, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
	if r.SizeBytes.Valid {
		n := r.SizeBytes.Int64
		cv.SizeBytes = &n
	}
	return cv
}

// optText stores an empty string as NULL (attempt_id before the first parse).
func optText(s string) pgtype.Text {
	return pgtype.Text{String: s, Valid: s != ""}
}

func toInt8(n *int64) pgtype.Int8 {
	if n == nil {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: *n, Valid: true}
}
