package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// QuotaResult is the outcome of QuotaRepo.Reserve.
type QuotaResult int

// Reserve outcomes.
const (
	QuotaReserved QuotaResult = iota + 1
	QuotaExhausted
	QuotaAlreadyReleased
)

// QuotaRepo keeps the daily application quota and one reservation per
// application. Both methods must run inside the transaction carried by ctx.
type QuotaRepo interface {
	// Reserve is idempotent per appID: a RESERVED row answers QuotaReserved
	// without counting again, a RELEASED row answers QuotaAlreadyReleased.
	Reserve(ctx context.Context, appID, candidateID uuid.UUID, day time.Time, limit int) (QuotaResult, error)
	// Release frees a reservation once; for an unknown appID it records a
	// RELEASED tombstone so a late Reserve cannot take quota.
	Release(ctx context.Context, appID uuid.UUID) error
}

// ReserveQuota holds one unit of the candidate's quota for the UTC day of
// Now on behalf of the application saga (internal RPC, no caller identity).
func (s *Service) ReserveQuota(ctx context.Context, appID, candidateID uuid.UUID) error {
	now := s.Now().UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	var res QuotaResult
	err := s.Tx.WithTx(ctx, func(ctx context.Context) error {
		var err error
		res, err = s.Quota.Reserve(ctx, appID, candidateID, day, s.DailyQuota)
		return err
	})
	if err != nil {
		return err
	}
	switch res {
	case QuotaExhausted:
		return domain.ErrQuotaExhausted
	case QuotaAlreadyReleased:
		return domain.ErrReservationReleased
	}
	return nil
}

// ReleaseQuota gives back the application's reservation (saga compensation).
func (s *Service) ReleaseQuota(ctx context.Context, appID uuid.UUID) error {
	return s.Tx.WithTx(ctx, func(ctx context.Context) error {
		return s.Quota.Release(ctx, appID)
	})
}
