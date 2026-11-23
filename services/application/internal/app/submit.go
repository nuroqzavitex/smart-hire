package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/services/application/internal/domain"
)

const maxIdempotencyKey = 128

var errJobNotOpen = apperr.New(apperr.ErrConflict, "job is not open")

// SubmitInput is a candidate's application request.
type SubmitInput struct {
	JobID          uuid.UUID
	CVID           uuid.UUID
	IdempotencyKey string
}

// RequestHash fingerprints a submit request so a reused idempotency key with
// a different body is detected.
func RequestHash(jobID, cvID uuid.UUID) string {
	sum := sha256.Sum256([]byte(jobID.String() + ":" + cvID.String()))
	return hex.EncodeToString(sum[:])
}

// Submit is saga step T1 (spec §6.4): it checks the job is open and the CV
// is the caller's and parsed, then writes the PENDING application, its saga
// and the events in one transaction. The rest of the saga runs in the
// worker. Replaying the same idempotency key returns the stored application.
func (s *Service) Submit(ctx context.Context, in SubmitInput) (domain.Application, error) {
	if err := authz.RequireRole(ctx, authz.RoleCandidate); err != nil {
		return domain.Application{}, err
	}
	cand, _, err := caller(ctx)
	if err != nil {
		return domain.Application{}, err
	}
	if in.IdempotencyKey == "" || len(in.IdempotencyKey) > maxIdempotencyKey {
		return domain.Application{}, apperr.New(apperr.ErrValidation, "Idempotency-Key header must be 1-128 characters")
	}
	hash := RequestHash(in.JobID, in.CVID)
	if a, found, err := s.replay(ctx, cand, in.IdempotencyKey, hash); found || err != nil {
		return a, err
	}

	// The job service hides closed jobs from candidates (404); a job we saw
	// close is reported as such.
	closed, err := s.ClosedJobs.IsClosed(ctx, in.JobID)
	if err != nil {
		return domain.Application{}, err
	}
	if closed {
		return domain.Application{}, errJobNotOpen
	}
	job, err := s.Jobs.GetJob(ctx, in.JobID)
	if err != nil {
		return domain.Application{}, err
	}
	if !job.Open {
		return domain.Application{}, errJobNotOpen
	}
	cv, err := s.CVs.GetCV(ctx, in.CVID)
	if err != nil {
		return domain.Application{}, err
	}
	if !cv.Parsed {
		return domain.Application{}, apperr.New(apperr.ErrConflict, "cv is not parsed")
	}

	now := s.Now().UTC()
	a := domain.Application{
		ID: s.NewID(), JobID: in.JobID, CandidateID: cand, RecruiterID: job.RecruiterID, CVID: in.CVID,
		Status: domain.StatusPending, IdempotencyKey: in.IdempotencyKey, RequestHash: hash,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	saga := domain.Saga{
		ID: s.NewID(), ApplicationID: a.ID, JobID: a.JobID, Step: domain.StepReserveQuota,
		State: domain.SagaRunning, NextRunAt: now, DeadlineAt: now.Add(s.SagaDeadline),
		StepStartedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		if err := s.Applications.Create(ctx, a); err != nil {
			return err
		}
		if err := s.Sagas.Create(ctx, saga); err != nil {
			return err
		}
		if err := s.Applications.AddHistory(ctx, a.ID, domain.HistoryEntry{To: a.Status, Actor: cand.String(), At: now}); err != nil {
			return err
		}
		if err := s.emitSubmitted(ctx, a, now); err != nil {
			return err
		}
		return s.emitStatusChanged(ctx, a, nil, cand.String(), now)
	})
	if errors.Is(err, domain.ErrIdempotencyKeyConflict) {
		// A concurrent request with the same key committed first.
		a, found, err := s.replay(ctx, cand, in.IdempotencyKey, hash)
		if err == nil && !found {
			err = fmt.Errorf("application with idempotency key vanished")
		}
		return a, err
	}
	if err != nil {
		return domain.Application{}, err
	}
	s.Wake()
	return a, nil
}

// replay looks up an earlier application with the same idempotency key.
func (s *Service) replay(ctx context.Context, cand uuid.UUID, key, hash string) (domain.Application, bool, error) {
	a, err := s.Applications.GetByKey(ctx, cand, key)
	if errors.Is(err, domain.ErrApplicationNotFound) {
		return domain.Application{}, false, nil
	}
	if err != nil {
		return domain.Application{}, false, err
	}
	if a.RequestHash != hash {
		return domain.Application{}, true, apperr.New(apperr.ErrValidation, "idempotency key reused with a different request")
	}
	return a, true, nil
}
