package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

const pdfMagic = "%PDF-"

// UploadTicket is a presigned PUT URL for a new CV.
type UploadTicket struct {
	CVID      uuid.UUID
	URL       string
	ExpiresAt time.Time
}

// CreateUploadURL records the caller's consent, creates an UPLOADING CV and
// returns a presigned PUT URL for its PDF. fileName is display-only.
func (s *Service) CreateUploadURL(ctx context.Context, consent bool, fileName string) (UploadTicket, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return UploadTicket{}, err
	}
	if !consent {
		return UploadTicket{}, apperr.New(apperr.ErrValidation, "consent is required")
	}
	now := s.Now()
	cv, err := domain.NewCV(s.NewID(), uid, fileName, now)
	if err != nil {
		return UploadTicket{}, err
	}
	var url string
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		if err := s.CVs.Create(ctx, cv); err != nil {
			return fmt.Errorf("create cv: %w", err)
		}
		// Presigning is a local signature; doing it in the tx means a failure
		// leaves no orphan CV behind.
		url, err = s.Storage.PresignPut(ctx, cv.ObjectKey, domain.PDFContentType, s.UploadTTL)
		if err != nil {
			return fmt.Errorf("presign upload: %w", err)
		}
		return nil
	})
	if err != nil {
		return UploadTicket{}, err
	}
	return UploadTicket{CVID: cv.ID, URL: url, ExpiresAt: now.Add(s.UploadTTL)}, nil
}

// CompleteUpload verifies the uploaded object and starts the first parse
// attempt (decision #7). The object checks run outside the transaction; an
// invalid object is deleted and the CV stays UPLOADING so the client can PUT
// again. A CV that is no longer UPLOADING is returned unchanged.
func (s *Service) CompleteUpload(ctx context.Context, id uuid.UUID) (domain.CV, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return domain.CV{}, err
	}
	cv, err := s.ownedCV(ctx, s.CVs.Get, id, uid)
	if err != nil {
		return domain.CV{}, err
	}
	if cv.Status != domain.StatusUploading {
		return cv, nil
	}
	size, err := s.checkObject(ctx, cv)
	if err != nil {
		return domain.CV{}, err
	}
	var out domain.CV
	err = s.Tx.WithTx(ctx, func(ctx context.Context) error {
		cv, err := s.CVs.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if cv.Status != domain.StatusUploading { // a concurrent complete won
			out = cv
			return nil
		}
		now := s.Now()
		if err := cv.Complete(size, s.NewAttemptID(), now); err != nil {
			return err
		}
		if err := s.CVs.Update(ctx, cv); err != nil {
			return fmt.Errorf("update cv: %w", err)
		}
		if err := s.emitUploaded(ctx, cv, now); err != nil {
			return err
		}
		out = cv
		return nil
	})
	if err != nil {
		return domain.CV{}, err
	}
	return out, nil
}

// checkObject validates the uploaded object and returns its size. Invalid
// objects are deleted.
func (s *Service) checkObject(ctx context.Context, cv domain.CV) (int64, error) {
	notFound := apperr.New(apperr.ErrValidation, "uploaded file not found")
	info, err := s.Storage.Stat(ctx, cv.ObjectKey)
	if errors.Is(err, ErrObjectNotFound) {
		return 0, notFound
	}
	if err != nil {
		return 0, fmt.Errorf("stat cv object: %w", err)
	}
	if info.Size > domain.MaxCVBytes {
		return 0, s.reject(ctx, cv, "file exceeds 5 MiB")
	}
	if info.ContentType != domain.PDFContentType {
		return 0, s.reject(ctx, cv, "file must be application/pdf")
	}
	head, err := s.Storage.ReadHead(ctx, cv.ObjectKey, int64(len(pdfMagic)))
	if errors.Is(err, ErrObjectNotFound) {
		return 0, notFound
	}
	if err != nil {
		return 0, fmt.Errorf("read cv object: %w", err)
	}
	if string(head) != pdfMagic {
		return 0, s.reject(ctx, cv, "file is not a PDF")
	}
	return info.Size, nil
}

// reject deletes an invalid object and returns the validation error. A failed
// delete is only logged: the client may PUT again, overwriting the object.
// The storage error is not logged because it may carry the object key.
func (s *Service) reject(ctx context.Context, cv domain.CV, msg string) error {
	if err := s.Storage.Delete(ctx, cv.ObjectKey); err != nil {
		s.Logger.WarnContext(ctx, "delete rejected cv object failed", "cv_id", cv.ID.String())
	}
	return apperr.New(apperr.ErrValidation, msg)
}

// ownedCV loads a CV with get and hides CVs of other candidates as
// domain.ErrCVNotFound.
func (s *Service) ownedCV(ctx context.Context, get func(context.Context, uuid.UUID) (domain.CV, error),
	id, uid uuid.UUID,
) (domain.CV, error) {
	cv, err := get(ctx, id)
	if err != nil {
		return domain.CV{}, err
	}
	if cv.CandidateID != uid {
		return domain.CV{}, domain.ErrCVNotFound
	}
	return cv, nil
}
