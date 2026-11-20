package app

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

const (
	defaultPageSize = 20
	maxPageSize     = 50
)

// PageRequest selects a page; Size 0 means the default (20).
type PageRequest struct {
	Size  int
	Token string
}

// Page is one page of CVs; NextToken is empty on the last page.
type Page struct {
	CVs       []domain.CV
	NextToken string
}

func (pr PageRequest) size() (int, error) {
	switch {
	case pr.Size == 0:
		return defaultPageSize, nil
	case pr.Size < 0 || pr.Size > maxPageSize:
		return 0, apperr.New(apperr.ErrValidation, "page_size must be between 1 and 50")
	}
	return pr.Size, nil
}

// GetCV returns one of the caller's CVs.
func (s *Service) GetCV(ctx context.Context, id uuid.UUID) (domain.CV, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return domain.CV{}, err
	}
	return s.ownedCV(ctx, s.CVs.Get, id, uid)
}

// ListMyCVs pages through the caller's CVs, newest first.
func (s *Service) ListMyCVs(ctx context.Context, pr PageRequest) (Page, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return Page{}, err
	}
	size, err := pr.size()
	if err != nil {
		return Page{}, err
	}
	var after *Cursor
	if pr.Token != "" {
		c, err := DecodeCursor(pr.Token)
		if err != nil {
			return Page{}, err
		}
		after = &c
	}
	cvs, err := s.CVs.ListByCandidate(ctx, uid, after, size+1)
	if err != nil {
		return Page{}, fmt.Errorf("list cvs: %w", err)
	}
	more := len(cvs) > size
	if more {
		cvs = cvs[:size]
	}
	page := Page{CVs: cvs}
	if more {
		last := cvs[len(cvs)-1]
		page.NextToken = EncodeCursor(Cursor{At: last.CreatedAt, ID: last.ID})
	}
	return page, nil
}

// UpdateParsedData replaces the parsed data of a PARSED CV with the caller's
// edits. No event is emitted.
func (s *Service) UpdateParsedData(ctx context.Context, id uuid.UUID, p domain.ParsedCV) (domain.CV, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return domain.CV{}, err
	}
	doc, err := p.MarshalCanonical()
	if err != nil {
		return domain.CV{}, fmt.Errorf("marshal parsed data: %w", err)
	}
	if err := s.Schema.ValidateParsedCV(doc); err != nil {
		// The schema error echoes field values (PII); keep it out.
		return domain.CV{}, apperr.New(apperr.ErrValidation, "invalid parsed data")
	}
	return s.mutate(ctx, id, uid, func(cv *domain.CV) (bool, error) {
		return false, cv.UpdateParsed(doc, s.Now())
	})
}

// ReparseCV discards the CV's parse result and starts a new attempt.
func (s *Service) ReparseCV(ctx context.Context, id uuid.UUID) (domain.CV, error) {
	uid, err := candidate(ctx)
	if err != nil {
		return domain.CV{}, err
	}
	return s.mutate(ctx, id, uid, func(cv *domain.CV) (bool, error) {
		return true, cv.Reparse(s.NewAttemptID(), s.Now())
	})
}

// mutate locks an owned CV, applies fn and saves it, emitting cv.uploaded
// when fn asks for it, all in one transaction. The clock is read by fn only
// after the lock.
func (s *Service) mutate(ctx context.Context, id, uid uuid.UUID,
	fn func(cv *domain.CV) (emit bool, err error),
) (domain.CV, error) {
	var out domain.CV
	err := s.Tx.WithTx(ctx, func(ctx context.Context) error {
		cv, err := s.ownedCV(ctx, s.CVs.GetForUpdate, id, uid)
		if err != nil {
			return err
		}
		emit, err := fn(&cv)
		if err != nil {
			return err
		}
		if err := s.CVs.Update(ctx, cv); err != nil {
			return fmt.Errorf("update cv: %w", err)
		}
		if err := s.emitUpdated(ctx, cv, cv.UpdatedAt); err != nil {
			return err
		}
		if emit {
			if err := s.emitUploaded(ctx, cv, cv.UpdatedAt); err != nil {
				return err
			}
		}
		out = cv
		return nil
	})
	if err != nil {
		return domain.CV{}, err
	}
	return out, nil
}
