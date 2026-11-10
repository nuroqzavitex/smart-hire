// Package domain holds the job service's entities and invariants. It has no
// dependency on infrastructure.
package domain

import (
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
)

const (
	minTitleRunes = 5
	maxTitleRunes = 200
	minDescRunes  = 20
	maxDescRunes  = 20000
	maxLocRunes   = 100
)

// Status is the lifecycle state of a job posting.
type Status string

// Job statuses.
const (
	StatusDraft  Status = "draft"
	StatusOpen   Status = "open"
	StatusClosed Status = "closed"
)

// ParseStatus validates a status string.
func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusDraft, StatusOpen, StatusClosed:
		return st, nil
	}
	return "", apperr.New(apperr.ErrValidation, "invalid job status")
}

// Level is the seniority level of a job.
type Level string

// Job levels.
const (
	LevelIntern Level = "intern"
	LevelJunior Level = "junior"
	LevelMiddle Level = "middle"
	LevelSenior Level = "senior"
	LevelLead   Level = "lead"
)

// ParseLevel validates a level string.
func ParseLevel(s string) (Level, error) {
	switch l := Level(s); l {
	case LevelIntern, LevelJunior, LevelMiddle, LevelSenior, LevelLead:
		return l, nil
	}
	return "", apperr.New(apperr.ErrValidation, "invalid job level")
}

// Job is a job posting owned by a recruiter.
type Job struct {
	ID, RecruiterID  uuid.UUID
	Title            string
	Description      string
	RequiredSkills   []string
	NiceToHaveSkills []string
	Level            Level
	Location         string
	SalaryMin        *int
	SalaryMax        *int
	Status           Status
	Version          int64
	CreatedAt        time.Time
	UpdatedAt        time.Time
	PublishedAt      *time.Time
	DeletedAt        *time.Time
}

// Draft is the input for creating a job.
type Draft struct {
	Title            string
	Description      string
	RequiredSkills   []string
	NiceToHaveSkills []string
	Level            Level
	Location         string
	SalaryMin        *int
	SalaryMax        *int
}

// Patch is a partial update; nil fields are left unchanged.
type Patch struct {
	Title            *string
	Description      *string
	RequiredSkills   *[]string
	NiceToHaveSkills *[]string
	Level            *Level
	Location         *string
	SalaryMin        *int
	SalaryMax        *int
}

// Empty reports whether the patch changes nothing.
func (p Patch) Empty() bool {
	return p.Title == nil && p.Description == nil && p.RequiredSkills == nil &&
		p.NiceToHaveSkills == nil && p.Level == nil && p.Location == nil &&
		p.SalaryMin == nil && p.SalaryMax == nil
}

// NewJob validates a draft and returns a new job in draft status.
func NewJob(id, recruiterID uuid.UUID, d Draft, now time.Time) (Job, error) {
	j := Job{
		ID:               id,
		RecruiterID:      recruiterID,
		Title:            d.Title,
		Description:      d.Description,
		RequiredSkills:   d.RequiredSkills,
		NiceToHaveSkills: d.NiceToHaveSkills,
		Level:            d.Level,
		Location:         d.Location,
		SalaryMin:        d.SalaryMin,
		SalaryMax:        d.SalaryMax,
		Status:           StatusDraft,
		Version:          1,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := j.normalizeAndValidate(); err != nil {
		return Job{}, err
	}
	return j, nil
}

// Apply merges a patch and validates the result. On error j is unchanged.
func (j *Job) Apply(p Patch, now time.Time) error {
	if j.Status == StatusClosed {
		return ErrJobClosed
	}
	next := *j
	if p.Title != nil {
		next.Title = *p.Title
	}
	if p.Description != nil {
		next.Description = *p.Description
	}
	if p.RequiredSkills != nil {
		next.RequiredSkills = *p.RequiredSkills
	}
	if p.NiceToHaveSkills != nil {
		next.NiceToHaveSkills = *p.NiceToHaveSkills
	}
	if p.Level != nil {
		next.Level = *p.Level
	}
	if p.Location != nil {
		next.Location = *p.Location
	}
	if p.SalaryMin != nil {
		next.SalaryMin = p.SalaryMin
	}
	if p.SalaryMax != nil {
		next.SalaryMax = p.SalaryMax
	}
	if err := next.normalizeAndValidate(); err != nil {
		return err
	}
	next.touch(now)
	*j = next
	return nil
}

// Publish moves a draft job to open.
func (j *Job) Publish(now time.Time) error {
	if j.Status != StatusDraft {
		return ErrInvalidTransition
	}
	j.Status = StatusOpen
	j.PublishedAt = &now
	j.touch(now)
	return nil
}

// Close moves an open job to closed.
func (j *Job) Close(now time.Time) error {
	if j.Status != StatusOpen {
		return ErrInvalidTransition
	}
	j.Status = StatusClosed
	j.touch(now)
	return nil
}

// Delete soft-deletes a draft or closed job.
func (j *Job) Delete(now time.Time) error {
	if j.Status != StatusDraft && j.Status != StatusClosed {
		return ErrInvalidTransition
	}
	j.DeletedAt = &now
	j.touch(now)
	return nil
}

// OwnedBy reports whether subject is the recruiter who owns the job.
func (j Job) OwnedBy(subject string) bool {
	return subject != "" && j.RecruiterID.String() == subject
}

func (j *Job) touch(now time.Time) {
	j.Version++
	j.UpdatedAt = now
}

func (j *Job) normalizeAndValidate() error {
	j.Title = strings.TrimSpace(j.Title)
	j.Location = strings.TrimSpace(j.Location)
	if err := checkLen(j.Title, minTitleRunes, maxTitleRunes, "title must be between 5 and 200 characters"); err != nil {
		return err
	}
	if err := checkLen(strings.TrimSpace(j.Description), minDescRunes, maxDescRunes, "description must be between 20 and 20000 characters"); err != nil {
		return err
	}
	if err := checkLen(j.Location, 1, maxLocRunes, "location must be between 1 and 100 characters"); err != nil {
		return err
	}
	var err error
	if j.RequiredSkills, err = NormalizeSkills(j.RequiredSkills); err != nil {
		return err
	}
	if j.NiceToHaveSkills, err = NormalizeSkills(j.NiceToHaveSkills); err != nil {
		return err
	}
	if _, err := ParseLevel(string(j.Level)); err != nil {
		return err
	}
	if j.SalaryMin != nil && *j.SalaryMin < 0 || j.SalaryMax != nil && *j.SalaryMax < 0 {
		return apperr.New(apperr.ErrValidation, "salary must not be negative")
	}
	if j.SalaryMin != nil && j.SalaryMax != nil && *j.SalaryMin > *j.SalaryMax {
		return apperr.New(apperr.ErrValidation, "salary_min must not exceed salary_max")
	}
	return nil
}

func checkLen(s string, lo, hi int, msg string) error {
	if n := utf8.RuneCountInString(s); n < lo || n > hi {
		return apperr.New(apperr.ErrValidation, msg)
	}
	return nil
}
