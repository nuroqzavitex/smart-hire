package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/notification/internal/domain"
)

// notifiedStatuses are the recruiter-driven moves a candidate hears about.
var notifiedStatuses = map[string]bool{"INTERVIEW": true, "OFFERED": true, "HIRED": true, "REJECTED": true}

// payload decodes the fields any handled event may carry.
type payload struct {
	UserID        string `json:"user_id"`
	Email         string `json:"email"`
	Role          string `json:"role"`
	JobID         string `json:"job_id"`
	RecruiterID   string `json:"recruiter_id"`
	Title         string `json:"title"`
	Version       int64  `json:"version"`
	CVID          string `json:"cv_id"`
	CandidateID   string `json:"candidate_id"`
	Reason        string `json:"reason"`
	ApplicationID string `json:"application_id"`
	To            string `json:"to"`
	Actor         string `json:"actor"`
	Top           []struct {
		CandidateID string `json:"candidate_id"`
	} `json:"top"`
}

// pending is a notification to create in the event's transaction.
type pending struct {
	user  uuid.UUID
	kind  domain.Type
	view  view
	data  map[string]string
	email bool
}

// HandleEvent consumes one event (spec §4 of the M4a design): replica
// updates and at most one notification, with its queued email, in one
// transaction deduplicated by event id; the notification is pushed to open
// WebSockets only after the commit. Unknown event types are ignored.
// Returned errors never contain payload values (PII).
func (s *Service) HandleEvent(ctx context.Context, raw []byte) error {
	var env event.Envelope
	if err := json.Unmarshal(raw, &env); err != nil || env.EventID == "" {
		return fmt.Errorf("%w: malformed envelope", ErrPoisonEvent)
	}
	if env.EventType == userdeletion.TopicDeleted {
		return s.handleUserDeleted(ctx, env)
	}
	var p payload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return fmt.Errorf("%w: malformed %s payload", ErrPoisonEvent, env.EventType)
	}
	var created []domain.Notification
	err := s.Tx.WithTx(ctx, func(ctx context.Context) error {
		created = nil
		first, err := s.Idem.Once(ctx, env.EventID)
		if err != nil || !first {
			return err
		}
		todo, err := s.apply(ctx, env.EventType, p)
		if err != nil {
			return err
		}
		if todo == nil {
			return nil
		}
		if gone, err := s.deletedUser(ctx, todo.user); err != nil || gone {
			return err // nobody to tell about a deleted account
		}
		n, err := s.create(ctx, *todo)
		if err != nil {
			return err
		}
		created = append(created, n)
		return nil
	})
	if err != nil {
		return err
	}
	for _, n := range created {
		notificationCreated(ctx, n.Type)
		s.Pusher.Push(n.UserID, n)
	}
	return nil
}

// apply updates replicas and decides the notification, if any.
func (s *Service) apply(ctx context.Context, eventType string, p payload) (*pending, error) {
	switch eventType {
	case "user.registered":
		id, err := ids(eventType, p.UserID)
		if err != nil {
			return nil, err
		}
		if err := s.Users.Upsert(ctx, domain.User{ID: id[0], Email: p.Email, Role: p.Role}); err != nil {
			return nil, err
		}
		return &pending{user: id[0], kind: domain.TypeWelcome, email: true}, nil
	case "job.created", "job.updated":
		id, err := ids(eventType, p.JobID, p.RecruiterID)
		if err != nil {
			return nil, err
		}
		_, err = s.Jobs.Upsert(ctx, domain.Job{ID: id[0], RecruiterID: id[1], Title: p.Title, Version: p.Version})
		return nil, err
	case "job.deleted":
		id, err := ids(eventType, p.JobID)
		if err != nil {
			return nil, err
		}
		_, err = s.Jobs.Upsert(ctx, domain.Job{ID: id[0], Version: p.Version, Deleted: true})
		return nil, err
	case "cv.parsed", "cv.parse_failed":
		id, err := ids(eventType, p.CandidateID, p.CVID)
		if err != nil {
			return nil, err
		}
		kind := domain.TypeCVParsed
		if eventType == "cv.parse_failed" {
			kind = domain.TypeCVParseFailed
		}
		return &pending{user: id[0], kind: kind, view: view{Reason: p.Reason}, data: map[string]string{"cv_id": id[1].String()}}, nil
	case "application.submitted":
		id, err := ids(eventType, p.RecruiterID, p.JobID, p.ApplicationID)
		if err != nil {
			return nil, err
		}
		title, err := s.jobTitle(ctx, id[1])
		if err != nil {
			return nil, err
		}
		return &pending{user: id[0], kind: domain.TypeApplicationReceived, view: view{JobTitle: title}, email: true,
			data: map[string]string{"job_id": id[1].String(), "application_id": id[2].String()}}, nil
	case "application.status_changed":
		if !notifiedStatuses[p.To] || p.Actor == "system" || p.Actor == "" {
			return nil, nil
		}
		id, err := ids(eventType, p.CandidateID, p.JobID, p.ApplicationID)
		if err != nil {
			return nil, err
		}
		title, err := s.jobTitle(ctx, id[1])
		if err != nil {
			return nil, err
		}
		return &pending{user: id[0], kind: domain.TypeApplicationStatus, email: true,
			view: view{JobTitle: title, StatusLabel: statusLabels[p.To]},
			data: map[string]string{"job_id": id[1].String(), "application_id": id[2].String(), "status": p.To}}, nil
	case "match.completed":
		if len(p.Top) == 0 {
			return nil, nil
		}
		id, err := ids(eventType, p.JobID)
		if err != nil {
			return nil, err
		}
		job, err := s.Jobs.Get(ctx, id[0])
		if errors.Is(err, domain.ErrJobNotFound) || err == nil && (job.Deleted || job.RecruiterID == uuid.Nil) {
			return nil, nil // owner unknown: nobody to tell
		}
		if err != nil {
			return nil, err
		}
		return &pending{user: job.RecruiterID, kind: domain.TypeCandidatesMatched,
			view: view{JobTitle: job.Title, Count: len(p.Top)},
			data: map[string]string{"job_id": id[0].String(), "count": strconv.Itoa(len(p.Top))}}, nil
	}
	return nil, nil
}

// deletedUser reports whether the user's replica is a tombstone.
func (s *Service) deletedUser(ctx context.Context, id uuid.UUID) (bool, error) {
	u, err := s.Users.Get(ctx, id)
	if errors.Is(err, domain.ErrUserNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return u.Deleted, nil
}

// jobTitle returns the replica's title or a generic phrase.
func (s *Service) jobTitle(ctx context.Context, jobID uuid.UUID) (string, error) {
	job, err := s.Jobs.Get(ctx, jobID)
	if errors.Is(err, domain.ErrJobNotFound) || err == nil && (job.Deleted || job.Title == "") {
		return fallbackJobTitle, nil
	}
	if err != nil {
		return "", err
	}
	return job.Title, nil
}

// create stores the notification and, when wanted and the user's address is
// known, its email.
func (s *Service) create(ctx context.Context, p pending) (domain.Notification, error) {
	title, body, err := render(p.kind, p.view)
	if err != nil {
		return domain.Notification{}, err
	}
	now := s.Now().UTC()
	n := domain.Notification{ID: s.NewID(), UserID: p.user, Type: p.kind, Title: title, Body: body, Data: p.data, CreatedAt: now}
	if n.Data == nil {
		n.Data = map[string]string{}
	}
	if err := s.Notifications.Create(ctx, n); err != nil {
		return domain.Notification{}, err
	}
	if !p.email {
		return n, nil
	}
	u, err := s.Users.Get(ctx, p.user)
	if errors.Is(err, domain.ErrUserNotFound) || err == nil && u.Email == "" {
		return n, nil // registered before M4a: in-app only
	}
	if err != nil {
		return domain.Notification{}, err
	}
	html, err := renderEmail(title, body)
	if err != nil {
		return domain.Notification{}, err
	}
	return n, s.Emails.Enqueue(ctx, domain.Email{
		ID: s.NewID(), NotificationID: n.ID, To: u.Email, Subject: title, HTML: html, Text: body + "\n",
		Status: domain.EmailPending, NextAttemptAt: now, CreatedAt: now,
	})
}

// ids parses uuids of an event's payload; a bad one makes the event poison.
func ids(eventType string, raw ...string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, len(raw))
	for i, r := range raw {
		id, err := uuid.Parse(r)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid id in %s", ErrPoisonEvent, eventType)
		}
		out[i] = id
	}
	return out, nil
}
