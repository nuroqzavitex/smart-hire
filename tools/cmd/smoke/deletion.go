package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// deletionTimeout bounds the account-deletion saga: five services purge and
// Auth collects their confirmations (ADR 0014).
const deletionTimeout = 60 * time.Second

type smokeDeletion struct {
	ID       string   `json:"id"`
	UserID   string   `json:"user_id"`
	Status   string   `json:"status"`
	Expected []string `json:"expected_services"`
	Purged   []string `json:"purged_services"`
}

// deletedUser is a throwaway account created only to be deleted, never one
// of the seeded users.
type deletedUser struct {
	email, id, access, requestID string
}

// registerAs creates an account and returns its id.
func (s *smoke) registerAs(email, role string) (string, error) {
	r, err := s.do(http.MethodPost, "/v1/auth/register",
		map[string]string{"email": email, "password": smokePassword, "role": role}, nil)
	if err != nil {
		return "", err
	}
	if err := expectStatus(r, http.StatusCreated); err != nil {
		return "", err
	}
	var out struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil || out.UserID == "" {
		return "", fmt.Errorf("no user_id in body: %s", r.body)
	}
	return out.UserID, nil
}

// loginStatus returns the status of a login attempt, waiting out the login
// rate limit (10/min per IP) instead of failing on it.
func (s *smoke) loginStatus(email, password string) (int, error) {
	for range 4 {
		r, err := s.do(http.MethodPost, "/v1/auth/login", map[string]string{"email": email, "password": password}, nil)
		if err != nil {
			return 0, err
		}
		if r.status != http.StatusTooManyRequests {
			return r.status, nil
		}
		time.Sleep(20 * time.Second)
	}
	return 0, errors.New("login still rate limited")
}

// freshUser registers a new account of role and logs it in.
func (s *smoke) freshUser(role, tag string) (deletedUser, error) {
	u := deletedUser{email: "smoke-del-" + strings.ToLower(role[5:]) + "-" + tag + "@smarthire.local"}
	var err error
	if u.id, err = s.registerAs(u.email, role); err != nil {
		return u, err
	}
	if u.access, err = s.accessFor(u.email, smokePassword); err != nil {
		return u, err
	}
	return u, nil
}

// deleteMe calls DELETE /v1/me and expects 202 with a PENDING request.
func (s *smoke) deleteMe(u *deletedUser) error {
	r, err := s.do(http.MethodDelete, "/v1/me", map[string]string{"password": smokePassword}, bearer(u.access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusAccepted); err != nil {
		return err
	}
	var out struct {
		ID     string `json:"deletion_request_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil || out.ID == "" {
		return fmt.Errorf("no deletion_request_id in body: %s", r.body)
	}
	if out.Status != "PENDING" {
		return fmt.Errorf("status %q, want PENDING", out.Status)
	}
	u.requestID = out.ID
	return nil
}

// waitDeletionCompleted polls the admin view until the request is COMPLETED
// with every service's confirmation.
func (s *smoke) waitDeletionCompleted(admin string, u deletedUser) error {
	return poll(deletionTimeout, func() error {
		var d smokeDeletion
		if err := s.getJSON("/v1/admin/deletion-requests/"+url.PathEscape(u.requestID), admin, http.StatusOK, &d); err != nil {
			return err
		}
		if d.UserID != u.id {
			return fmt.Errorf("request is for user %s, want %s", d.UserID, u.id)
		}
		if d.Status != "COMPLETED" {
			return fmt.Errorf("status %s, purged %v of %v", d.Status, d.Purged, d.Expected)
		}
		for _, svc := range []string{"candidate", "job", "application", "ai", "notification"} {
			if !slices.Contains(d.Purged, svc) {
				return fmt.Errorf("completed without %s: %v", svc, d.Purged)
			}
		}
		return nil
	})
}

func (s *smoke) deletionSteps() []namedStep {
	tag := strings.ToLower(ulid.Make().String())
	var cand, rec deletedUser
	var admin, jobID, cvID string

	return []namedStep{
		{"deletion: fresh candidate with a cv and an application, fresh recruiter with a job", func() (err error) {
			if cand, err = s.freshUser("ROLE_CANDIDATE", tag); err != nil {
				return err
			}
			if rec, err = s.freshUser("ROLE_RECRUITER", tag); err != nil {
				return err
			}
			if jobID, err = s.openJob(rec.access, "Golang deletion test "+tag); err != nil {
				return err
			}
			cvID, err = s.uploadParsedCV(cand.access, "Deleted Candidate "+tag)
			if err != nil {
				return err
			}
			_, err = s.submitOK(cand.access, "smoke-del-"+tag, jobID, cvID)
			return err
		}},
		{"deletion: wrong password is refused", func() error {
			r, err := s.do(http.MethodDelete, "/v1/me", map[string]string{"password": "not-the-password"}, bearer(cand.access))
			if err != nil {
				return err
			}
			return expectStatus(r, http.StatusUnauthorized)
		}},
		{"deletion: DELETE /v1/me answers 202 for candidate and recruiter", func() error {
			if err := s.deleteMe(&cand); err != nil {
				return err
			}
			return s.deleteMe(&rec)
		}},
		{"deletion: the old access token is denied at once", func() error {
			r, err := s.do(http.MethodGet, "/v1/me", nil, bearer(cand.access))
			if err != nil {
				return err
			}
			return expectStatus(r, http.StatusUnauthorized)
		}},
		{"deletion: login of a pending account is refused", func() error {
			st, err := s.loginStatus(cand.email, smokePassword)
			if err != nil {
				return err
			}
			// 403 while DELETION_PENDING, 401 once the purge already completed.
			if st != http.StatusForbidden && st != http.StatusUnauthorized {
				return fmt.Errorf("login status %d, want 403 or 401", st)
			}
			return nil
		}},
		{"deletion: every service confirms and the accounts are gone", func() (err error) {
			if admin, err = s.accessFor(s.adminEmail, s.adminPassword); err != nil {
				return err
			}
			if err := s.waitDeletionCompleted(admin, cand); err != nil {
				return err
			}
			return s.waitDeletionCompleted(admin, rec)
		}},
		{"deletion: five user.data_purged on kafka", func() error {
			return waitForKeyCount(context.Background(), s.kafka, "user.data_purged", cand.id, 5)
		}},
		{"deletion: the deleted recruiter's job is gone", func() error {
			r, err := s.do(http.MethodGet, "/v1/jobs/"+url.PathEscape(jobID), nil, nil)
			if err != nil {
				return err
			}
			return expectStatus(r, http.StatusNotFound)
		}},
		{"deletion: login now fails as unknown", func() error {
			st, err := s.loginStatus(cand.email, smokePassword)
			if err != nil {
				return err
			}
			if st != http.StatusUnauthorized {
				return fmt.Errorf("login status %d, want 401", st)
			}
			return nil
		}},
	}
}

// uploadParsedCV uploads a small PDF for the candidate and waits until the
// fake LLM parsed it.
func (s *smoke) uploadParsedCV(access, name string) (string, error) {
	pdf := minimalPDF([]string{name, "Backend engineer building services in Golang.", "Skills: Golang, Kafka"})
	cvID, uploadURL, err := s.createUploadURL(access, "deleted.pdf")
	if err != nil {
		return "", err
	}
	if err := s.putObject(uploadURL, pdf, "application/pdf"); err != nil {
		return "", err
	}
	r, err := s.do(http.MethodPost, "/v1/cvs/"+url.PathEscape(cvID)+":complete", map[string]any{}, bearer(access))
	if err != nil {
		return "", err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return "", err
	}
	_, err = s.waitParsed(access, cvID, 60*time.Second)
	return cvID, err
}
