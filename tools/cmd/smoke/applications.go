package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// The saga's SCORE step waits up to 2 minutes for the AI replicas of a fresh
// CV and job (ADR 0011), so screening can take a while on a cold stack.
const screenTimeout = 150 * time.Second

type smokeApplication struct {
	ID     string   `json:"id"`
	JobID  string   `json:"job_id"`
	Status string   `json:"status"`
	Reason string   `json:"reason"`
	Score  *float64 `json:"score"`
}

type smokeApplicationDetail struct {
	Application smokeApplication `json:"application"`
	History     []struct {
		From  string `json:"from"`
		To    string `json:"to"`
		Actor string `json:"actor"`
	} `json:"history"`
}

// applicationFixture is a published job of the seeded recruiter and a parsed
// CV of the seeded candidate.
type applicationFixture struct {
	recruiter, candidate, other string
	cvID                        string
}

func (s *smoke) applicationFixture(tag string) (applicationFixture, error) {
	var f applicationFixture
	var err error
	if f.recruiter, _, err = s.loginAs(s.recruiterEmail, s.recruiterPassword); err != nil {
		return f, err
	}
	if f.candidate, _, err = s.loginAs(s.candidateEmail, s.candidatePassword); err != nil {
		return f, err
	}
	if f.other, _, err = s.login(); err != nil {
		return f, err
	}
	pdf := minimalPDF([]string{
		"Application Candidate " + tag,
		"Backend engineer building payment services in Golang.",
		"Skills: Golang, Kafka, PostgreSQL",
	})
	cvID, uploadURL, err := s.createUploadURL(f.candidate, "apply.pdf")
	if err != nil {
		return f, err
	}
	if err := s.putObject(uploadURL, pdf, "application/pdf"); err != nil {
		return f, err
	}
	r, err := s.do(http.MethodPost, "/v1/cvs/"+url.PathEscape(cvID)+":complete", map[string]any{}, bearer(f.candidate))
	if err != nil {
		return f, err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return f, err
	}
	if _, err := s.waitParsed(f.candidate, cvID, 60*time.Second); err != nil {
		return f, err
	}
	f.cvID = cvID
	return f, nil
}

func (s *smoke) openJob(recruiter, title string) (string, error) {
	id, err := s.createJobWith(recruiter, title,
		"Build payment services in Golang with Kafka and PostgreSQL.", []string{"go", "kafka", "postgresql"})
	if err != nil {
		return "", err
	}
	return id, s.transitionJob(recruiter, id, "publish", "JOB_STATUS_OPEN")
}

// submitApplication POSTs /v1/applications with the Idempotency-Key header.
func (s *smoke) submitApplication(access, key, jobID, cvID string) (response, error) {
	h := bearer(access)
	h.Set("Idempotency-Key", key)
	return s.do(http.MethodPost, "/v1/applications", map[string]any{"job_id": jobID, "cv_id": cvID}, h)
}

func (s *smoke) submitOK(access, key, jobID, cvID string) (smokeApplication, error) {
	r, err := s.submitApplication(access, key, jobID, cvID)
	if err != nil {
		return smokeApplication{}, err
	}
	if err := expectStatus(r, http.StatusAccepted); err != nil {
		return smokeApplication{}, err
	}
	var a smokeApplication
	if err := json.Unmarshal(r.body, &a); err != nil {
		return a, fmt.Errorf("decode application: %w", err)
	}
	return a, nil
}

// waitApplication polls the application until done accepts it.
func (s *smoke) waitApplication(access, id string, timeout time.Duration, done func(smokeApplicationDetail) error) (smokeApplicationDetail, error) {
	var d smokeApplicationDetail
	err := poll(timeout, func() error {
		d = smokeApplicationDetail{}
		if err := s.getJSON("/v1/applications/"+url.PathEscape(id), access, http.StatusOK, &d); err != nil {
			return err
		}
		return done(d)
	})
	return d, err
}

func statusIs(want string) func(smokeApplicationDetail) error {
	return func(d smokeApplicationDetail) error {
		if d.Application.Status != want {
			return fmt.Errorf("status %s (reason %q), want %s", d.Application.Status, d.Application.Reason, want)
		}
		return nil
	}
}

func (s *smoke) patchStatus(access, id, status string, want int) error {
	r, err := s.do(http.MethodPatch, "/v1/applications/"+url.PathEscape(id)+"/status",
		map[string]any{"status": status}, bearer(access))
	if err != nil {
		return err
	}
	return expectStatus(r, want)
}

// applicationSteps covers M3 (ADR 0011): submit returns 202 and the saga
// reserves quota and scores in the background; the idempotency key makes
// retries safe; the recruiter moves the application; others are denied.
func (s *smoke) applicationSteps() []namedStep {
	tag := "app" + strings.ToLower(ulid.Make().String())
	key := "smoke-" + tag
	var f applicationFixture
	var jobID string
	var app smokeApplication

	return []namedStep{
		{"applications: job and parsed cv", func() (err error) {
			if f, err = s.applicationFixture(tag); err != nil {
				return err
			}
			jobID, err = s.openJob(f.recruiter, "Golang payments engineer "+tag)
			return err
		}},
		{"applications: submit returns 202 pending", func() (err error) {
			app, err = s.submitOK(f.candidate, key, jobID, f.cvID)
			if err == nil && app.Status != "APPLICATION_STATUS_PENDING" {
				err = fmt.Errorf("status %s, want PENDING", app.Status)
			}
			return err
		}},
		{"applications: same idempotency key returns the same application", func() error {
			again, err := s.submitOK(f.candidate, key, jobID, f.cvID)
			if err != nil {
				return err
			}
			if again.ID != app.ID {
				return fmt.Errorf("replay created %s, want %s", again.ID, app.ID)
			}
			return nil
		}},
		{"applications: second application to the same job conflicts", func() error {
			r, err := s.submitApplication(f.candidate, key+"-2", jobID, f.cvID)
			if err != nil {
				return err
			}
			return expectStatus(r, http.StatusConflict)
		}},
		{"applications: saga screens with a score", func() error {
			d, err := s.waitApplication(f.candidate, app.ID, screenTimeout, statusIs("APPLICATION_STATUS_SCREENED"))
			if err != nil {
				return err
			}
			if d.Application.Score == nil || *d.Application.Score <= 0 {
				return fmt.Errorf("screened without a score: %+v", d.Application)
			}
			var path []string
			for _, h := range d.History {
				path = append(path, strings.TrimPrefix(h.To, "APPLICATION_STATUS_"))
			}
			if !slices.Equal(path, []string{"PENDING", "SCREENING", "SCREENED"}) {
				return fmt.Errorf("history %v", path)
			}
			return nil
		}},
		{"applications: recruiter lists and moves", func() error {
			var list struct {
				Applications []smokeApplication `json:"applications"`
			}
			if err := s.getJSON("/v1/applications?job_id="+url.QueryEscape(jobID), f.recruiter, http.StatusOK, &list); err != nil {
				return err
			}
			if len(list.Applications) != 1 || list.Applications[0].ID != app.ID {
				return fmt.Errorf("recruiter list %+v", list.Applications)
			}
			if err := s.patchStatus(f.recruiter, app.ID, "APPLICATION_STATUS_INTERVIEW", http.StatusOK); err != nil {
				return err
			}
			if err := s.patchStatus(f.recruiter, app.ID, "APPLICATION_STATUS_OFFERED", http.StatusOK); err != nil {
				return err
			}
			if err := s.patchStatus(f.recruiter, app.ID, "APPLICATION_STATUS_INTERVIEW", http.StatusConflict); err != nil {
				return fmt.Errorf("backwards move: %w", err)
			}
			return nil
		}},
		{"applications: others denied", func() error {
			if err := s.getJSON("/v1/applications/"+url.PathEscape(app.ID), f.other, http.StatusNotFound, nil); err != nil {
				return fmt.Errorf("other candidate: %w", err)
			}
			if err := s.patchStatus(f.candidate, app.ID, "APPLICATION_STATUS_HIRED", http.StatusForbidden); err != nil {
				return fmt.Errorf("candidate moving own application: %w", err)
			}
			return nil
		}},
		{"application.status_changed on kafka", func() error {
			return waitForKey(context.Background(), s.kafka, "application.status_changed", app.ID)
		}},
	}
}

// chaosAISteps expects the ai-api container to be stopped (make chaos-ai):
// every application must still leave the saga, as NEEDS_MANUAL_REVIEW with
// reason ai_unavailable, instead of hanging or being cancelled.
func (s *smoke) chaosAISteps() []namedStep {
	const n = 6
	tag := "chaos" + strings.ToLower(ulid.Make().String())
	var f applicationFixture
	ids := make([]string, 0, n)
	return []namedStep{
		{"chaos: parsed cv and open jobs", func() (err error) {
			f, err = s.applicationFixture(tag)
			return err
		}},
		{"chaos: submit applications while the AI is down", func() error {
			for i := range n {
				jobID, err := s.openJob(f.recruiter, fmt.Sprintf("Chaos job %d %s", i, tag))
				if err != nil {
					return err
				}
				a, err := s.submitOK(f.candidate, fmt.Sprintf("%s-%d", tag, i), jobID, f.cvID)
				if err != nil {
					return err
				}
				ids = append(ids, a.ID)
			}
			return nil
		}},
		{"chaos: every application falls back to manual review", func() error {
			for _, id := range ids {
				d, err := s.waitApplication(f.candidate, id, 60*time.Second, statusIs("APPLICATION_STATUS_NEEDS_MANUAL_REVIEW"))
				if err != nil {
					return fmt.Errorf("application %s: %w", id, err)
				}
				if d.Application.Reason != "ai_unavailable" {
					return fmt.Errorf("application %s reason %q", id, d.Application.Reason)
				}
			}
			return nil
		}},
	}
}
