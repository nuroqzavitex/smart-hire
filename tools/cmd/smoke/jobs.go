package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/oklog/ulid/v2"
)

// smokeJob is the subset of a smarthire.job.v1.Job the smoke test reads.
type smokeJob struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Status string `json:"status"`
}

// jobSteps runs the recruiter job flow (M1a design spec §7): create a draft,
// see job.created on Kafka, check the draft is hidden from search, publish,
// find it with an unaccented query, then close it and check it is hidden.
func (s *smoke) jobSteps() []namedStep {
	tag := "smk" + strings.ToLower(ulid.Make().String())
	// Accented title; the search below types it without diacritics.
	title := "Lập trình viên Go " + tag
	query := "lap trinh " + tag
	var access, id string

	return []namedStep{
		{"recruiter login", func() (err error) {
			access, _, err = s.loginAs(s.recruiterEmail, s.recruiterPassword)
			return err
		}},
		{"create job", func() (err error) {
			id, err = s.createJob(access, title)
			return err
		}},
		{"job.created on kafka", func() error {
			return waitForKey(context.Background(), s.kafka, "job.created", id)
		}},
		{"draft hidden", func() error { return s.searchExpect(query, id, false) }},
		{"publish job", func() error { return s.transitionJob(access, id, "publish", "JOB_STATUS_OPEN") }},
		{"search finds job", func() error { return s.searchExpect(query, id, true) }},
		{"close job then hidden", func() error {
			if err := s.transitionJob(access, id, "close", "JOB_STATUS_CLOSED"); err != nil {
				return err
			}
			return s.searchExpect(query, id, false)
		}},
	}
}

func bearer(access string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+access)
	return h
}

func decodeJob(r response) (smokeJob, error) {
	var j smokeJob
	if err := json.Unmarshal(r.body, &j); err != nil {
		return smokeJob{}, fmt.Errorf("decode job: %w", err)
	}
	if j.ID == "" {
		return smokeJob{}, fmt.Errorf("no id in body: %s", r.body)
	}
	return j, nil
}

// createJob creates a draft job and returns its id.
func (s *smoke) createJob(access, title string) (string, error) {
	r, err := s.do(http.MethodPost, "/v1/jobs", map[string]any{
		"title":           title,
		"description":     "Phát triển dịch vụ backend bằng Go và PostgreSQL cho nền tảng tuyển dụng.",
		"required_skills": []string{"Go", "PostgreSQL"},
		"level":           "JOB_LEVEL_SENIOR",
		"location":        "Hà Nội",
	}, bearer(access))
	if err != nil {
		return "", err
	}
	if err := expectStatus(r, http.StatusCreated); err != nil {
		return "", err
	}
	j, err := decodeJob(r)
	if err != nil {
		return "", err
	}
	if j.Status != "JOB_STATUS_DRAFT" {
		return "", fmt.Errorf("status %q, want JOB_STATUS_DRAFT", j.Status)
	}
	if j.Title != title {
		return "", fmt.Errorf("title %q, want %q", j.Title, title)
	}
	return j.ID, nil
}

// transitionJob calls POST /v1/jobs/{id}:<action> and checks the new status.
func (s *smoke) transitionJob(access, id, action, wantStatus string) error {
	if id == "" {
		return errors.New("no job id")
	}
	r, err := s.do(http.MethodPost, "/v1/jobs/"+url.PathEscape(id)+":"+action, map[string]any{}, bearer(access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return err
	}
	j, err := decodeJob(r)
	if err != nil {
		return err
	}
	if j.ID != id || j.Status != wantStatus {
		return fmt.Errorf("got id=%s status=%q, want id=%s %s", j.ID, j.Status, id, wantStatus)
	}
	return nil
}

// searchExpect runs the anonymous public search and checks whether id is in
// the results.
func (s *smoke) searchExpect(q, id string, want bool) error {
	if id == "" {
		return errors.New("no job id")
	}
	r, err := s.do(http.MethodGet, "/v1/jobs?"+url.Values{"q": {q}}.Encode(), nil, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return err
	}
	var out struct {
		Jobs []smokeJob `json:"jobs"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil {
		return fmt.Errorf("decode list: %w", err)
	}
	found := false
	for _, j := range out.Jobs {
		if j.ID == id {
			found = true
			break
		}
	}
	if found != want {
		return fmt.Errorf("job %s in results = %v, want %v (%d results)", id, found, want, len(out.Jobs))
	}
	return nil
}
