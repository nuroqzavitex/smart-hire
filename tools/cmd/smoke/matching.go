package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const matchTimeout = 45 * time.Second

type matchBreakdown struct {
	MatchedSkills []string `json:"matched_skills"`
	MissingSkills []string `json:"missing_skills"`
}

type matchedCandidates struct {
	Candidates []struct {
		CandidateID string         `json:"candidate_id"`
		CVID        string         `json:"cv_id"`
		Score       float64        `json:"score"`
		Breakdown   matchBreakdown `json:"breakdown"`
	} `json:"candidates"`
	ModelVersion string `json:"model_version"`
}

type recommendedJobs struct {
	Jobs []struct {
		JobID     string         `json:"job_id"`
		Title     string         `json:"title"`
		Score     float64        `json:"score"`
		Breakdown matchBreakdown `json:"breakdown"`
	} `json:"jobs"`
}

// matchingSteps covers M2b (ADR 0010): a published job and a parsed CV with the
// same skills meet in the AI matcher; the recruiter sees the candidate ranked
// for the job, the candidate sees the job recommended for the CV, and other
// users see neither.
func (s *smoke) matchingSteps() []namedStep {
	tag := "smk" + strings.ToLower(ulid.Make().String())
	pdf := minimalPDF([]string{
		"Matching Candidate " + tag,
		"Backend engineer building payment services in Golang.",
		"Skills: Golang, Kafka, PostgreSQL",
	})
	var recruiter, candidate, other, jobID, cvID, uploadURL string

	return []namedStep{
		{"matching: logins", func() (err error) {
			if recruiter, err = s.accessFor(s.recruiterEmail, s.recruiterPassword); err != nil {
				return err
			}
			if candidate, err = s.accessFor(s.candidateEmail, s.candidatePassword); err != nil {
				return err
			}
			other, err = s.accessFor(s.email, smokePassword) // the freshly registered smoke candidate
			return err
		}},
		{"matching: publish job", func() (err error) {
			jobID, err = s.createJobWith(recruiter, "Golang backend engineer "+tag,
				"Build payment services in Golang with Kafka and PostgreSQL.", []string{"go", "kafka", "postgresql"})
			if err != nil {
				return err
			}
			return s.transitionJob(recruiter, jobID, "publish", "JOB_STATUS_OPEN")
		}},
		{"matching: upload and parse cv", func() (err error) {
			if cvID, uploadURL, err = s.createUploadURL(candidate, "match.pdf"); err != nil {
				return err
			}
			if err = s.putObject(uploadURL, pdf, "application/pdf"); err != nil {
				return err
			}
			r, err := s.do(http.MethodPost, "/v1/cvs/"+url.PathEscape(cvID)+":complete", map[string]any{}, bearer(candidate))
			if err != nil {
				return err
			}
			if err := expectStatus(r, http.StatusOK); err != nil {
				return err
			}
			_, err = s.waitParsed(candidate, cvID, 60*time.Second)
			return err
		}},
		{"matching: recruiter sees candidate", func() error {
			return poll(matchTimeout, func() error {
				var out matchedCandidates
				if err := s.getJSON("/v1/jobs/"+url.PathEscape(jobID)+"/candidates", recruiter, http.StatusOK, &out); err != nil {
					return err
				}
				for _, c := range out.Candidates {
					if c.CVID == cvID {
						if c.Score <= 40 || len(c.Breakdown.MatchedSkills) != 3 {
							return fmt.Errorf("weak match: score %.2f, matched %v", c.Score, c.Breakdown.MatchedSkills)
						}
						return nil
					}
				}
				return fmt.Errorf("cv %s not among %d candidates", cvID, len(out.Candidates))
			})
		}},
		{"matching: candidate sees job", func() error {
			return poll(matchTimeout, func() error {
				var out recommendedJobs
				if err := s.getJSON("/v1/cvs/"+url.PathEscape(cvID)+"/recommended-jobs", candidate, http.StatusOK, &out); err != nil {
					return err
				}
				for _, j := range out.Jobs {
					if j.JobID == jobID {
						if !strings.Contains(j.Title, tag) || j.Score <= 40 {
							return fmt.Errorf("unexpected job %q score %.2f", j.Title, j.Score)
						}
						return nil
					}
				}
				return fmt.Errorf("job %s not among %d recommendations", jobID, len(out.Jobs))
			})
		}},
		{"matching: others denied", func() error {
			if err := s.getJSON("/v1/jobs/"+url.PathEscape(jobID)+"/candidates", other, http.StatusForbidden, nil); err != nil {
				return fmt.Errorf("candidate on job candidates: %w", err)
			}
			if err := s.getJSON("/v1/cvs/"+url.PathEscape(cvID)+"/recommended-jobs", other, http.StatusNotFound, nil); err != nil {
				return fmt.Errorf("other candidate on recommendations: %w", err)
			}
			return nil
		}},
	}
}

// getJSON GETs path with the access token, checks the status and decodes the
// body into out (when not nil).
func (s *smoke) getJSON(path, access string, want int, out any) error {
	r, err := s.do(http.MethodGet, path, nil, bearer(access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, want); err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(r.body, out); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

// poll retries fn every 500ms until it succeeds or timeout passes.
func poll(timeout time.Duration, fn func() error) error {
	deadline := time.Now().Add(timeout)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("after %s: %w", timeout, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
