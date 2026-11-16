package main

import (
	"bytes"
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

// smokeCV is the subset of a smarthire.candidate.v1.CV the smoke test reads.
// protojson renders int64 as a string; json.Number accepts both.
type smokeCV struct {
	ID         string         `json:"id"`
	Status     string         `json:"status"`
	ParseError string         `json:"parse_error"`
	Version    json.Number    `json:"version"`
	Parsed     map[string]any `json:"parsed"`
}

func (c smokeCV) skills() []string {
	var out []string
	if raw, ok := c.Parsed["skills"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}

// cvSteps runs the candidate CV flow (M1b design spec): profile, presigned
// upload straight to MinIO, complete, cv.uploaded on Kafka, wait for the AI
// service to parse, edit the parsed data, and reject a non-PDF upload.
func (s *smoke) cvSteps() []namedStep {
	tag := "smk" + strings.ToLower(ulid.Make().String())
	pdf := minimalPDF([]string{
		"Smoke Candidate " + tag,
		tag + "@smarthire.local",
		"Backend engineer building distributed services.",
		"Skills: Golang, k8s, PostgreSQL",
	})
	var access, id, uploadURL string
	var parsed smokeCV

	return []namedStep{
		{"candidate login", func() (err error) {
			access, _, err = s.loginAs(s.candidateEmail, s.candidatePassword)
			return err
		}},
		{"profile upsert and get", func() error { return s.profileRoundTrip(access) }},
		{"create upload url", func() (err error) {
			id, uploadURL, err = s.createUploadURL(access, "cv.pdf")
			return err
		}},
		{"put pdf", func() error { return s.putObject(uploadURL, pdf, "application/pdf") }},
		{"complete upload", func() error {
			r, err := s.do(http.MethodPost, "/v1/cvs/"+url.PathEscape(id)+":complete", map[string]any{}, bearer(access))
			if err != nil {
				return err
			}
			if err := expectStatus(r, http.StatusOK); err != nil {
				return err
			}
			if bytes.Contains(r.body, []byte("object_key")) {
				return errors.New("response leaks object_key")
			}
			cv, err := decodeCV(r)
			if err != nil {
				return err
			}
			if cv.Status != "CV_STATUS_UPLOADED" {
				return fmt.Errorf("status %q, want CV_STATUS_UPLOADED", cv.Status)
			}
			return nil
		}},
		{"cv.uploaded on kafka", func() error {
			return waitForKey(context.Background(), s.kafka, "cv.uploaded", id)
		}},
		{"cv parsed", func() (err error) {
			parsed, err = s.waitParsed(access, id, 60*time.Second)
			return err
		}},
		{"skills extracted", func() error {
			got := parsed.skills()
			for _, want := range []string{"Go", "Kubernetes", "PostgreSQL"} {
				if !slices.Contains(got, want) {
					return fmt.Errorf("skills %v lack %q", got, want)
				}
			}
			return nil
		}},
		{"patch parsed", func() error { return s.patchParsed(access, id, parsed) }},
		{"non-pdf rejected", func() error { return s.nonPDFRejected(access) }},
	}
}

func decodeCV(r response) (smokeCV, error) {
	var c smokeCV
	if err := json.Unmarshal(r.body, &c); err != nil {
		return smokeCV{}, fmt.Errorf("decode cv: %w", err)
	}
	if c.ID == "" {
		return smokeCV{}, fmt.Errorf("no id in body: %s", r.body)
	}
	return c, nil
}

func (s *smoke) profileRoundTrip(access string) error {
	r, err := s.do(http.MethodPut, "/v1/me/profile",
		map[string]string{"full_name": "Smoke Candidate", "location": "Hà Nội"}, bearer(access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return err
	}
	r, err = s.do(http.MethodGet, "/v1/me/profile", nil, bearer(access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return err
	}
	var p struct {
		FullName string `json:"full_name"`
	}
	if err := json.Unmarshal(r.body, &p); err != nil {
		return fmt.Errorf("decode profile: %w", err)
	}
	if p.FullName != "Smoke Candidate" {
		return fmt.Errorf("full_name %q, want %q", p.FullName, "Smoke Candidate")
	}
	return nil
}

// createUploadURL returns the new cv id and its presigned PUT URL.
func (s *smoke) createUploadURL(access, fileName string) (string, string, error) {
	r, err := s.do(http.MethodPost, "/v1/cvs/upload-url",
		map[string]any{"consent": true, "file_name": fileName}, bearer(access))
	if err != nil {
		return "", "", err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return "", "", err
	}
	var out struct {
		CVID      string `json:"cv_id"`
		UploadURL string `json:"upload_url"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil {
		return "", "", fmt.Errorf("decode body: %w", err)
	}
	if out.CVID == "" || out.ExpiresAt == "" || !strings.HasPrefix(out.UploadURL, "http://127.0.0.1:9000/") {
		return "", "", errors.New("upload-url response missing cv_id/expires_at or upload_url has wrong host")
	}
	return out.CVID, out.UploadURL, nil
}

// putObject uploads body straight to the presigned URL (MinIO) and expects 200.
func (s *smoke) putObject(url string, body []byte, contentType string) error {
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return errors.New("build put request")
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := s.client.Do(req)
	if err != nil {
		// Not wrapped: the error embeds the presigned URL.
		return errors.New("put object failed (connection error)")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("put object: status %d, want 200", resp.StatusCode)
	}
	return nil
}

func (s *smoke) getCV(access, id string) (smokeCV, error) {
	r, err := s.do(http.MethodGet, "/v1/cvs/"+url.PathEscape(id), nil, bearer(access))
	if err != nil {
		return smokeCV{}, err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return smokeCV{}, err
	}
	return decodeCV(r)
}

func (s *smoke) waitParsed(access, id string, timeout time.Duration) (smokeCV, error) {
	deadline := time.Now().Add(timeout)
	for {
		cv, err := s.getCV(access, id)
		if err != nil {
			return smokeCV{}, err
		}
		switch cv.Status {
		case "CV_STATUS_PARSED":
			return cv, nil
		case "CV_STATUS_FAILED":
			return smokeCV{}, fmt.Errorf("cv parse failed: %s", cv.ParseError)
		}
		if time.Now().After(deadline) {
			return smokeCV{}, fmt.Errorf("cv still %s after %s", cv.Status, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *smoke) patchParsed(access, id string, cur smokeCV) error {
	before, err := cur.Version.Int64()
	if err != nil {
		return fmt.Errorf("version %q: %w", cur.Version, err)
	}
	edited := make(map[string]any, len(cur.Parsed)+1)
	for k, v := range cur.Parsed {
		edited[k] = v
	}
	edited["skills"] = append(cur.skills(), "Rust")
	r, err := s.do(http.MethodPatch, "/v1/cvs/"+url.PathEscape(id)+"/parsed", edited, bearer(access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return err
	}
	got, err := decodeCV(r)
	if err != nil {
		return err
	}
	after, err := got.Version.Int64()
	if err != nil || after != before+1 {
		return fmt.Errorf("version %s, want %d", got.Version, before+1)
	}
	again, err := s.getCV(access, id)
	if err != nil {
		return err
	}
	if !slices.Contains(again.skills(), "Rust") {
		return fmt.Errorf("skills %v lack Rust after patch", again.skills())
	}
	return nil
}

func (s *smoke) nonPDFRejected(access string) error {
	id, uploadURL, err := s.createUploadURL(access, "fake.pdf")
	if err != nil {
		return err
	}
	if err := s.putObject(uploadURL, []byte("<html>not a pdf</html>"), "application/pdf"); err != nil {
		return err
	}
	r, err := s.do(http.MethodPost, "/v1/cvs/"+url.PathEscape(id)+":complete", map[string]any{}, bearer(access))
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusBadRequest); err != nil {
		return err
	}
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(r.body, &env); err != nil || env.Error.Code != "INVALID_ARGUMENT" {
		return fmt.Errorf("error.code %q, want INVALID_ARGUMENT: %s", env.Error.Code, r.body)
	}
	return nil
}
