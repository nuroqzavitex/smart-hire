// Command smoke drives the full auth flow (M0 design spec §6.3, plus a check
// that user.registered reaches Kafka) and the job flow (M1a design spec §7)
// through a running gateway, and exits non-zero on the first failing step.
//
// Cookies are sent by hand instead of through a cookiejar so that an old
// refresh token can be replayed to exercise reuse detection.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	refreshCookie = "sh_rt"
	sessionCookie = "sh_session"
	//nolint:gosec // throwaway credential for a freshly generated smoke-test user
	smokePassword = "Smoke-Password-123!"
)

type smoke struct {
	base   string
	origin string
	kafka  string
	client *http.Client
	email  string
	userID string

	recruiterEmail    string
	recruiterPassword string
}

type namedStep struct {
	name string
	fn   func() error
}

type response struct {
	status  int
	body    []byte
	cookies []*http.Cookie
}

func main() {
	base := flag.String("base", "http://localhost:8080", "gateway base URL")
	origin := flag.String("origin", "http://localhost:3001", "allowed Origin sent on cookie routes")
	kafka := flag.String("kafka", "127.0.0.1:19092", "Kafka (Redpanda) dev listener")
	recruiterEmail := flag.String("recruiter-email", "recruiter@smarthire.local", "seeded recruiter email")
	recruiterPassword := flag.String("recruiter-password", "Password123!", "seeded recruiter password")
	flag.Parse()

	s := &smoke{
		base:              strings.TrimRight(*base, "/"),
		origin:            *origin,
		kafka:             *kafka,
		recruiterEmail:    *recruiterEmail,
		recruiterPassword: *recruiterPassword,
		client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		email: "smoke-" + strings.ToLower(ulid.Make().String()) + "@smarthire.local",
	}
	if err := s.run(); err != nil {
		os.Exit(1)
	}
}

// step prints PASS/FAIL for one step and returns its error.
func step(name string, fn func() error) error {
	if err := fn(); err != nil {
		fmt.Printf("FAIL %s: %v\n", name, err)
		return err
	}
	fmt.Printf("PASS %s\n", name)
	return nil
}

func (s *smoke) run() error {
	var access, rt1, rt2 string
	steps := []namedStep{
		{"register", s.register},
		{"user.registered on kafka", func() error {
			return waitForKey(context.Background(), s.kafka, "user.registered", s.userID)
		}},
		{"login", func() (err error) { access, rt1, err = s.login(); return err }},
		{"me", func() error { return s.me(access) }},
		{"refresh", func() (err error) { rt2, err = s.refreshOK(rt1); return err }},
		{"refresh reuse RT1", func() error { return s.refreshDenied(rt1) }},
		{"refresh RT2 after family revoked", func() error { return s.refreshDenied(rt2) }},
		{"login logout refresh", s.loginLogoutRefresh},
	}
	steps = append(steps, s.jobSteps()...)
	for _, st := range steps {
		if err := step(st.name, st.fn); err != nil {
			return err
		}
	}
	return nil
}

func (s *smoke) do(method, path string, body any, header http.Header) (response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return response{}, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, s.base+path, rd)
	if err != nil {
		return response{}, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return response{}, err
	}
	return response{status: resp.StatusCode, body: b, cookies: resp.Cookies()}, nil
}

func expectStatus(r response, want int) error {
	if r.status != want {
		return fmt.Errorf("status %d, want %d: %s", r.status, want, strings.TrimSpace(string(r.body)))
	}
	return nil
}

func findCookie(cs []*http.Cookie, name string) *http.Cookie {
	for _, c := range cs {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// cookieHeader builds the headers for a cookie-authenticated route.
func (s *smoke) cookieHeader(rt string) http.Header {
	h := http.Header{}
	h.Set("Origin", s.origin)
	h.Set("Cookie", refreshCookie+"="+rt)
	return h
}

func (s *smoke) register() error {
	r, err := s.do(http.MethodPost, "/v1/auth/register",
		map[string]string{"email": s.email, "password": smokePassword, "role": "ROLE_CANDIDATE"}, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusCreated); err != nil {
		return err
	}
	var out struct {
		UserID string `json:"user_id"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil || out.UserID == "" {
		return fmt.Errorf("no user_id in body: %s", r.body)
	}
	s.userID = out.UserID
	return nil
}

// login logs the smoke user in and returns the access token and the refresh
// token from the sh_rt cookie.
func (s *smoke) login() (string, string, error) {
	return s.loginAs(s.email, smokePassword)
}

// loginAs returns the access token and the refresh token from the sh_rt cookie.
func (s *smoke) loginAs(email, password string) (string, string, error) {
	r, err := s.do(http.MethodPost, "/v1/auth/login",
		map[string]string{"email": email, "password": password}, nil)
	if err != nil {
		return "", "", err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return "", "", err
	}
	access, err := accessToken(r)
	if err != nil {
		return "", "", err
	}
	rt := findCookie(r.cookies, refreshCookie)
	if rt == nil || rt.Value == "" {
		return "", "", errors.New("no sh_rt cookie set")
	}
	if !rt.HttpOnly || rt.Path != "/v1/auth" {
		return "", "", fmt.Errorf("sh_rt cookie attributes wrong: %s", rt.String())
	}
	if findCookie(r.cookies, sessionCookie) == nil {
		return "", "", errors.New("no sh_session cookie set")
	}
	return access, rt.Value, nil
}

func accessToken(r response) (string, error) {
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil || out.AccessToken == "" || out.ExpiresIn <= 0 {
		return "", fmt.Errorf("no access token in body: %s", r.body)
	}
	return out.AccessToken, nil
}

func (s *smoke) me(access string) error {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+access)
	r, err := s.do(http.MethodGet, "/v1/me", nil, h)
	if err != nil {
		return err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return err
	}
	var out struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	if err := json.Unmarshal(r.body, &out); err != nil {
		return fmt.Errorf("decode body: %w", err)
	}
	if out.Email != s.email || out.Role != "ROLE_CANDIDATE" {
		return fmt.Errorf("got email=%q role=%q, want %q ROLE_CANDIDATE", out.Email, out.Role, s.email)
	}
	return nil
}

// refreshOK rotates rt and returns the new refresh token.
func (s *smoke) refreshOK(rt string) (string, error) {
	r, err := s.do(http.MethodPost, "/v1/auth/refresh", nil, s.cookieHeader(rt))
	if err != nil {
		return "", err
	}
	if err := expectStatus(r, http.StatusOK); err != nil {
		return "", err
	}
	if _, err := accessToken(r); err != nil {
		return "", err
	}
	c := findCookie(r.cookies, refreshCookie)
	if c == nil || c.Value == "" {
		return "", errors.New("no rotated sh_rt cookie")
	}
	if c.Value == rt {
		return "", errors.New("refresh token was not rotated")
	}
	return c.Value, nil
}

func (s *smoke) refreshDenied(rt string) error {
	r, err := s.do(http.MethodPost, "/v1/auth/refresh", nil, s.cookieHeader(rt))
	if err != nil {
		return err
	}
	return expectStatus(r, http.StatusUnauthorized)
}

func (s *smoke) loginLogoutRefresh() error {
	_, rt, err := s.login()
	if err != nil {
		return fmt.Errorf("login: %w", err)
	}
	r, err := s.do(http.MethodPost, "/v1/auth/logout", nil, s.cookieHeader(rt))
	if err != nil {
		return fmt.Errorf("logout: %w", err)
	}
	if err := expectStatus(r, http.StatusNoContent); err != nil {
		return fmt.Errorf("logout: %w", err)
	}
	c := findCookie(r.cookies, refreshCookie)
	if c == nil || c.MaxAge >= 0 || c.Value != "" {
		return errors.New("logout did not clear the sh_rt cookie")
	}
	if err := s.refreshDenied(rt); err != nil {
		return fmt.Errorf("refresh after logout: %w", err)
	}
	return nil
}
