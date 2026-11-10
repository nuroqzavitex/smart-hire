package domain_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/apperr"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

var now = time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)

func ptr[T any](v T) *T { return &v }

func validDraft() domain.Draft {
	return domain.Draft{
		Title:            "Senior Go Engineer",
		Description:      strings.Repeat("x", 40),
		RequiredSkills:   []string{"Go"},
		NiceToHaveSkills: []string{"Kafka"},
		Level:            domain.LevelSenior,
		Location:         "Hanoi",
		SalaryMin:        ptr(10),
		SalaryMax:        ptr(20),
	}
}

func make31Distinct() []string {
	out := make([]string, 31)
	for i := range out {
		out[i] = fmt.Sprintf("skill%d", i)
	}
	return out
}

func newJob(t *testing.T) domain.Job {
	t.Helper()
	j, err := domain.NewJob(uuid.New(), uuid.New(), validDraft(), now)
	require.NoError(t, err)
	return j
}

func TestNormalizeSkills(t *testing.T) {
	got, err := domain.NormalizeSkills([]string{"  Go ", "go", "Postgre   SQL", "", "GO", "postgre sql"})
	require.NoError(t, err)
	assert.Equal(t, []string{"Go", "Postgre SQL"}, got)
	_, err = domain.NormalizeSkills(make31Distinct())
	assert.ErrorIs(t, err, apperr.ErrValidation)
	_, err = domain.NormalizeSkills([]string{strings.Repeat("a", 51)})
	assert.ErrorIs(t, err, apperr.ErrValidation)
	got, err = domain.NormalizeSkills(nil)
	require.NoError(t, err)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

func TestParseStatusLevel(t *testing.T) {
	s, err := domain.ParseStatus("open")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusOpen, s)
	_, err = domain.ParseStatus("nope")
	assert.ErrorIs(t, err, apperr.ErrValidation)
	l, err := domain.ParseLevel("lead")
	require.NoError(t, err)
	assert.Equal(t, domain.LevelLead, l)
	_, err = domain.ParseLevel("guru")
	assert.ErrorIs(t, err, apperr.ErrValidation)
}

func TestNewJob_Validation(t *testing.T) {
	cases := map[string]func(*domain.Draft){
		"title short": func(d *domain.Draft) { d.Title = "Abcd" },
		"title long":  func(d *domain.Draft) { d.Title = strings.Repeat("á", 201) },
		"desc short":  func(d *domain.Draft) { d.Description = strings.Repeat("x", 19) },
		"desc long":   func(d *domain.Draft) { d.Description = strings.Repeat("x", 20001) },
		"no location": func(d *domain.Draft) { d.Location = " " },
		"long loc":    func(d *domain.Draft) { d.Location = strings.Repeat("x", 101) },
		"salary neg":  func(d *domain.Draft) { d.SalaryMin = ptr(-1) },
		"salary inv":  func(d *domain.Draft) { d.SalaryMin, d.SalaryMax = ptr(10), ptr(5) },
		"bad level":   func(d *domain.Draft) { d.Level = "guru" },
		"many skills": func(d *domain.Draft) { d.RequiredSkills = make31Distinct() },
	}
	for name, mod := range cases {
		d := validDraft()
		mod(&d)
		_, err := domain.NewJob(uuid.New(), uuid.New(), d, now)
		assert.ErrorIs(t, err, apperr.ErrValidation, name)
	}
	j, err := domain.NewJob(uuid.New(), uuid.New(), validDraft(), now)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusDraft, j.Status)
	assert.EqualValues(t, 1, j.Version)
	assert.Equal(t, now, j.CreatedAt)
	assert.Equal(t, now, j.UpdatedAt)
	d := validDraft()
	d.Title = strings.Repeat("á", 200)
	_, err = domain.NewJob(uuid.New(), uuid.New(), d, now)
	assert.NoError(t, err) // upper bound counts runes
	d = validDraft()
	d.Title = "  Go Dev  "
	d.Location = "  Hue "
	j, err = domain.NewJob(uuid.New(), uuid.New(), d, now)
	require.NoError(t, err)
	assert.Equal(t, "Go Dev", j.Title)
	assert.Equal(t, "Hue", j.Location)
}

func TestTransitions(t *testing.T) {
	later := now.Add(time.Hour)
	type action struct {
		name string
		do   func(*domain.Job) error
	}
	actions := []action{
		{"publish", func(j *domain.Job) error { return j.Publish(later) }},
		{"close", func(j *domain.Job) error { return j.Close(later) }},
		{"delete", func(j *domain.Job) error { return j.Delete(later) }},
		{"apply", func(j *domain.Job) error { return j.Apply(domain.Patch{Location: ptr("HCM")}, later) }},
	}
	setup := map[domain.Status]func(*domain.Job){
		domain.StatusDraft:  func(*domain.Job) {},
		domain.StatusOpen:   func(j *domain.Job) { j.Status = domain.StatusOpen },
		domain.StatusClosed: func(j *domain.Job) { j.Status = domain.StatusClosed },
	}
	// from -> action -> expected error (nil = ok)
	want := map[domain.Status]map[string]error{
		domain.StatusDraft:  {"publish": nil, "close": domain.ErrInvalidTransition, "delete": nil, "apply": nil},
		domain.StatusOpen:   {"publish": domain.ErrInvalidTransition, "close": nil, "delete": domain.ErrInvalidTransition, "apply": nil},
		domain.StatusClosed: {"publish": domain.ErrInvalidTransition, "close": domain.ErrInvalidTransition, "delete": nil, "apply": domain.ErrJobClosed},
	}
	for from, setupFn := range setup {
		for _, a := range actions {
			name := string(from) + "/" + a.name
			j := newJob(t)
			setupFn(&j)
			before := j
			err := a.do(&j)
			if exp := want[from][a.name]; exp != nil {
				assert.ErrorIs(t, err, exp, name)
				assert.ErrorIs(t, err, apperr.ErrValidation, name)
				assert.Equal(t, before, j, name)
				continue
			}
			require.NoError(t, err, name)
			assert.Equal(t, before.Version+1, j.Version, name)
			assert.Equal(t, later, j.UpdatedAt, name)
		}
	}
	j := newJob(t)
	require.NoError(t, j.Delete(later))
	require.NotNil(t, j.DeletedAt)
	assert.Equal(t, later, *j.DeletedAt)
	j = newJob(t)
	require.NoError(t, j.Publish(later))
	require.NoError(t, j.Close(later))
	assert.Equal(t, domain.StatusClosed, j.Status)
}

func TestPublish_SetsPublishedAt(t *testing.T) {
	j := newJob(t)
	require.NoError(t, j.Publish(now.Add(time.Minute)))
	assert.Equal(t, domain.StatusOpen, j.Status)
	require.NotNil(t, j.PublishedAt)
	assert.Equal(t, now.Add(time.Minute), *j.PublishedAt)
}

func TestApply_ValidatesMerged(t *testing.T) {
	d := validDraft()
	d.SalaryMin, d.SalaryMax = ptr(10), nil
	j, err := domain.NewJob(uuid.New(), uuid.New(), d, now)
	require.NoError(t, err)
	before := j
	err = j.Apply(domain.Patch{SalaryMax: ptr(1)}, now.Add(time.Hour))
	assert.ErrorIs(t, err, apperr.ErrValidation)
	assert.Equal(t, before, j)
	assert.EqualValues(t, 1, j.Version)

	skills := []string{" Rust ", "rust", "Go"}
	require.NoError(t, j.Apply(domain.Patch{Title: ptr("  New title  "), RequiredSkills: &skills}, now.Add(time.Hour)))
	assert.Equal(t, "New title", j.Title)
	assert.Equal(t, []string{"Rust", "Go"}, j.RequiredSkills)
	assert.EqualValues(t, 2, j.Version)
}

func TestPatchEmpty(t *testing.T) {
	s := "x"
	assert.True(t, domain.Patch{}.Empty())
	assert.False(t, domain.Patch{Title: &s}.Empty())
}

func TestOwnedBy(t *testing.T) {
	j := newJob(t)
	assert.True(t, j.OwnedBy(j.RecruiterID.String()))
	assert.False(t, j.OwnedBy(uuid.NewString()))
	assert.False(t, j.OwnedBy(""))
}
