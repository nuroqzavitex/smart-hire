package eventschema_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event"
	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
)

const testUUID = "0b0f4a52-7a0b-4d0c-9a52-0e7f6b1c2d3e"

var jobPayload = map[string]any{
	"job_id": testUUID, "recruiter_id": testUUID, "title": "Go dev", "description": "build services",
	"location": "Hanoi", "required_skills": []string{"go"}, "nice_to_have_skills": []string{},
	"level": "senior", "salary_min": 1000, "salary_max": nil, "status": "open", "version": 1, "published_at": "2026-11-20T10:00:00Z",
}

func samplePayload(typ string) any {
	switch typ {
	case "user.registered":
		return map[string]any{"user_id": testUUID, "email": "a@x.com", "role": "candidate"}
	case "job.created", "job.updated":
		return jobPayload
	default:
		return map[string]any{"job_id": testUUID, "version": 1}
	}
}

func source(typ string) string {
	if typ == "user.registered" {
		return "auth-service"
	}
	return "job-service"
}

func build(t *testing.T, typ string, payload any) []byte {
	t.Helper()
	e, err := event.New(context.Background(), source(typ), typ, 1, payload, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(e)
	require.NoError(t, err)
	return b
}

func TestValidate_Samples(t *testing.T) {
	for _, typ := range []string{"user.registered", "job.created", "job.updated", "job.closed", "job.deleted"} {
		require.NoError(t, eventschema.Validate(typ, build(t, typ, samplePayload(typ))), typ)
	}
}

func TestValidate_Rejects(t *testing.T) {
	assert.Error(t, eventschema.Validate("job.closed", build(t, "job.closed", map[string]any{"job_id": "x", "version": 1})))
	assert.Error(t, eventschema.Validate("job.closed", build(t, "job.closed", map[string]any{"job_id": testUUID, "version": 1, "extra": 1})))
	assert.Error(t, eventschema.Validate("job.created", build(t, "job.updated", jobPayload)))
	assert.Error(t, eventschema.Validate("nope", []byte(`{}`)))
}

const testULID = "01JC0000000000000000000000"

func buildFrom(t *testing.T, src, typ string, payload any) []byte {
	t.Helper()
	e, err := event.New(context.Background(), src, typ, 1, payload, time.Now())
	require.NoError(t, err)
	b, err := json.Marshal(e)
	require.NoError(t, err)
	return b
}

func buildCV(t *testing.T, typ string, payload any) []byte {
	t.Helper()
	src := "ai-service"
	if typ == "cv.uploaded" {
		src = "candidate-service"
	}
	return buildFrom(t, src, typ, payload)
}

func fixtureDir(t *testing.T) string {
	t.Helper()
	dir, err := eventschema.Dir()
	require.NoError(t, err)
	return filepath.Join(dir, "testdata", "parsed_cv")
}

func glob(t *testing.T, pattern string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(fixtureDir(t), filepath.FromSlash(pattern)))
	require.NoError(t, err)
	require.NotEmpty(t, files, pattern)
	return files
}

func read(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test fixture path from glob
	require.NoError(t, err)
	return b
}

func fixture(t *testing.T, rel string) json.RawMessage {
	t.Helper()
	return read(t, filepath.Join(fixtureDir(t), filepath.FromSlash(rel)))
}

func uploadedPayload(objectKey string) map[string]any {
	return map[string]any{"cv_id": testUUID, "candidate_id": testUUID, "attempt_id": testULID, "object_key": objectKey}
}

func parsedPayload(t *testing.T, parsedFixture, modelVersion string) map[string]any {
	t.Helper()
	return map[string]any{
		"cv_id": testUUID, "candidate_id": testUUID, "attempt_id": testULID,
		"parsed": fixture(t, parsedFixture), "model_version": modelVersion,
	}
}

func parseFailedPayload(reason, detail string) map[string]any {
	return map[string]any{"cv_id": testUUID, "candidate_id": testUUID, "attempt_id": testULID, "reason": reason, "detail": detail}
}

func TestValidate_CVSamples(t *testing.T) {
	key := "cvs/" + testUUID + "/" + testUUID + ".pdf"
	require.NoError(t, eventschema.Validate("cv.uploaded", buildCV(t, "cv.uploaded", uploadedPayload(key))))
	require.NoError(t, eventschema.Validate("cv.parsed", buildCV(t, "cv.parsed", parsedPayload(t, "valid/full.json", "fake:rules-v1"))))
	require.NoError(t, eventschema.Validate("cv.parse_failed", buildCV(t, "cv.parse_failed", parseFailedPayload("no_text", "text shorter than 50 characters"))))
}

func TestValidate_CVRejects(t *testing.T) {
	assert.Error(t, eventschema.Validate("cv.uploaded", buildCV(t, "cv.uploaded", uploadedPayload("cvs/"+testUUID+"/"+testUUID+".exe"))), "bad object key")
	assert.Error(t, eventschema.Validate("cv.uploaded", buildCV(t, "cv.uploaded", uploadedPayload("other/"+testUUID+"/"+testUUID+".pdf"))), "bad prefix")
	noAttempt := uploadedPayload("cvs/" + testUUID + "/" + testUUID + ".pdf")
	delete(noAttempt, "attempt_id")
	assert.Error(t, eventschema.Validate("cv.uploaded", buildCV(t, "cv.uploaded", noAttempt)), "missing attempt_id")
	badAttempt := uploadedPayload("cvs/" + testUUID + "/" + testUUID + ".pdf")
	badAttempt["attempt_id"] = "not-a-ulid"
	assert.Error(t, eventschema.Validate("cv.uploaded", buildCV(t, "cv.uploaded", badAttempt)), "bad attempt_id")

	assert.Error(t, eventschema.Validate("cv.parsed", buildFrom(t, "candidate-service", "cv.parsed", parsedPayload(t, "valid/full.json", "fake:rules-v1"))), "wrong source")
	assert.Error(t, eventschema.Validate("cv.parsed", buildCV(t, "cv.parsed", parsedPayload(t, "valid/full.json", ""))), "empty model_version")
	assert.Error(t, eventschema.Validate("cv.parsed", buildCV(t, "cv.parsed", parsedPayload(t, "valid/full.json", strings.Repeat("m", 101)))), "long model_version")
	assert.Error(t, eventschema.Validate("cv.parsed", buildCV(t, "cv.parsed", parsedPayload(t, "invalid/skills_51.json", "fake:rules-v1"))), "invalid parsed via $ref")
}

func TestValidateDef_ParsedCVFixtures(t *testing.T) {
	valid := glob(t, "valid/*.json")
	assert.Len(t, valid, 4)
	for _, f := range valid {
		assert.NoError(t, eventschema.ValidateDef("cv.parsed", "ParsedCV", read(t, f)), f)
	}
	// Plan decision #2 limits, shared with the Python tests.
	assert.FileExists(t, filepath.Join(fixtureDir(t), "valid", "boundaries.json"))
	for _, name := range []string{"education_11", "languages_21", "full_name_201", "email_255"} {
		assert.FileExists(t, filepath.Join(fixtureDir(t), "invalid", name+".json"))
	}
	invalid := glob(t, "invalid/*.json")
	assert.Len(t, invalid, 14)
	for _, f := range invalid {
		assert.Error(t, eventschema.ValidateDef("cv.parsed", "ParsedCV", read(t, f)), f)
	}
}

func TestValidate_ParseFailedRejectsUnknownReasonAndLongDetail(t *testing.T) {
	assert.Error(t, eventschema.Validate("cv.parse_failed", buildCV(t, "cv.parse_failed", parseFailedPayload("oops", "x"))))
	assert.Error(t, eventschema.Validate("cv.parse_failed", buildCV(t, "cv.parse_failed", parseFailedPayload("no_text", strings.Repeat("x", 201)))))
	assert.NoError(t, eventschema.Validate("cv.parse_failed", buildCV(t, "cv.parse_failed", parseFailedPayload("download_failed", strings.Repeat("x", 200)))))
	for _, r := range []string{"no_text", "not_pdf", "too_large", "llm_refused", "llm_invalid_output", "download_failed"} {
		assert.NoError(t, eventschema.Validate("cv.parse_failed", buildCV(t, "cv.parse_failed", parseFailedPayload(r, ""))), r)
	}
}

func TestValidateDef_RejectsBadDefName(t *testing.T) {
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "../x", []byte(`{}`)))
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "", []byte(`{}`)))
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "NoSuchDef", []byte(`{}`)))
	assert.Error(t, eventschema.ValidateDef("../cv", "ParsedCV", []byte(`{}`)))
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "ParsedCV", []byte(`not json`)))
}

func TestValidateDef_EducationAndExperience(t *testing.T) {
	assert.NoError(t, eventschema.ValidateDef("cv.parsed", "Education", []byte(`{"school":"ĐH Bách Khoa Hà Nội","degree":null,"major":null,"end_year":2020}`)))
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "Education", []byte(`{"school":"x","degree":null,"major":null,"end_year":1899}`)))
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "Education", []byte(`{"school":"x","degree":null,"major":null,"end_year":2020.5}`)))
	assert.NoError(t, eventschema.ValidateDef("cv.parsed", "Experience", []byte(`{"company":"a","title":"b","start":"2020-01","end":null,"highlights":[]}`)))
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "Experience", []byte(`{"company":"a","title":"b","start":"2020-13","end":null,"highlights":[]}`)))
	assert.Error(t, eventschema.ValidateDef("cv.parsed", "Experience", []byte(`{"company":"a","title":"b","start":null,"highlights":[]}`)))
}

// Job events emitted before M2a have no published_at; the job read model
// projector must still accept them (ADR 0009), while a bad value is rejected.
func TestValidate_JobPublishedAtOptional(t *testing.T) {
	legacy := map[string]any{}
	for k, v := range jobPayload {
		if k != "published_at" {
			legacy[k] = v
		}
	}
	for _, typ := range []string{"job.created", "job.updated"} {
		assert.NoError(t, eventschema.Validate(typ, build(t, typ, legacy)), typ)
		bad := map[string]any{"published_at": "yesterday"}
		for k, v := range legacy {
			bad[k] = v
		}
		assert.Error(t, eventschema.Validate(typ, build(t, typ, bad)), typ)
	}
}

func TestValidate_CvUpdated(t *testing.T) {
	parsed := map[string]any{
		"full_name": nil, "email": nil, "skills": []string{"go"}, "years_of_experience": 3.0,
		"current_title": "Backend", "education": []any{}, "experiences": []any{}, "languages": []any{}, "location": nil,
	}
	ok := []map[string]any{
		{"cv_id": testUUID, "candidate_id": testUUID, "version": 3, "status": "PARSED", "parsed": parsed, "parsed_at": "2026-11-20T10:00:00Z"},
		{"cv_id": testUUID, "candidate_id": testUUID, "version": 4, "status": "UPLOADED", "parsed": nil, "parsed_at": nil},
		{"cv_id": testUUID, "candidate_id": testUUID, "version": 2, "status": "FAILED", "parsed": nil, "parsed_at": nil},
	}
	for _, p := range ok {
		assert.NoError(t, eventschema.Validate("cv.updated", buildFrom(t, "candidate-service", "cv.updated", p)))
	}
	bad := []map[string]any{
		{"cv_id": testUUID, "candidate_id": testUUID, "version": 0, "status": "PARSED", "parsed": nil, "parsed_at": nil},
		{"cv_id": testUUID, "candidate_id": testUUID, "version": 1, "status": "UPLOADING", "parsed": nil, "parsed_at": nil},
		{"cv_id": testUUID, "candidate_id": testUUID, "version": 1, "status": "PARSED", "parsed": nil},
	}
	for _, p := range bad {
		assert.Error(t, eventschema.Validate("cv.updated", buildFrom(t, "candidate-service", "cv.updated", p)))
	}
	assert.Error(t, eventschema.Validate("cv.updated", buildFrom(t, "ai-service", "cv.updated", ok[0])), "wrong source")
}

func TestValidate_MatchCompleted(t *testing.T) {
	entry := map[string]any{"candidate_id": testUUID, "cv_id": testUUID, "score": 87.5}
	p := map[string]any{"job_id": testUUID, "model_version": "fake-hash@768", "top": []any{entry}}
	assert.NoError(t, eventschema.Validate("match.completed", buildFrom(t, "ai-service", "match.completed", p)))
	top := make([]any, 21)
	for i := range top {
		top[i] = entry
	}
	assert.Error(t, eventschema.Validate("match.completed", buildFrom(t, "ai-service", "match.completed",
		map[string]any{"job_id": testUUID, "model_version": "m", "top": top})), "more than 20")
	assert.Error(t, eventschema.Validate("match.completed", buildFrom(t, "ai-service", "match.completed",
		map[string]any{"job_id": testUUID, "model_version": "m", "top": []any{map[string]any{"candidate_id": testUUID, "cv_id": testUUID, "score": 101}}})), "score > 100")
}
