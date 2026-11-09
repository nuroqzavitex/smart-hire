package eventschema_test

import (
	"context"
	"encoding/json"
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
	"level": "senior", "salary_min": 1000, "salary_max": nil, "status": "open", "version": 1,
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
