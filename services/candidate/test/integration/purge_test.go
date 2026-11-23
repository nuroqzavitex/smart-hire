//go:build integration

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/pkg/event/eventschema"
	"github.com/nuroqzavitex/smarthire/pkg/event/userdeletion"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

func TestPurge_RemovesFilesProfileAndQuota(t *testing.T) {
	truncate(t)
	ctx, tk := newUpload(t)
	require.Equal(t, http.StatusOK, put(t, tk.URL, "application/pdf", []byte(samplePDF)))
	cv, err := svc.CompleteUpload(ctx, tk.CVID)
	require.NoError(t, err)
	require.NoError(t, svc.HandleParseResult(context.Background(), parsedEvent(t, cv, cv.AttemptID, fullParsedCV(t))))
	user := cv.CandidateID
	_, err = svc.UpsertProfile(ctx, domain.ProfileInput{FullName: "Nguyen Van A"})
	require.NoError(t, err)
	require.NoError(t, svc.ReserveQuota(context.Background(), uuid.New(), user))
	other := uuid.New()
	require.NoError(t, svc.ReserveQuota(context.Background(), uuid.New(), other))

	var before int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE event_type = 'cv.updated' AND aggregate_id = $1`, cv.ID.String()).Scan(&before))
	require.Positive(t, before, "the parse wrote a cv.updated with the parsed CV")

	env, err := userdeletion.NewDeletedEvent(context.Background(), userdeletion.Deleted{
		UserID: user, Role: "candidate", RequestID: uuid.New(), RequestedAt: time.Now(),
	}, time.Now())
	require.NoError(t, err)
	require.NoError(t, svc.HandleUserDeleted(context.Background(), env))
	require.NoError(t, svc.HandleUserDeleted(context.Background(), env), "redelivery")

	assert.Equal(t, "NoSuchKey", miniogo.ToErrorResponse(rootStat(t, cv.ObjectKey)).Code, "file deleted")
	var status, name string
	var parsed []byte
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status, file_name, parsed_data FROM cvs WHERE id = $1`, cv.ID).Scan(&status, &name, &parsed))
	assert.Equal(t, "DELETED", status)
	assert.Empty(t, name)
	assert.Nil(t, parsed)
	for q, want := range map[string]int{
		`SELECT count(*) FROM candidates WHERE user_id = $1`:              0,
		`SELECT count(*) FROM daily_quota WHERE candidate_id = $1`:        0,
		`SELECT count(*) FROM quota_reservations WHERE candidate_id = $1`: 0,
	} {
		var n int
		require.NoError(t, pool.QueryRow(context.Background(), q, user).Scan(&n))
		assert.Equal(t, want, n, q)
	}
	var n int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM daily_quota WHERE candidate_id = $1`, other).Scan(&n))
	assert.Equal(t, 1, n, "other candidates keep their quota")

	for _, typ := range []string{"cv.updated", userdeletion.TopicPurged} {
		var payload []byte
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT payload FROM outbox WHERE event_type = $1 ORDER BY created_at DESC LIMIT 1`, typ).Scan(&payload))
		assert.NoError(t, eventschema.Validate(typ, payload), typ)
	}
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE event_type = $1`, userdeletion.TopicPurged).Scan(&n))
	assert.Equal(t, 1, n)

	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE event_type = 'cv.updated' AND aggregate_id = $1
		 AND payload->'payload'->>'status' <> 'DELETED'`, cv.ID.String()).Scan(&n))
	assert.Zero(t, n, "older cv.updated rows are gone")

	page, err := svc.ListMyCVs(ctx, app.PageRequest{Size: 10})
	require.NoError(t, err)
	assert.Empty(t, page.CVs, "tombstones are not listed")
}
