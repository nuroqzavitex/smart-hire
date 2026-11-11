package grpc_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"

	candidatev1 "github.com/nuroqzavitex/smarthire/gen/go/smarthire/candidate/v1"
	"github.com/nuroqzavitex/smarthire/pkg/authz"
	"github.com/nuroqzavitex/smarthire/pkg/authz/authztest"
	"github.com/nuroqzavitex/smarthire/pkg/grpcx"
	candgrpc "github.com/nuroqzavitex/smarthire/services/candidate/internal/adapter/grpc"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/app"
	"github.com/nuroqzavitex/smarthire/services/candidate/internal/domain"
)

// fakeUC records the arguments of the last call and returns canned results.
type fakeUC struct {
	cand     domain.Candidate
	cv       domain.CV
	page     app.Page
	ticket   app.UploadTicket
	err      error
	calls    int
	profile  domain.ProfileInput
	consent  bool
	fileName string
	id       uuid.UUID
	pageReq  app.PageRequest
	parsed   domain.ParsedCV
}

func (f *fakeUC) GetProfile(context.Context) (domain.Candidate, error) {
	f.calls++
	return f.cand, f.err
}

func (f *fakeUC) UpsertProfile(_ context.Context, in domain.ProfileInput) (domain.Candidate, error) {
	f.calls++
	f.profile = in
	return f.cand, f.err
}

func (f *fakeUC) CreateUploadURL(_ context.Context, consent bool, fileName string) (app.UploadTicket, error) {
	f.calls++
	f.consent, f.fileName = consent, fileName
	return f.ticket, f.err
}

func (f *fakeUC) CompleteUpload(_ context.Context, id uuid.UUID) (domain.CV, error) {
	f.calls++
	f.id = id
	return f.cv, f.err
}

func (f *fakeUC) GetCV(_ context.Context, id uuid.UUID) (domain.CV, error) {
	f.calls++
	f.id = id
	return f.cv, f.err
}

func (f *fakeUC) ListMyCVs(_ context.Context, pr app.PageRequest) (app.Page, error) {
	f.calls++
	f.pageReq = pr
	return f.page, f.err
}

func (f *fakeUC) UpdateParsedData(_ context.Context, id uuid.UUID, p domain.ParsedCV) (domain.CV, error) {
	f.calls++
	f.id, f.parsed = id, p
	return f.cv, f.err
}

func (f *fakeUC) ReparseCV(_ context.Context, id uuid.UUID) (domain.CV, error) {
	f.calls++
	f.id = id
	return f.cv, f.err
}

type env struct {
	client candidatev1.CandidateServiceClient
	issuer *authztest.KeyIssuer
	uc     *fakeUC
}

func newEnv(t *testing.T) *env {
	t.Helper()
	is := authztest.NewIssuer(t)
	v, err := authz.NewVerifier(t.Context(), is.JWKSURL(), authztest.Issuer, authztest.Audience)
	require.NoError(t, err)
	t.Cleanup(v.Close)

	uc := &fakeUC{}
	srv, _ := grpcx.NewServer(grpcx.ServerConfig{
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Verifier:      v,
		PublicMethods: append([]string{grpc_health_v1.Health_Check_FullMethodName}, candgrpc.PublicMethods...),
	})
	candidatev1.RegisterCandidateServiceServer(srv, candgrpc.NewHandler(uc))
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpcx.Dial("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return &env{client: candidatev1.NewCandidateServiceClient(conn), issuer: is, uc: uc}
}

func (e *env) candidateCtx(t *testing.T) context.Context {
	t.Helper()
	tok := e.issuer.Sign(t, uuid.NewString(), authz.RoleCandidate, time.Minute)
	return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+tok)
}

func code(err error) codes.Code { return status.Code(err) }

func ptr[T any](v T) *T { return &v }

var t0 = time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC)

// sampleCV is a PARSED CV whose parsed data is the canonical form of p.
func sampleCV(t *testing.T, st domain.Status, p *domain.ParsedCV) domain.CV {
	t.Helper()
	cand, id := uuid.New(), uuid.New()
	cv := domain.CV{
		ID: id, CandidateID: cand, ObjectKey: domain.ObjectKey(cand, id), FileName: "cv.pdf",
		Status: st, AttemptID: "01ATTEMPT", SizeBytes: ptr(int64(2048)), ConsentAt: t0,
		Version: 3, CreatedAt: t0, UpdatedAt: t0.Add(time.Minute),
	}
	if p != nil {
		raw, err := p.MarshalCanonical()
		require.NoError(t, err)
		cv.ParsedData = raw
		cv.ParsedAt = ptr(t0.Add(time.Minute))
	}
	return cv
}

func TestHandler_RequiresToken(t *testing.T) {
	e := newEnv(t)
	assert.Empty(t, candgrpc.PublicMethods)
	ctx := context.Background()
	_, err := e.client.GetMyProfile(ctx, &candidatev1.GetMyProfileRequest{})
	assert.Equal(t, codes.Unauthenticated, code(err))
	_, err = e.client.ListMyCVs(ctx, &candidatev1.ListMyCVsRequest{})
	assert.Equal(t, codes.Unauthenticated, code(err))
	assert.Zero(t, e.uc.calls, "use cases must not run without a token")
}

func TestHandler_ConsentValidated(t *testing.T) {
	e := newEnv(t)
	_, err := e.client.CreateUploadURL(e.candidateCtx(t),
		&candidatev1.CreateUploadURLRequest{Consent: false, FileName: "cv.pdf"})
	assert.Equal(t, codes.InvalidArgument, code(err))
	assert.Zero(t, e.uc.calls, "use case must not run on an invalid request")

	cvID := uuid.New()
	e.uc.ticket = app.UploadTicket{CVID: cvID, URL: "http://127.0.0.1:9000/cvs/x?sig", ExpiresAt: t0}
	got, err := e.client.CreateUploadURL(e.candidateCtx(t),
		&candidatev1.CreateUploadURLRequest{Consent: true, FileName: "my cv.pdf"})
	require.NoError(t, err)
	assert.True(t, e.uc.consent)
	assert.Equal(t, "my cv.pdf", e.uc.fileName)
	assert.Equal(t, cvID.String(), got.GetCvId())
	assert.Equal(t, "http://127.0.0.1:9000/cvs/x?sig", got.GetUploadUrl())
	assert.Equal(t, t0, got.GetExpiresAt().AsTime())
}

func TestHandler_InvalidIDRejected(t *testing.T) {
	e := newEnv(t)
	_, err := e.client.GetCV(e.candidateCtx(t), &candidatev1.GetCVRequest{Id: "not-a-uuid"})
	assert.Equal(t, codes.InvalidArgument, code(err))
	assert.Zero(t, e.uc.calls)
}

func TestHandler_GetCVNeverExposesObjectKey(t *testing.T) {
	e := newEnv(t)
	e.uc.cv = sampleCV(t, domain.StatusParsed, &domain.ParsedCV{FullName: ptr("An"), Skills: []string{"Go"}})
	got, err := e.client.GetCV(e.candidateCtx(t), &candidatev1.GetCVRequest{Id: e.uc.cv.ID.String()})
	require.NoError(t, err)
	assert.Equal(t, e.uc.cv.ID, e.uc.id)

	b, err := protojson.Marshal(got)
	require.NoError(t, err)
	assert.NotContains(t, string(b), "cvs/")
	assert.NotContains(t, string(b), "object_key")
	assert.NotContains(t, string(b), "objectKey")

	assert.Equal(t, e.uc.cv.ID.String(), got.GetId())
	assert.Equal(t, "cv.pdf", got.GetFileName())
	assert.Equal(t, int64(2048), got.GetSizeBytes())
	assert.Equal(t, int64(3), got.GetVersion())
	assert.Equal(t, t0, got.GetCreatedAt().AsTime())
	assert.Equal(t, t0.Add(time.Minute), got.GetUpdatedAt().AsTime())
	assert.Equal(t, t0.Add(time.Minute), got.GetParsedAt().AsTime())
	assert.Equal(t, "An", got.GetParsed().GetFullName())
	assert.Equal(t, []string{"Go"}, got.GetParsed().GetSkills())
	assert.Empty(t, got.GetParseError())
}

func TestHandler_CVWithoutParsedData(t *testing.T) {
	e := newEnv(t)
	cv := sampleCV(t, domain.StatusFailed, nil)
	cv.ParseError = ptr("no_text")
	cv.SizeBytes = nil
	e.uc.cv = cv
	got, err := e.client.CompleteUpload(e.candidateCtx(t), &candidatev1.CompleteUploadRequest{Id: cv.ID.String()})
	require.NoError(t, err)
	assert.Equal(t, cv.ID, e.uc.id)
	assert.Nil(t, got.GetParsed())
	assert.Nil(t, got.GetParsedAt())
	assert.Nil(t, got.SizeBytes)
	assert.Equal(t, "no_text", got.GetParseError())
}

func TestHandler_ParsedRoundTrip(t *testing.T) {
	e := newEnv(t)
	id := uuid.New()
	req := &candidatev1.UpdateParsedDataRequest{
		Id: id.String(),
		Parsed: &candidatev1.ParsedCV{
			FullName: ptr("An"),
			Skills:   []string{"Go", "SQL"},
			Education: []*candidatev1.Education{
				{School: "HUST", EndYear: ptr(int32(2020))},
			},
			Experiences: []*candidatev1.Experience{
				{Company: "Acme", Title: "Engineer", Start: ptr("2020-01")},
			},
		},
	}
	want := domain.ParsedCV{
		FullName:    ptr("An"),
		Skills:      []string{"Go", "SQL"},
		Education:   []domain.Education{{School: "HUST", EndYear: ptr(2020)}},
		Experiences: []domain.Experience{{Company: "Acme", Title: "Engineer", Start: ptr("2020-01"), Highlights: []string{}}},
		Languages:   []string{},
	}
	e.uc.cv = sampleCV(t, domain.StatusParsed, &want)

	got, err := e.client.UpdateParsedData(e.candidateCtx(t), req)
	require.NoError(t, err)
	assert.Equal(t, id, e.uc.id)

	p := e.uc.parsed
	assert.Equal(t, want, p)
	// unset optionals → nil; empty repeated → non-nil empty slice
	assert.Nil(t, p.Email)
	assert.Nil(t, p.YearsOfExperience)
	assert.Nil(t, p.Education[0].Degree)
	assert.Nil(t, p.Experiences[0].End)
	assert.NotNil(t, p.Languages)
	assert.Empty(t, p.Languages)
	assert.NotNil(t, p.Experiences[0].Highlights)

	gp := got.GetParsed()
	require.NotNil(t, gp)
	assert.Equal(t, "An", gp.GetFullName())
	assert.Nil(t, gp.Email)
	assert.Nil(t, gp.YearsOfExperience)
	assert.Equal(t, []string{"Go", "SQL"}, gp.GetSkills())
	require.Len(t, gp.GetEducation(), 1)
	assert.Equal(t, "HUST", gp.GetEducation()[0].GetSchool())
	assert.Equal(t, int32(2020), gp.GetEducation()[0].GetEndYear())
	assert.Nil(t, gp.GetEducation()[0].Degree)
	require.Len(t, gp.GetExperiences(), 1)
	assert.Equal(t, "2020-01", gp.GetExperiences()[0].GetStart())
	assert.Nil(t, gp.GetExperiences()[0].End)
	assert.Empty(t, gp.GetLanguages())
}

func TestHandler_ParsedRequired(t *testing.T) {
	e := newEnv(t)
	_, err := e.client.UpdateParsedData(e.candidateCtx(t), &candidatev1.UpdateParsedDataRequest{Id: uuid.NewString()})
	assert.Equal(t, codes.InvalidArgument, code(err))
	assert.Zero(t, e.uc.calls)
}

func TestHandler_StatusMapping(t *testing.T) {
	e := newEnv(t)
	for st, want := range map[domain.Status]candidatev1.CVStatus{
		domain.StatusUploading: candidatev1.CVStatus_CV_STATUS_UPLOADING,
		domain.StatusUploaded:  candidatev1.CVStatus_CV_STATUS_UPLOADED,
		domain.StatusParsed:    candidatev1.CVStatus_CV_STATUS_PARSED,
		domain.StatusFailed:    candidatev1.CVStatus_CV_STATUS_FAILED,
		domain.Status("WEIRD"): candidatev1.CVStatus_CV_STATUS_UNSPECIFIED,
	} {
		e.uc.cv = sampleCV(t, st, nil)
		got, err := e.client.ReparseCV(e.candidateCtx(t), &candidatev1.ReparseCVRequest{Id: e.uc.cv.ID.String()})
		require.NoError(t, err)
		assert.Equal(t, want, got.GetStatus(), string(st))
	}
}

func TestHandler_ErrorMapping(t *testing.T) {
	e := newEnv(t)
	for err, want := range map[error]codes.Code{
		domain.ErrCVNotFound:        codes.NotFound,
		domain.ErrProfileNotFound:   codes.NotFound,
		domain.ErrInvalidTransition: codes.InvalidArgument,
	} {
		e.uc.err = err
		_, got := e.client.GetCV(e.candidateCtx(t), &candidatev1.GetCVRequest{Id: uuid.NewString()})
		assert.Equal(t, want, code(got), err.Error())
	}
}

func TestHandler_CorruptParsedDataIsInternal(t *testing.T) {
	e := newEnv(t)
	cv := sampleCV(t, domain.StatusParsed, nil)
	cv.ParsedData = json.RawMessage(`{"full_name": 42, "secret":"x@example.com"}`)
	e.uc.cv = cv
	_, err := e.client.GetCV(e.candidateCtx(t), &candidatev1.GetCVRequest{Id: cv.ID.String()})
	assert.Equal(t, codes.Internal, code(err))
	assert.NotContains(t, status.Convert(err).Message(), "example.com")
}

func TestHandler_Profile(t *testing.T) {
	e := newEnv(t)
	uid := uuid.New()
	e.uc.cand = domain.Candidate{UserID: uid, FullName: "An", Location: ptr("Hà Nội"), CreatedAt: t0, UpdatedAt: t0}

	got, err := e.client.UpsertMyProfile(e.candidateCtx(t), &candidatev1.UpsertMyProfileRequest{
		FullName: "An", Phone: ptr("+84 912 345 678"),
	})
	require.NoError(t, err)
	assert.Equal(t, "An", e.uc.profile.FullName)
	require.NotNil(t, e.uc.profile.Phone)
	assert.Equal(t, "+84 912 345 678", *e.uc.profile.Phone)
	assert.Nil(t, e.uc.profile.Location)

	assert.Equal(t, uid.String(), got.GetUserId())
	assert.Equal(t, "An", got.GetFullName())
	assert.Nil(t, got.Phone)
	assert.Equal(t, "Hà Nội", got.GetLocation())
	assert.Equal(t, t0, got.GetCreatedAt().AsTime())

	got, err = e.client.GetMyProfile(e.candidateCtx(t), &candidatev1.GetMyProfileRequest{})
	require.NoError(t, err)
	assert.Equal(t, uid.String(), got.GetUserId())
}

func TestHandler_ListMyCVs(t *testing.T) {
	e := newEnv(t)
	a, b := sampleCV(t, domain.StatusUploading, nil), sampleCV(t, domain.StatusUploaded, nil)
	e.uc.page = app.Page{CVs: []domain.CV{a, b}, NextToken: "next"}
	got, err := e.client.ListMyCVs(e.candidateCtx(t), &candidatev1.ListMyCVsRequest{PageSize: 2, PageToken: "tok"})
	require.NoError(t, err)
	assert.Equal(t, app.PageRequest{Size: 2, Token: "tok"}, e.uc.pageReq)
	require.Len(t, got.GetCvs(), 2)
	assert.Equal(t, a.ID.String(), got.GetCvs()[0].GetId())
	assert.Equal(t, b.ID.String(), got.GetCvs()[1].GetId())
	assert.Equal(t, "next", got.GetNextPageToken())

	b2, err := protojson.Marshal(got)
	require.NoError(t, err)
	assert.False(t, strings.Contains(string(b2), "cvs/"), "list must not expose object keys")

	_, err = e.client.ListMyCVs(e.candidateCtx(t), &candidatev1.ListMyCVsRequest{PageSize: 51})
	assert.Equal(t, codes.InvalidArgument, code(err))
}
