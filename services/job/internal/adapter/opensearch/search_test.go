package opensearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
)

const aggsJSON = `"aggs":{
	"skills":{"terms":{"field":"required_skills","size":20}},
	"locations":{"terms":{"field":"location","size":20}},
	"levels":{"terms":{"field":"level","size":5}}}`

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}

func TestBuildQuery_Golden(t *testing.T) {
	t.Run("no text", func(t *testing.T) {
		got := mustJSON(t, buildSearch(app.SearchQuery{Size: 10}))
		assert.JSONEq(t, `{"size":11,"track_total_hits":false,
			"query":{"bool":{"filter":[{"term":{"status":"open"}}]}},
			"sort":[{"published_at":{"order":"desc","missing":"_last"}},{"job_id":"desc"}],`+aggsJSON+`}`, got)
	})

	t.Run("text and every filter", func(t *testing.T) {
		senior := domain.LevelSenior
		salary := 3000
		got := mustJSON(t, buildSearch(app.SearchQuery{
			Size:  20,
			After: json.RawMessage(`[1.5,1732000000000,"j9"]`),
			Filter: app.SearchFilter{
				Query: "ky su go", Skills: []string{"golang", "rust"}, Location: "Ha Noi",
				Level: &senior, SalaryMin: &salary,
			},
		}))
		assert.JSONEq(t, `{"size":21,"track_total_hits":false,
			"query":{"bool":{
				"filter":[
					{"term":{"status":"open"}},
					{"terms":{"required_skills":["golang","rust"]}},
					{"term":{"location":"Ha Noi"}},
					{"term":{"level":"senior"}},
					{"bool":{"should":[
						{"bool":{"must_not":[{"exists":{"field":"salary_max"}}]}},
						{"range":{"salary_max":{"gte":3000}}}],"minimum_should_match":1}}],
				"must":[{"multi_match":{"query":"ky su go","type":"cross_fields","operator":"and",
					"fields":["title^3","title.raw^4","required_skills.text^2","description","location.text"]}}]}},
			"sort":["_score",{"published_at":{"order":"desc","missing":"_last"}},{"job_id":"desc"}],
			"search_after":[1.5,1732000000000,"j9"],`+aggsJSON+`}`, got)
	})
}
