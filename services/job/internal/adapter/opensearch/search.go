package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"

	"github.com/nuroqzavitex/smarthire/services/job/internal/app"
	"github.com/nuroqzavitex/smarthire/services/job/internal/domain"
	"github.com/nuroqzavitex/smarthire/services/job/internal/projection"
)

var textFields = []string{"title^3", "title.raw^4", "required_skills.text^2", "description", "location.text"}

// buildSearch renders the request body. Filters copy the M1a Postgres
// semantics (SearchOpenJobs) so results do not change on fallback. It asks
// for one extra hit to know whether another page exists.
func buildSearch(q app.SearchQuery) map[string]any {
	f := q.Filter
	filter := []any{term("status", projection.StatusOpen)}
	if len(f.Skills) > 0 {
		filter = append(filter, map[string]any{"terms": map[string]any{"required_skills": f.Skills}})
	}
	if f.Location != "" {
		filter = append(filter, term("location", f.Location))
	}
	if f.Level != nil {
		filter = append(filter, term("level", string(*f.Level)))
	}
	if f.SalaryMin != nil {
		filter = append(filter, map[string]any{"bool": map[string]any{
			"should": []any{
				map[string]any{"bool": map[string]any{"must_not": []any{map[string]any{"exists": map[string]any{"field": "salary_max"}}}}},
				map[string]any{"range": map[string]any{"salary_max": map[string]any{"gte": *f.SalaryMin}}},
			},
			"minimum_should_match": 1,
		}})
	}
	boolQ := map[string]any{"filter": filter}
	recency := []any{
		map[string]any{"published_at": map[string]any{"order": "desc", "missing": "_last"}},
		map[string]any{"job_id": "desc"},
	}
	sort := recency
	if f.Query != "" {
		boolQ["must"] = []any{map[string]any{"multi_match": map[string]any{
			"query": f.Query, "type": "cross_fields", "operator": "and", "fields": textFields,
		}}}
		sort = append([]any{"_score"}, recency...)
	}
	body := map[string]any{
		"size":             q.Size + 1,
		"track_total_hits": false,
		"query":            map[string]any{"bool": boolQ},
		"sort":             sort,
		"aggs": map[string]any{
			"skills":    terms("required_skills", 20),
			"locations": terms("location", 20),
			"levels":    terms("level", 5),
		},
	}
	if len(q.After) > 0 {
		body["search_after"] = q.After
	}
	return body
}

func term(field string, v any) map[string]any {
	return map[string]any{"term": map[string]any{field: v}}
}

func terms(field string, size int) map[string]any {
	return map[string]any{"terms": map[string]any{"field": field, "size": size}}
}

type searchResponse struct {
	Hits struct {
		Hits []struct {
			Source projection.Document `json:"_source"`
			Sort   json.RawMessage     `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
	Aggregations map[string]struct {
		Buckets []struct {
			Key   string `json:"key"`
			Count int64  `json:"doc_count"`
		} `json:"buckets"`
	} `json:"aggregations"`
}

// Search implements app.SearchBackend over the read alias.
func (c *Client) Search(ctx context.Context, q app.SearchQuery) (app.SearchResult, error) {
	status, body, err := c.do(ctx, http.MethodPost, "/"+ReadAlias+"/_search", nil, buildSearch(q))
	if err != nil {
		return app.SearchResult{}, err
	}
	if status != http.StatusOK {
		return app.SearchResult{}, newError("search", status, body)
	}
	var r searchResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return app.SearchResult{}, fmt.Errorf("decode search response: %w", err)
	}
	hits := r.Hits.Hits
	var res app.SearchResult
	if len(hits) > q.Size {
		hits = hits[:q.Size]
		res.Next = hits[len(hits)-1].Sort
	}
	res.Jobs = make([]domain.Job, 0, len(hits))
	for _, h := range hits {
		j, err := toJob(h.Source)
		if err != nil {
			return app.SearchResult{}, err
		}
		res.Jobs = append(res.Jobs, j)
	}
	facet := func(name string) []app.FacetCount {
		buckets := r.Aggregations[name].Buckets
		out := make([]app.FacetCount, len(buckets))
		for i, b := range buckets {
			out[i] = app.FacetCount{Value: b.Key, Count: b.Count}
		}
		return out
	}
	res.Facets = app.Facets{Skills: facet("skills"), Locations: facet("locations"), Levels: facet("levels")}
	return res, nil
}

func toJob(d projection.Document) (domain.Job, error) {
	id, err := uuid.Parse(d.JobID)
	if err != nil {
		return domain.Job{}, fmt.Errorf("read model document has invalid job_id")
	}
	recruiter, err := uuid.Parse(d.RecruiterID)
	if err != nil {
		return domain.Job{}, fmt.Errorf("read model document has invalid recruiter_id")
	}
	return domain.Job{
		ID: id, RecruiterID: recruiter, Title: d.Title, Description: d.Description,
		RequiredSkills: nonNil(d.RequiredSkills), NiceToHaveSkills: nonNil(d.NiceToHaveSkills),
		Level: domain.Level(d.Level), Location: d.Location, SalaryMin: d.SalaryMin, SalaryMax: d.SalaryMax,
		Status: domain.Status(d.Status), Version: d.Version, PublishedAt: d.PublishedAt,
	}, nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
