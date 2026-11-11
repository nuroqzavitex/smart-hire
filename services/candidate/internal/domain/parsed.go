package domain

import "encoding/json"

// ParsedCV is the structured data extracted from a CV (contract:
// contracts/events/cv.parsed.v1.json $defs.ParsedCV). Every key is always
// emitted: optionals as null, lists as [].
type ParsedCV struct {
	FullName          *string      `json:"full_name"`
	Email             *string      `json:"email"`
	Skills            []string     `json:"skills"`
	YearsOfExperience *float64     `json:"years_of_experience"`
	CurrentTitle      *string      `json:"current_title"`
	Education         []Education  `json:"education"`
	Experiences       []Experience `json:"experiences"`
	Languages         []string     `json:"languages"`
	Location          *string      `json:"location"`
}

// Education is one education entry of a ParsedCV.
type Education struct {
	School  string  `json:"school"`
	Degree  *string `json:"degree"`
	Major   *string `json:"major"`
	EndYear *int    `json:"end_year"`
}

// Experience is one work-experience entry of a ParsedCV. Start and End are
// "YYYY-MM" or nil.
type Experience struct {
	Company    string   `json:"company"`
	Title      string   `json:"title"`
	Start      *string  `json:"start"`
	End        *string  `json:"end"`
	Highlights []string `json:"highlights"`
}

// MarshalCanonical encodes p with nil slices (including nested ones) as [] so
// the output carries every key the schema requires. p is not modified.
func (p ParsedCV) MarshalCanonical() (json.RawMessage, error) {
	p.Skills = nonNil(p.Skills)
	p.Languages = nonNil(p.Languages)
	p.Education = nonNil(p.Education)
	exps := make([]Experience, len(p.Experiences))
	for i, e := range p.Experiences {
		e.Highlights = nonNil(e.Highlights)
		exps[i] = e
	}
	p.Experiences = exps
	return json.Marshal(p)
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
