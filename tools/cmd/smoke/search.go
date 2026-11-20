package main

import (
	"errors"
	"fmt"
	"strings"

	"github.com/oklog/ulid/v2"
)

// searchSteps checks the OpenSearch read model (M2a, ADR 0009): a published
// job becomes searchable, unaccented and synonym queries find it, facets are
// returned (so the read model, not the Postgres fallback, served it), and a
// closed job disappears.
func (s *smoke) searchSteps() []namedStep {
	tag := "smk" + strings.ToLower(ulid.Make().String())
	title := "Kỹ sư Backend " + tag
	desc := "Xây dựng dịch vụ phân tán cho nền tảng tuyển dụng, mã " + tag + "."
	var access, id string

	return []namedStep{
		{"search: recruiter login", func() (err error) {
			access, _, err = s.loginAs(s.recruiterEmail, s.recruiterPassword)
			return err
		}},
		{"search: create and publish job", func() (err error) {
			if id, err = s.createJobWith(access, title, desc, []string{"Golang", "Kubernetes"}); err != nil {
				return err
			}
			return s.transitionJob(access, id, "publish", "JOB_STATUS_OPEN")
		}},
		{"search finds new job", func() error {
			r, err := s.searchUntil(tag, func(r searchResult) bool { return r.has(id) })
			if err != nil {
				return err
			}
			if r.Degraded {
				return errors.New("served by the postgres fallback (degraded), want the read model")
			}
			return nil
		}},
		{"unaccented and synonym", func() error {
			for _, q := range []string{"ha noi " + tag, "ky su " + tag, "go " + tag, "k8s " + tag} {
				if err := s.searchExpect(q, id, true); err != nil {
					return err
				}
			}
			return nil
		}},
		{"facets", func() error {
			r, err := s.search(tag)
			if err != nil {
				return err
			}
			for _, f := range r.Facets.Skills {
				if f.Value == "golang" && f.Count == "1" {
					return nil
				}
			}
			return fmt.Errorf("skills facet %+v has no golang=1", r.Facets.Skills)
		}},
		{"closed job disappears", func() error {
			if err := s.transitionJob(access, id, "close", "JOB_STATUS_CLOSED"); err != nil {
				return err
			}
			return s.searchExpect(tag, id, false)
		}},
	}
}
