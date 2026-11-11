// Package eventschema validates serialized event envelopes (and named $defs
// such as cv.parsed#ParsedCV) against the JSON Schemas stored in
// contracts/events. It is used by tests and at runtime by consumers; service
// images copy contracts/events to /contracts/events so Dir finds it.
package eventschema

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var (
	typeRe = regexp.MustCompile(`^[a-z]+(\.[a-z_]+)+$`)
	defRe  = regexp.MustCompile(`^[A-Za-z]+$`)
)

var (
	mu    sync.Mutex
	cache = map[string]*jsonschema.Schema{}
)

// Dir walks up from the working directory to the directory that contains
// contracts/events and returns that events directory.
func Dir() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		cand := filepath.Join(dir, "contracts", "events")
		if st, err := os.Stat(cand); err == nil && st.IsDir() {
			return cand, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("contracts/events not found above working directory")
		}
		dir = parent
	}
}

// compile returns the cached schema for eventType, or for its $defs/<def>
// when def is non-empty. Callers validate eventType and def first.
func compile(eventType, def string) (*jsonschema.Schema, error) {
	key := eventType
	if def != "" {
		key += "#" + def
	}
	mu.Lock()
	defer mu.Unlock()
	if s, ok := cache[key]; ok {
		return s, nil
	}
	dir, err := Dir()
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(filepath.Join(dir, eventType+".v1.json")) //nolint:gosec // eventType validated by typeRe
	if err != nil {
		return nil, fmt.Errorf("no schema for event type %q: %w", eventType, err)
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("parse schema %q: %w", eventType, err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	url := "https://smarthire.local/events/" + eventType + ".v1.json"
	if err := c.AddResource(url, doc); err != nil {
		return nil, err
	}
	target := url
	if def != "" {
		target += "#/$defs/" + def
	}
	s, err := c.Compile(target)
	if err != nil {
		return nil, fmt.Errorf("compile schema %q: %w", key, err)
	}
	cache[key] = s
	return s, nil
}

func validate(s *jsonschema.Schema, doc []byte) error {
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return fmt.Errorf("parse document: %w", err)
	}
	return s.Validate(inst)
}

// Validate checks a JSON event envelope against the v1 schema of eventType.
func Validate(eventType string, doc []byte) error {
	if !typeRe.MatchString(eventType) {
		return fmt.Errorf("invalid event type %q", eventType)
	}
	s, err := compile(eventType, "")
	if err != nil {
		return err
	}
	return validate(s, doc)
}

// ValidateDef checks a JSON document against $defs/<def> of the v1 schema of
// eventType (e.g. ValidateDef("cv.parsed", "ParsedCV", body)).
func ValidateDef(eventType, def string, doc []byte) error {
	if !typeRe.MatchString(eventType) {
		return fmt.Errorf("invalid event type %q", eventType)
	}
	if !defRe.MatchString(def) {
		return fmt.Errorf("invalid schema definition name %q", def)
	}
	s, err := compile(eventType, def)
	if err != nil {
		return err
	}
	return validate(s, doc)
}
