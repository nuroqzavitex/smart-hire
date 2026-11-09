// Package eventschema validates serialized event envelopes against the JSON
// Schemas stored in contracts/events.
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

var typeRe = regexp.MustCompile(`^[a-z]+(\.[a-z_]+)+$`)

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

func schemaFor(eventType string) (*jsonschema.Schema, error) {
	mu.Lock()
	defer mu.Unlock()
	if s, ok := cache[eventType]; ok {
		return s, nil
	}
	if !typeRe.MatchString(eventType) {
		return nil, fmt.Errorf("invalid event type %q", eventType)
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
	s, err := c.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("compile schema %q: %w", eventType, err)
	}
	cache[eventType] = s
	return s, nil
}

// Validate checks a JSON event envelope against the v1 schema of eventType.
func Validate(eventType string, doc []byte) error {
	s, err := schemaFor(eventType)
	if err != nil {
		return err
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(doc))
	if err != nil {
		return fmt.Errorf("parse event: %w", err)
	}
	return s.Validate(inst)
}
