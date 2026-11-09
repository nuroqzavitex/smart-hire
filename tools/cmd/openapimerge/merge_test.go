package main

import (
	"strings"
	"testing"
)

func doc(paths, defs map[string]any) map[string]any {
	return map[string]any{
		"swagger":     "2.0",
		"info":        map[string]any{"title": "generated", "version": "1"},
		"paths":       paths,
		"definitions": defs,
	}
}

func TestMerge_CombinesPaths(t *testing.T) {
	gen := doc(
		map[string]any{"/v1/auth/register": map[string]any{"post": map[string]any{"operationId": "r"}}},
		map[string]any{"v1User": map[string]any{"type": "object"}},
	)
	hand := doc(
		map[string]any{
			"/v1/auth/login":    map[string]any{"post": map[string]any{"operationId": "l"}},
			"/v1/auth/register": map[string]any{"get": map[string]any{"operationId": "g"}},
		},
		map[string]any{"ErrorEnvelope": map[string]any{"type": "object"}},
	)
	out, err := merge(gen, hand)
	if err != nil {
		t.Fatalf("merge: %v", err)
	}
	paths := out["paths"].(map[string]any)
	if len(paths) != 2 {
		t.Fatalf("paths = %d, want 2", len(paths))
	}
	reg := paths["/v1/auth/register"].(map[string]any)
	if _, ok := reg["post"]; !ok {
		t.Error("register lost post")
	}
	if _, ok := reg["get"]; !ok {
		t.Error("register missing get")
	}
	defs := out["definitions"].(map[string]any)
	if _, ok := defs["v1User"]; !ok {
		t.Error("missing v1User")
	}
	if _, ok := defs["ErrorEnvelope"]; !ok {
		t.Error("missing ErrorEnvelope")
	}
	if got := out["info"].(map[string]any)["title"]; got != "SmartHire Gateway API" {
		t.Errorf("title = %v", got)
	}
}

func TestMerge_DuplicatePathMethodFails(t *testing.T) {
	p := func() map[string]any {
		return map[string]any{"/v1/auth/register": map[string]any{"post": map[string]any{}}}
	}
	_, err := merge(doc(p(), nil), doc(p(), nil))
	if err == nil || !strings.Contains(err.Error(), "/v1/auth/register") {
		t.Fatalf("err = %v, want mention of /v1/auth/register", err)
	}
}

func TestMerge_DefinitionConflictFails(t *testing.T) {
	a := doc(nil, map[string]any{"X": map[string]any{"type": "object"}})
	b := doc(nil, map[string]any{"X": map[string]any{"type": "string"}})
	_, err := merge(a, b)
	if err == nil || !strings.Contains(err.Error(), "X") {
		t.Fatalf("err = %v, want mention of X", err)
	}
	// identical definitions are fine
	if _, err := merge(a, doc(nil, map[string]any{"X": map[string]any{"type": "object"}})); err != nil {
		t.Fatalf("identical defs: %v", err)
	}
}
