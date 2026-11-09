package main

import (
	"fmt"
	"reflect"
)

const title = "SmartHire Gateway API"

// merge combines two Swagger 2.0 documents: paths and definitions are unioned,
// info comes from generated with its title replaced. A repeated (path, method)
// or a definition with different content is an error.
func merge(generated, handwritten map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(generated))
	for k, v := range generated {
		out[k] = v
	}

	paths := map[string]any{}
	for _, src := range []map[string]any{generated, handwritten} {
		ps, _ := src["paths"].(map[string]any)
		for path, item := range ps {
			ops, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("path %q: not an object", path)
			}
			dst, _ := paths[path].(map[string]any)
			if dst == nil {
				dst = map[string]any{}
				paths[path] = dst
			}
			for method, op := range ops {
				if _, dup := dst[method]; dup {
					return nil, fmt.Errorf("duplicate operation %s %s", method, path)
				}
				dst[method] = op
			}
		}
	}
	out["paths"] = paths

	defs := map[string]any{}
	for _, src := range []map[string]any{generated, handwritten} {
		ds, _ := src["definitions"].(map[string]any)
		for name, def := range ds {
			if prev, ok := defs[name]; ok && !reflect.DeepEqual(prev, def) {
				return nil, fmt.Errorf("definition %q conflicts between generated and handwritten", name)
			}
			defs[name] = def
		}
	}
	out["definitions"] = defs

	info := map[string]any{}
	if gi, ok := generated["info"].(map[string]any); ok {
		for k, v := range gi {
			info[k] = v
		}
	}
	info["title"] = title
	out["info"] = info
	return out, nil
}
