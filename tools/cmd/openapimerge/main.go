// Command openapimerge merges the grpc-gateway generated Swagger file with the
// handwritten one into a single gateway.yaml.
package main

import (
	"flag"
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

func load(path string) (map[string]any, error) {
	b, err := os.ReadFile(path) //nolint:gosec // CLI input path
	if err != nil {
		return nil, err
	}
	var m map[string]any
	// YAML is a superset of JSON, so this reads both inputs.
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return m, nil
}

func run() error {
	gen := flag.String("gen", "contracts/openapi/gen/gateway.swagger.json", "generated swagger file")
	hand := flag.String("hand", "contracts/openapi/gateway.handwritten.yaml", "handwritten swagger file")
	out := flag.String("out", "contracts/openapi/gateway.yaml", "output file")
	flag.Parse()

	g, err := load(*gen)
	if err != nil {
		return err
	}
	h, err := load(*hand)
	if err != nil {
		return err
	}
	merged, err := merge(g, h)
	if err != nil {
		return err
	}
	b, err := yaml.Marshal(merged)
	if err != nil {
		return err
	}
	return os.WriteFile(*out, b, 0o644) //nolint:gosec // contract file is meant to be readable
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "openapimerge:", err)
		os.Exit(1)
	}
}
