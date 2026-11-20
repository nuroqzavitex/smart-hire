//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestOpenSearch_ICUInstalled(t *testing.T) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, osURL+"/_cat/plugins", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "analysis-icu")
}

// openSearchImage is built by `make opensearch-image` (deploy/docker/opensearch).
const openSearchImage = "smarthire/opensearch:2.19.2-icu"

// startOpenSearch runs a single-node OpenSearch with the security plugin
// disabled, like deploy/docker, and returns its base URL.
func startOpenSearch(ctx context.Context) (string, func(), error) {
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        openSearchImage,
			ExposedPorts: []string{"9200/tcp"},
			Env: map[string]string{
				"discovery.type":              "single-node",
				"DISABLE_SECURITY_PLUGIN":     "true",
				"DISABLE_INSTALL_DEMO_CONFIG": "true",
				"OPENSEARCH_JAVA_OPTS":        "-Xms512m -Xmx512m",
			},
			WaitingFor: wait.ForHTTP("/_cluster/health").WithPort("9200/tcp").
				WithStartupTimeout(180 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return "", func() {}, err
	}
	stop := func() { _ = testcontainers.TerminateContainer(ctr) }
	host, err := ctr.Host(ctx)
	if err != nil {
		stop()
		return "", func() {}, err
	}
	port, err := ctr.MappedPort(ctx, "9200/tcp")
	if err != nil {
		stop()
		return "", func() {}, err
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port()), stop, nil
}
