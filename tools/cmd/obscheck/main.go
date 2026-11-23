// Command obscheck verifies the telemetry pipeline of a running stack (ADR
// 0013): service metrics reached Prometheus through the OTel collector, logs
// reached Loki, and the alert rules are loaded. It exits non-zero on the
// first failing check.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

// metrics must each have at least one series in Prometheus.
var metrics = []string{
	"outbox_pending",
	"saga_instances",
	"circuit_breaker_state",
	"kafka_consumer_lag",
	"rpc_server_call_duration_seconds_count",
	"notifications_created_total",
	"account_deletions",
}

// alertRules is how many rules deploy/docker/prometheus/rules defines.
const alertRules = 8

type check struct {
	name string
	fn   func() error
}

func main() {
	prom := flag.String("prom", "http://127.0.0.1:9090", "Prometheus base URL")
	loki := flag.String("loki", "http://127.0.0.1:3100", "Loki base URL")
	service := flag.String("service", "gateway", "service whose logs must be in Loki")
	flag.Parse()
	c := &http.Client{Timeout: 10 * time.Second}
	var checks []check
	for _, m := range metrics {
		checks = append(checks, check{"metric " + m, func() error { return series(c, *prom, m) }})
	}
	checks = append(checks,
		check{"logs of " + *service + " in loki", func() error { return logs(c, *loki, *service) }},
		check{"alert rules loaded", func() error { return rules(c, *prom) }},
	)
	failed := false
	for _, ch := range checks {
		if err := ch.fn(); err != nil {
			fmt.Printf("FAIL %s: %v\n", ch.name, err)
			failed = true
			continue
		}
		fmt.Printf("PASS %s\n", ch.name)
	}
	if failed {
		os.Exit(1)
	}
}

func getJSON(c *http.Client, u string, out any) error {
	resp, err := c.Get(u)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", u, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func series(c *http.Client, prom, metric string) error {
	var out struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := getJSON(c, prom+"/api/v1/query?query="+url.QueryEscape(metric), &out); err != nil {
		return err
	}
	if len(out.Data.Result) == 0 {
		return errors.New("no series")
	}
	return nil
}

func logs(c *http.Client, loki, service string) error {
	now := time.Now()
	q := url.Values{
		"query": {fmt.Sprintf(`{service_name=%q}`, service)},
		"start": {strconv.FormatInt(now.Add(-15*time.Minute).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.UnixNano(), 10)},
		"limit": {"1"},
	}
	var out struct {
		Data struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := getJSON(c, loki+"/loki/api/v1/query_range?"+q.Encode(), &out); err != nil {
		return err
	}
	if len(out.Data.Result) == 0 {
		return errors.New("no log lines in the last 15 minutes")
	}
	return nil
}

func rules(c *http.Client, prom string) error {
	var out struct {
		Data struct {
			Groups []struct {
				Rules []json.RawMessage `json:"rules"`
			} `json:"groups"`
		} `json:"data"`
	}
	if err := getJSON(c, prom+"/api/v1/rules?type=alert", &out); err != nil {
		return err
	}
	n := 0
	for _, g := range out.Data.Groups {
		n += len(g.Rules)
	}
	if n != alertRules {
		return fmt.Errorf("%d alert rules loaded, want %d", n, alertRules)
	}
	return nil
}
