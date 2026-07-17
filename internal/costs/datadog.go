package costs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/runs-on/action/internal/config"
	"github.com/sethvargo/go-githubactions"
)

const datadogIntakePath = "/api/v2/series"
const datadogTimeout = 10 * time.Second

// datadogPoint is a single (timestamp, value) tuple in the v2 series payload.
type datadogPoint struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// datadogResource identifies the resource a metric is attached to.
type datadogResource struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// datadogSeries is a single metric series in the v2 payload.
type datadogSeries struct {
	Metric    string            `json:"metric"`
	Type      int               `json:"type"`
	Points    []datadogPoint    `json:"points"`
	Tags      []string          `json:"tags,omitempty"`
	Resources []datadogResource `json:"resources,omitempty"`
	Unit      string            `json:"unit,omitempty"`
}

// datadogPayload is the v2 series intake payload.
type datadogPayload struct {
	Series []datadogSeries `json:"series"`
}

const datadogLogsIntakePath = "/api/v2/logs"

const costLogService = "ci-runs-on-cost"

type datadogLog struct {
	DDSource string `json:"ddsource"`
	Service  string `json:"service"`
	DDTags   string `json:"ddtags"`
	Message  string `json:"message"`
}

type costLogAttrs struct {
	CostUSD           float64 `json:"cost_usd"`
	DurationMinutes   float64 `json:"duration_minutes"`
	RunID             string  `json:"run_id"`
	RunAttempt        string  `json:"run_attempt"`
	Workflow          string  `json:"workflow"`
	Job               string  `json:"job"`
	Repository        string  `json:"repository"`
	Event             string  `json:"event"`
	Ref               string  `json:"ref"`
	InstanceType      string  `json:"instance_type"`
	InstanceLifecycle string  `json:"instance_lifecycle"`
	Region            string  `json:"region"`
}

// pushCostLogToDatadog emits one per-job cost log to the Datadog logs intake.
// run_id/run_attempt are high-cardinality but cheap on logs (billed per event).
func pushCostLogToDatadog(action *githubactions.Action, cfg *config.Config, costData *CostResponseData) error {
	if !cfg.HasDatadog() || costData == nil {
		return nil
	}
	message, err := json.Marshal(costLogAttrs{
		CostUSD:           costData.TotalCost,
		DurationMinutes:   costData.DurationMinutes,
		RunID:             os.Getenv("GITHUB_RUN_ID"),
		RunAttempt:        os.Getenv("GITHUB_RUN_ATTEMPT"),
		Workflow:          os.Getenv("GITHUB_WORKFLOW"),
		Job:               os.Getenv("GITHUB_JOB"),
		Repository:        os.Getenv("GITHUB_REPOSITORY"),
		Event:             os.Getenv("GITHUB_EVENT_NAME"),
		Ref:               os.Getenv("GITHUB_REF_NAME"),
		InstanceType:      costData.InstanceType,
		InstanceLifecycle: costData.InstanceLifecycle,
		Region:            costData.Region,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal cost log: %w", err)
	}
	body, err := json.Marshal([]datadogLog{{
		DDSource: "ci",
		Service:  costLogService,
		DDTags:   fmt.Sprintf("repository:%s", os.Getenv("GITHUB_REPOSITORY")),
		Message:  string(message),
	}})
	if err != nil {
		return fmt.Errorf("failed to marshal cost log payload: %w", err)
	}

	url := fmt.Sprintf("https://http-intake.logs.%s%s", cfg.DatadogSite, datadogLogsIntakePath)
	ctx, cancel := context.WithTimeout(context.Background(), datadogTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to build Datadog log request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", cfg.DatadogAPIKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Datadog log request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		action.Infof("Pushed per-job cost log to Datadog (%s).", url)
		return nil
	}
	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("Datadog logs intake returned %s: %s", resp.Status, string(respBody))
}

// datadogMetricTypeGauge is the v2 numeric type code for gauge metrics.
const datadogMetricTypeGauge = 3

// PushCostMetricsToDatadog forwards cost data to Datadog as custom gauge metrics.
//
// No-op when DD credentials are not configured. Errors are returned to the
// caller, which logs them as warnings — cost reporting failures must not fail
// the workflow.
func PushCostMetricsToDatadog(action *githubactions.Action, cfg *config.Config, costData *CostResponseData) error {
	if !cfg.HasDatadog() {
		action.Infof("Datadog credentials not configured; skipping cost metric push.")
		return nil
	}
	if costData == nil {
		return nil
	}

	tags := buildDatadogTags(costData)
	timestamp := time.Now().Unix()

	metrics := []struct {
		name  string
		value float64
		unit  string
	}{
		{"ci.runs_on.job_cost_usd", costData.TotalCost, "usd"},
		{"ci.runs_on.job_duration_minutes", costData.DurationMinutes, "minute"},
		{"ci.runs_on.github_equivalent_cost_usd", costData.Github.TotalCost, "usd"},
		{"ci.runs_on.savings_usd", costData.Savings.Amount, "usd"},
	}

	series := make([]datadogSeries, 0, len(metrics))
	for _, m := range metrics {
		series = append(series, datadogSeries{
			Metric: m.name,
			Type:   datadogMetricTypeGauge,
			Points: []datadogPoint{{Timestamp: timestamp, Value: m.value}},
			Tags:   tags,
			Unit:   m.unit,
		})
	}

	if err := submitDatadogSeries(action, cfg, datadogPayload{Series: series}); err != nil {
		return err
	}
	// Also emit a per-job cost LOG keyed by run_id so run-level cost can be
	// summed without a high-cardinality run id on the metric.
	return pushCostLogToDatadog(action, cfg, costData)
}

func buildDatadogTags(costData *CostResponseData) []string {
	tags := []string{
		fmt.Sprintf("workflow:%s", os.Getenv("GITHUB_WORKFLOW")),
		fmt.Sprintf("job:%s", os.Getenv("GITHUB_JOB")),
		fmt.Sprintf("repository:%s", os.Getenv("GITHUB_REPOSITORY")),
		fmt.Sprintf("event:%s", os.Getenv("GITHUB_EVENT_NAME")),
		fmt.Sprintf("instance_type:%s", costData.InstanceType),
		fmt.Sprintf("instance_lifecycle:%s", costData.InstanceLifecycle),
		fmt.Sprintf("region:%s", costData.Region),
		fmt.Sprintf("arch:%s", costData.Arch),
		fmt.Sprintf("platform:%s", costData.Platform),
	}
	if standbyType := os.Getenv("RUNS_ON_POOL_STANDBY_TYPE"); standbyType != "" {
		tags = append(tags, fmt.Sprintf("pool_standby_type:%s", standbyType))
	}
	return tags
}

func submitDatadogSeries(action *githubactions.Action, cfg *config.Config, payload datadogPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal Datadog payload: %w", err)
	}

	url := fmt.Sprintf("https://api.%s%s", cfg.DatadogSite, datadogIntakePath)
	ctx, cancel := context.WithTimeout(context.Background(), datadogTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to build Datadog request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("DD-API-KEY", cfg.DatadogAPIKey)
	req.Header.Set("DD-APPLICATION-KEY", cfg.DatadogAppKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("Datadog request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		action.Infof("Pushed %d cost metric series to Datadog (%s).", len(payload.Series), url)
		return nil
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("Datadog intake returned %s: %s", resp.Status, string(respBody))
}
