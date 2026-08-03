package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	agentstormv1alpha1 "github.com/Alphasxd/AgentStorm/api/v1alpha1"
	"github.com/Alphasxd/AgentStorm/internal/reliability"
	"github.com/Alphasxd/AgentStorm/internal/results"
)

type ResultWriter interface {
	Register(context.Context, string, []byte, results.Registration) error
	Terminal(context.Context, string, []byte, results.TerminalRequest) error
}

type QueueReader interface {
	QueueStatus(context.Context, string) (results.QueueStatus, error)
}

type HTTPResultWriter struct {
	baseURL string
	client  *http.Client
}

func NewHTTPResultWriter(config ResultSinkConfig) *HTTPResultWriter {
	return &HTTPResultWriter{
		baseURL: config.URL,
		client:  &http.Client{Timeout: time.Duration(config.TimeoutSeconds) * time.Second},
	}
}

func (w *HTTPResultWriter) Register(ctx context.Context, runID string, token []byte, registration results.Registration) error {
	return w.put(ctx, runID, token, "/v1/runs/"+url.PathEscape(runID), "run/"+runID, registration)
}

func (w *HTTPResultWriter) Terminal(ctx context.Context, runID string, token []byte, terminal results.TerminalRequest) error {
	return w.put(
		ctx,
		runID,
		token,
		"/v1/runs/"+url.PathEscape(runID)+"/terminal",
		fmt.Sprintf("run/%s/terminal/%s", runID, terminal.Status),
		terminal,
	)
}

func (w *HTTPResultWriter) QueueStatus(ctx context.Context, runID string) (results.QueueStatus, error) {
	if runID == "" {
		return results.QueueStatus{}, fmt.Errorf("queue request identity is unavailable")
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, w.baseURL+"/v1/runs/"+url.PathEscape(runID)+"/queue", nil,
	)
	if err != nil {
		return results.QueueStatus{}, fmt.Errorf("create queue status request: %w", err)
	}
	response, err := w.client.Do(request)
	if err != nil {
		return results.QueueStatus{}, fmt.Errorf("result API is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return results.QueueStatus{}, fmt.Errorf("result API returned HTTP %d", response.StatusCode)
	}
	var status results.QueueStatus
	decoder := json.NewDecoder(io.LimitReader(response.Body, 1<<20))
	if err := decoder.Decode(&status); err != nil {
		return results.QueueStatus{}, fmt.Errorf("decode queue status: %w", err)
	}
	return status, nil
}

func (w *HTTPResultWriter) put(ctx context.Context, runID string, token []byte, path, idempotencyKey string, value any) error {
	if runID == "" || len(token) == 0 {
		return fmt.Errorf("result request identity is unavailable")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal result request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, w.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create result request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+string(token))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response, err := w.client.Do(request)
	if err != nil {
		return fmt.Errorf("result API is unavailable")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return fmt.Errorf("result API returned HTTP %d", response.StatusCode)
	}
	return nil
}

func buildResultRegistration(run *agentstormv1alpha1.AgentTestRun, scenario *reliability.Snapshot) results.Registration {
	registration := results.Registration{
		SchemaVersion:  results.SchemaVersion,
		ExpectedShards: int(run.Spec.Workload.Parallelism),
		Source: results.RunSource{
			Namespace: run.Namespace,
			Name:      run.Name,
		},
		Target: results.RunTarget{
			Provider: run.Spec.Target.Provider,
			Model:    run.Spec.Target.Model,
		},
		Dataset: results.RunDataset{
			Name: run.Spec.Workload.DatasetRef.Name,
			Key:  run.Spec.Workload.DatasetRef.Key,
		},
		Evaluation: results.Evaluation{
			MinSuccessRate: run.Spec.Evaluation.MinSuccessRate,
			MaxFailureRate: run.Spec.Evaluation.MaxErrorRate,
		},
		Scheduling: &results.RunScheduling{
			Strategy:        run.Spec.Scheduling.Strategy,
			MaxWorkers:      run.Spec.Scheduling.MaxWorkers,
			ResourceProfile: run.Spec.Scheduling.ResourceProfile,
		},
	}
	if value := run.Spec.Evaluation.MaxP95LatencyMs; value != nil {
		converted := float64(*value)
		registration.Evaluation.MaxP95MS = &converted
	}
	if pricing := run.Spec.Target.Pricing; pricing != nil {
		registration.Target.Pricing = &results.RunPricing{
			InputUSDPerMillionTokens:  pricing.InputUSDPerMillionTokens,
			OutputUSDPerMillionTokens: pricing.OutputUSDPerMillionTokens,
		}
	}
	if spec := run.Spec.Reliability; spec != nil {
		retry := spec.Retry
		registration.Reliability = &results.RunReliability{
			Seed: spec.Seed,
			Retry: results.RunRetry{
				MaxAttempts:            retry.MaxAttempts,
				InitialBackoffMS:       retry.InitialBackoffMs,
				MaxBackoffMS:           retry.MaxBackoffMs,
				MaxCumulativeBackoffMS: retry.MaxCumulativeBackoffMs,
				JitterRatio:            *retry.JitterRatio,
				AllowAmbiguousRetries:  retry.AllowAmbiguousRetries,
			},
		}
		if breaker := spec.CircuitBreaker; breaker != nil {
			registration.Reliability.CircuitBreaker = &results.RunCircuitBreaker{
				FailureThreshold: breaker.FailureThreshold,
				OpenDurationMS:   breaker.OpenDurationMs,
			}
		}
		if scenario != nil {
			registration.Reliability.Scenario = &results.RunFaultScenarioSnapshot{
				Source:   results.RunScenarioSource{Name: scenario.SourceName, Key: scenario.SourceKey},
				Digest:   scenario.Digest,
				Document: scenario.Scenario,
			}
		}
	}
	return registration
}
