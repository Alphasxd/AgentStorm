package results

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestHandler(t *testing.T, repository Repository, objects ObjectStore) http.Handler {
	t.Helper()
	handler, err := NewHTTPHandler(NewService(repository, objects), HTTPConfig{
		WriteToken: "write-secret",
		ReadToken:  "read-secret",
	})
	if err != nil {
		t.Fatalf("create handler: %v", err)
	}
	return handler
}

func TestHealthAndReadiness(t *testing.T) {
	handler := newTestHandler(t, repositoryStub{}, objectStoreStub{})
	for _, path := range []string{"/healthz", "/readyz"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s returned %d: %s", path, response.Code, response.Body.String())
		}
	}
}

func TestReadinessHidesDependencyDetails(t *testing.T) {
	handler := newTestHandler(t, repositoryStub{
		ready: func(context.Context) error { return ErrNotReady },
	}, objectStoreStub{})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable || bytes.Contains(response.Body.Bytes(), []byte("database")) {
		t.Fatalf("unexpected readiness response: %d %s", response.Code, response.Body.String())
	}
}

func TestWriteAndReadTokensAreSeparated(t *testing.T) {
	handler := newTestHandler(t, repositoryStub{
		registerRun: func(context.Context, string, string, string, Registration) (bool, error) {
			return true, nil
		},
	}, objectStoreStub{})
	payload, _ := json.Marshal(testRegistration(1))

	for name, test := range map[string]struct {
		token    string
		expected int
	}{
		"missing":     {expected: http.StatusUnauthorized},
		"read token":  {token: "read-secret", expected: http.StatusUnauthorized},
		"write token": {token: "write-secret", expected: http.StatusCreated},
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/v1/runs/run-1", bytes.NewReader(payload))
			request.Header.Set("Idempotency-Key", "run/run-1")
			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.expected {
				t.Fatalf("got %d, want %d: %s", response.Code, test.expected, response.Body.String())
			}
		})
	}
}

func TestRegistrationRejectsUnknownFields(t *testing.T) {
	handler := newTestHandler(t, repositoryStub{
		registerRun: func(context.Context, string, string, string, Registration) (bool, error) {
			t.Fatal("repository should not receive invalid JSON")
			return false, nil
		},
	}, objectStoreStub{})
	request := httptest.NewRequest(http.MethodPut, "/v1/runs/run-1", bytes.NewBufferString(`{"schema_version":"v1alpha1","unknown":true}`))
	request.Header.Set("Authorization", "Bearer write-secret")
	request.Header.Set("Idempotency-Key", "run/run-1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("got %d: %s", response.Code, response.Body.String())
	}
}

func TestTerminalEndpointIsWriteAuthenticatedAndIdempotent(t *testing.T) {
	handler := newTestHandler(t, repositoryStub{
		terminateRun: func(_ context.Context, runID, key, hash string, terminal TerminalRequest) (bool, error) {
			if runID != "run-1" || key != "run/run-1/terminal/cancelled" || hash == "" || terminal.ReasonCode != "user_requested" {
				t.Fatalf("unexpected terminal request")
			}
			return true, nil
		},
	}, objectStoreStub{})
	payload := bytes.NewBufferString(`{"status":"cancelled","reason_code":"user_requested"}`)
	request := httptest.NewRequest(http.MethodPut, "/v1/runs/run-1/terminal", payload)
	request.Header.Set("Authorization", "Bearer write-secret")
	request.Header.Set("Idempotency-Key", "run/run-1/terminal/cancelled")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("terminal endpoint returned %d: %s", response.Code, response.Body.String())
	}
}

func TestServiceErrorsHaveStableHTTPMappings(t *testing.T) {
	handler := newTestHandler(t, repositoryStub{
		getRun:  func(context.Context, string) (RunDetail, error) { return RunDetail{}, ErrNotFound },
		compare: func(context.Context, string, string) (Comparison, error) { return Comparison{}, ErrNotReady },
	}, objectStoreStub{})

	tests := []struct {
		path string
		code int
	}{
		{path: "/v1/runs/missing", code: http.StatusNotFound},
		{path: "/v1/comparisons?baseline=a&candidate=b", code: http.StatusConflict},
	}
	for _, test := range tests {
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		request.Header.Set("Authorization", "Bearer read-secret")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.code {
			t.Fatalf("%s got %d: %s", test.path, response.Code, response.Body.String())
		}
	}
}

func TestMetricsExposeBoundedOperationalDataWithoutAuthentication(t *testing.T) {
	finalizeCalls := 0
	handler := newTestHandler(t, repositoryStub{
		reserveShard: func(context.Context, string, int, string, string, string, ShardSummary) (ShardReservation, error) {
			return ShardReservation{Pricing: &RunPricing{
				InputUSDPerMillionTokens:  "2",
				OutputUSDPerMillionTokens: "4",
			}}, nil
		},
		finalizeShard: func(context.Context, string, int, string, []CaseResult, *RunPricing) (bool, error) {
			finalizeCalls++
			return finalizeCalls == 1, nil
		},
	}, objectStoreStub{
		put: func(context.Context, string, []byte, string, string) error { return nil },
	})
	upload := testShard("run-sensitive-id", 0, "case-sensitive-id", false)
	complete := true
	upload.Cases[0].FailureKind = "provider"
	upload.Cases[0].FailureCategory = "provider"
	upload.Cases[0].ErrorCode = "circuit_open"
	upload.Cases[0].Assertions = nil
	upload.Summary.InputTokens = 7
	upload.Summary.OutputTokens = 11
	upload.Summary.UsageComplete = &complete
	upload.Cases[0].InputTokens = 7
	upload.Cases[0].OutputTokens = 11
	upload.Cases[0].UsageComplete = &complete
	upload.Cases[0].Attempts = []AttemptResult{
		{
			Number: 1, LatencyMS: 2, Outcome: "failed", FailureCategory: "provider",
			ErrorCode: "malformed_response", InjectedRule: "sensitive-rule-id",
			InjectedFault: "malformed_response", Ambiguous: true, RetryDecision: "retry_ambiguous",
			InputTokens: 7, OutputTokens: 11, UsageComplete: &complete, CircuitEvents: []string{"open"},
		},
		{
			Number: 2, LatencyMS: 0, Outcome: "rejected", FailureCategory: "provider",
			ErrorCode: "circuit_open", RetryDecision: "not_retryable", UsageComplete: &complete,
			CircuitEvents: []string{"reject"},
		},
	}
	payload, err := json.Marshal(upload)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPut, "/v1/runs/run-sensitive-id/shards/0", bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer write-secret")
	request.Header.Set("Idempotency-Key", "run/run-sensitive-id/shard/0")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("upload returned %d: %s", response.Code, response.Body.String())
	}
	duplicateRequest := httptest.NewRequest(http.MethodPut, "/v1/runs/run-sensitive-id/shards/0", bytes.NewReader(payload))
	duplicateRequest.Header.Set("Authorization", "Bearer write-secret")
	duplicateRequest.Header.Set("Idempotency-Key", "run/run-sensitive-id/shard/0")
	duplicateResponse := httptest.NewRecorder()
	handler.ServeHTTP(duplicateResponse, duplicateRequest)
	if duplicateResponse.Code != http.StatusOK {
		t.Fatalf("duplicate upload returned %d: %s", duplicateResponse.Code, duplicateResponse.Body.String())
	}
	unmatchedResponse := httptest.NewRecorder()
	handler.ServeHTTP(unmatchedResponse, httptest.NewRequest("PRIVATE-METHOD", "/private-path", nil))

	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metricsResponse.Code != http.StatusOK {
		t.Fatalf("metrics returned %d: %s", metricsResponse.Code, metricsResponse.Body.String())
	}
	metrics := metricsResponse.Body.String()
	for _, expected := range []string{
		`agentstorm_result_api_shard_uploads_total{outcome="created"} 1`,
		`agentstorm_result_api_shard_uploads_total{outcome="duplicate"} 1`,
		`agentstorm_result_api_cases_total{failure_kind="provider",result="failure"} 1`,
		`agentstorm_result_api_case_failures_total{category="provider"} 1`,
		`agentstorm_result_api_attempts_total{outcome="failed"} 1`,
		`agentstorm_result_api_attempts_total{outcome="rejected"} 1`,
		`agentstorm_result_api_retry_decisions_total{decision="retry_ambiguous"} 1`,
		`agentstorm_result_api_retry_decisions_total{decision="not_retryable"} 1`,
		`agentstorm_result_api_retries_total{decision="retry_ambiguous",outcome="rejected"} 1`,
		`agentstorm_result_api_injected_faults_total{fault="malformed_response"} 1`,
		`agentstorm_result_api_circuit_events_total{event="open"} 1`,
		`agentstorm_result_api_circuit_events_total{event="reject"} 1`,
		`agentstorm_result_api_tokens_total{direction="input"} 7`,
		`agentstorm_result_api_tokens_total{direction="output"} 11`,
		`agentstorm_result_api_cost_usd_total{direction="input"} 1.4e-05`,
		`agentstorm_result_api_cost_usd_total{direction="output"} 4.4e-05`,
		`agentstorm_result_api_http_requests_total{method="PUT",route="PUT /v1/runs/{runID}/shards/{index}",status_class="2xx"} 2`,
		`agentstorm_result_api_http_requests_total{method="other",route="unmatched",status_class="4xx"} 1`,
	} {
		if !strings.Contains(metrics, expected) {
			t.Fatalf("metrics do not contain %q:\n%s", expected, metrics)
		}
	}
	for _, forbidden := range []string{"run-sensitive-id", "case-sensitive-id", "sensitive-rule-id", "write-secret", "PRIVATE-METHOD", "/private-path"} {
		if strings.Contains(metrics, forbidden) {
			t.Fatalf("metrics contain high-cardinality or sensitive value %q", forbidden)
		}
	}
}
