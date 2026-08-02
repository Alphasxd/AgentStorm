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
	handler := newTestHandler(t, repositoryStub{
		reserveShard: func(context.Context, string, int, string, string, string, ShardSummary) (ShardReservation, error) {
			return ShardReservation{}, nil
		},
		finalizeShard: func(context.Context, string, int, string, []CaseResult) (bool, error) {
			return true, nil
		},
	}, objectStoreStub{
		put: func(context.Context, string, []byte, string, string) error { return nil },
	})
	upload := testShard("run-sensitive-id", 0, "case-sensitive-id", false)
	upload.Cases[0].FailureKind = "provider-secret-material"
	upload.Summary.InputTokens = 7
	upload.Summary.OutputTokens = 11
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
		`agentstorm_result_api_cases_total{failure_kind="other",result="failure"} 1`,
		`agentstorm_result_api_tokens_total{direction="input"} 7`,
		`agentstorm_result_api_tokens_total{direction="output"} 11`,
		`agentstorm_result_api_http_requests_total{method="PUT",route="PUT /v1/runs/{runID}/shards/{index}",status_class="2xx"} 1`,
		`agentstorm_result_api_http_requests_total{method="other",route="unmatched",status_class="4xx"} 1`,
	} {
		if !strings.Contains(metrics, expected) {
			t.Fatalf("metrics do not contain %q:\n%s", expected, metrics)
		}
	}
	for _, forbidden := range []string{"run-sensitive-id", "case-sensitive-id", "provider-secret-material", "write-secret", "PRIVATE-METHOD", "/private-path"} {
		if strings.Contains(metrics, forbidden) {
			t.Fatalf("metrics contain high-cardinality or sensitive value %q", forbidden)
		}
	}
}
