package results

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type resultMetrics struct {
	httpRequests     *prometheus.CounterVec
	httpDuration     *prometheus.HistogramVec
	runRegistrations *prometheus.CounterVec
	shardUploads     *prometheus.CounterVec
	cases            *prometheus.CounterVec
	caseFailures     *prometheus.CounterVec
	attempts         *prometheus.CounterVec
	retryDecisions   *prometheus.CounterVec
	retries          *prometheus.CounterVec
	injectedFaults   *prometheus.CounterVec
	circuitEvents    *prometheus.CounterVec
	tokens           *prometheus.CounterVec
	costUSD          *prometheus.CounterVec
}

func newResultMetrics() (*resultMetrics, *prometheus.Registry) {
	metrics := &resultMetrics{
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "http_requests_total",
			Help:      "Total Result API HTTP requests by bounded route and status class.",
		}, []string{"method", "route", "status_class"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "http_request_duration_seconds",
			Help:      "Result API HTTP request duration in seconds by bounded route.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
		runRegistrations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "run_registrations_total",
			Help:      "Total run registration attempts by outcome.",
		}, []string{"outcome"}),
		shardUploads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "shard_uploads_total",
			Help:      "Total shard upload attempts by idempotent outcome.",
		}, []string{"outcome"}),
		cases: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "cases_total",
			Help:      "Total uniquely persisted cases by result and bounded failure category.",
		}, []string{"result", "failure_kind"}),
		caseFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "case_failures_total",
			Help:      "Total uniquely persisted failed cases by stable bounded M4 category.",
		}, []string{"category"}),
		attempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "attempts_total",
			Help:      "Total uniquely persisted structured attempts by bounded outcome.",
		}, []string{"outcome"}),
		retryDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "retry_decisions_total",
			Help:      "Total uniquely persisted retry decisions by bounded decision.",
		}, []string{"decision"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "retries_total",
			Help:      "Total uniquely persisted retry executions by bounded triggering decision and outcome.",
		}, []string{"decision", "outcome"}),
		injectedFaults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "injected_faults_total",
			Help:      "Total uniquely persisted injected faults by supported type.",
		}, []string{"fault"}),
		circuitEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "circuit_events_total",
			Help:      "Total uniquely persisted circuit-breaker events by bounded event.",
		}, []string{"event"}),
		tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "tokens_total",
			Help:      "Total tokens from uniquely persisted shards by direction.",
		}, []string{"direction"}),
		costUSD: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "cost_usd_total",
			Help:      "Total derived USD cost from uniquely persisted priced shards by direction.",
		}, []string{"direction"}),
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		metrics.httpRequests,
		metrics.httpDuration,
		metrics.runRegistrations,
		metrics.shardUploads,
		metrics.cases,
		metrics.caseFailures,
		metrics.attempts,
		metrics.retryDecisions,
		metrics.retries,
		metrics.injectedFaults,
		metrics.circuitEvents,
		metrics.tokens,
		metrics.costUSD,
	)
	return metrics, registry
}

func (m *resultMetrics) observeHTTP(method, route string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	statusClass := fmt.Sprintf("%dxx", status/100)
	method = boundedHTTPMethod(method)
	m.httpRequests.WithLabelValues(method, route, statusClass).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(duration.Seconds())
}

func boundedHTTPMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPut:
		return method
	default:
		return "other"
	}
}

func (m *resultMetrics) observeRegistration(created bool, err error) {
	m.runRegistrations.WithLabelValues(operationOutcome(created, err)).Inc()
}

func (m *resultMetrics) observeShard(result ShardResult, upload ShardUpload, err error) {
	m.shardUploads.WithLabelValues(operationOutcome(result.Created, err)).Inc()
	if err != nil || !result.Created {
		return
	}
	for _, item := range upload.Cases {
		result := "success"
		failureKind := "none"
		if !item.Success {
			result = "failure"
			failureKind = boundedFailureKind(item.FailureKind)
			m.caseFailures.WithLabelValues(boundedFailureCategory(item.FailureCategory)).Inc()
		}
		m.cases.WithLabelValues(result, failureKind).Inc()
		for index, attempt := range item.Attempts {
			m.attempts.WithLabelValues(boundedAttemptOutcome(attempt.Outcome)).Inc()
			m.retryDecisions.WithLabelValues(boundedRetryDecision(attempt.RetryDecision)).Inc()
			if index > 0 {
				m.retries.WithLabelValues(
					boundedRetryDecision(item.Attempts[index-1].RetryDecision),
					boundedAttemptOutcome(attempt.Outcome),
				).Inc()
			}
			if attempt.InjectedFault != "" {
				m.injectedFaults.WithLabelValues(boundedInjectedFault(attempt.InjectedFault)).Inc()
			}
			for _, event := range attempt.CircuitEvents {
				m.circuitEvents.WithLabelValues(boundedCircuitEvent(event)).Inc()
			}
		}
	}
	m.tokens.WithLabelValues("input").Add(float64(upload.Summary.InputTokens))
	m.tokens.WithLabelValues("output").Add(float64(upload.Summary.OutputTokens))
	if result.InputCostUSD != nil && result.OutputCostUSD != nil {
		inputCost, _ := strconv.ParseFloat(*result.InputCostUSD, 64)
		outputCost, _ := strconv.ParseFloat(*result.OutputCostUSD, 64)
		m.costUSD.WithLabelValues("input").Add(inputCost)
		m.costUSD.WithLabelValues("output").Add(outputCost)
	}
}

func operationOutcome(created bool, err error) string {
	if err == nil {
		if created {
			return "created"
		}
		return "duplicate"
	}
	var validation *ValidationError
	switch {
	case errors.As(err, &validation):
		return "invalid"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	default:
		return "error"
	}
}

func boundedFailureKind(value string) string {
	switch value {
	case "assertion", "provider", "timeout", "tool", "harness":
		return value
	default:
		return "other"
	}
}

func boundedFailureCategory(value string) string {
	switch value {
	case "evaluation", "provider", "tool", "harness":
		return value
	default:
		return "other"
	}
}

func boundedAttemptOutcome(value string) string {
	switch value {
	case "succeeded", "failed", "rejected", "cancelled":
		return value
	default:
		return "other"
	}
}

func boundedRetryDecision(value string) string {
	switch value {
	case "not_needed", "retry_safe", "retry_ambiguous", "ambiguous_blocked", "not_retryable",
		"half_open_no_retry", "attempt_limit", "backoff_budget", "time_budget":
		return value
	default:
		return "other"
	}
}

func boundedInjectedFault(value string) string {
	switch value {
	case "latency", "timeout", "http_error", "malformed_response", "rate_limit", "tool_error":
		return value
	default:
		return "other"
	}
}

func boundedCircuitEvent(value string) string {
	switch value {
	case "open", "reject", "half_open", "close":
		return value
	default:
		return "other"
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(payload []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(payload)
}

func (w *statusWriter) statusCode() int {
	if w.status == 0 {
		return 200
	}
	return w.status
}
