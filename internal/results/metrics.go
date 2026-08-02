package results

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type resultMetrics struct {
	httpRequests     *prometheus.CounterVec
	httpDuration     *prometheus.HistogramVec
	runRegistrations *prometheus.CounterVec
	shardUploads     *prometheus.CounterVec
	cases            *prometheus.CounterVec
	tokens           *prometheus.CounterVec
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
		tokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "agentstorm",
			Subsystem: "result_api",
			Name:      "tokens_total",
			Help:      "Total tokens from uniquely persisted shards by direction.",
		}, []string{"direction"}),
	}
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		metrics.httpRequests,
		metrics.httpDuration,
		metrics.runRegistrations,
		metrics.shardUploads,
		metrics.cases,
		metrics.tokens,
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

func (m *resultMetrics) observeShard(created bool, upload ShardUpload, err error) {
	m.shardUploads.WithLabelValues(operationOutcome(created, err)).Inc()
	if err != nil || !created {
		return
	}
	for _, item := range upload.Cases {
		result := "success"
		failureKind := "none"
		if !item.Success {
			result = "failure"
			failureKind = boundedFailureKind(item.FailureKind)
		}
		m.cases.WithLabelValues(result, failureKind).Inc()
	}
	m.tokens.WithLabelValues("input").Add(float64(upload.Summary.InputTokens))
	m.tokens.WithLabelValues("output").Add(float64(upload.Summary.OutputTokens))
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
