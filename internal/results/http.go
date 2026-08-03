package results

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const maxRequestBytes int64 = 32 << 20

type HTTPHandler struct {
	service    *Service
	writeToken [sha256.Size]byte
	readToken  [sha256.Size]byte
	mux        *http.ServeMux
	metrics    *resultMetrics
	metricsAPI http.Handler
}

type HTTPConfig struct {
	WriteToken string
	ReadToken  string
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHTTPHandler(service *Service, config HTTPConfig) (*HTTPHandler, error) {
	if config.WriteToken == "" || config.ReadToken == "" {
		return nil, errors.New("read and write bearer tokens are required")
	}
	metrics, registry := newResultMetrics()
	handler := &HTTPHandler{
		service:    service,
		writeToken: sha256.Sum256([]byte(config.WriteToken)),
		readToken:  sha256.Sum256([]byte(config.ReadToken)),
		mux:        http.NewServeMux(),
		metrics:    metrics,
		metricsAPI: promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
	}
	handler.routes()
	return handler, nil
}

func (h *HTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	started := time.Now()
	recorder := &statusWriter{ResponseWriter: writer}
	h.mux.ServeHTTP(recorder, request)
	h.metrics.observeHTTP(request.Method, request.Pattern, recorder.statusCode(), time.Since(started))
}

func (h *HTTPHandler) routes() {
	h.mux.HandleFunc("GET /healthz", h.health)
	h.mux.HandleFunc("GET /readyz", h.ready)
	h.mux.Handle("GET /metrics", h.metricsAPI)
	h.mux.HandleFunc("PUT /v1/runs/{runID}", h.requireWrite(h.registerRun))
	h.mux.HandleFunc("PUT /v1/runs/{runID}/terminal", h.requireWrite(h.terminateRun))
	h.mux.HandleFunc("PUT /v1/runs/{runID}/shards/{index}", h.requireWrite(h.uploadShard))
	h.mux.HandleFunc("GET /v1/runs/{runID}/queue", h.queueStatus)
	h.mux.HandleFunc("POST /v1/runs/{runID}/queue/claims", h.requireWrite(h.claimShard))
	h.mux.HandleFunc("POST /v1/runs/{runID}/queue/shards/{index}/renew", h.requireWrite(h.renewShardLease))
	h.mux.HandleFunc("POST /v1/runs/{runID}/permits", h.requireWrite(h.acquirePermit))
	h.mux.HandleFunc("POST /v1/runs/{runID}/permits/{permitID}/renew", h.requireWrite(h.renewPermit))
	h.mux.HandleFunc("POST /v1/runs/{runID}/permits/{permitID}/release", h.requireWrite(h.releasePermit))
	h.mux.HandleFunc("GET /v1/runs/{runID}", h.requireRead(h.getRun))
	h.mux.HandleFunc("GET /v1/runs/{runID}/cases", h.requireRead(h.listCases))
	h.mux.HandleFunc("GET /v1/comparisons", h.requireRead(h.compare))
}

func (h *HTTPHandler) terminateRun(writer http.ResponseWriter, request *http.Request) {
	var terminal TerminalRequest
	if err := decodeJSON(writer, request, &terminal); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	result, err := h.service.TerminateRun(
		request.Context(), request.PathValue("runID"), request.Header.Get("Idempotency-Key"), terminal,
	)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, map[string]any{
		"run_id": request.PathValue("runID"), "status": terminal.Status, "created": result.Created,
	})
}

func (h *HTTPHandler) requireWrite(next http.HandlerFunc) http.HandlerFunc {
	return h.authorize(h.writeToken, next)
}

func (h *HTTPHandler) requireRead(next http.HandlerFunc) http.HandlerFunc {
	return h.authorize(h.readToken, next)
}

func (h *HTTPHandler) authorize(expected [sha256.Size]byte, next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		token, ok := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
		actual := sha256.Sum256([]byte(token))
		if !ok || token == "" || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
			writeError(writer, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			return
		}
		next(writer, request)
	}
}

func (h *HTTPHandler) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *HTTPHandler) ready(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := contextWithTimeout(request, 3*time.Second)
	defer cancel()
	if err := h.service.Ready(ctx); err != nil {
		writeError(writer, http.StatusServiceUnavailable, "not_ready", "dependencies are not ready")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *HTTPHandler) registerRun(writer http.ResponseWriter, request *http.Request) {
	var registration Registration
	if err := decodeJSON(writer, request, &registration); err != nil {
		h.metrics.observeRegistration(false, validationError(err.Error()))
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	created, err := h.service.RegisterRun(request.Context(), request.PathValue("runID"), request.Header.Get("Idempotency-Key"), registration)
	h.metrics.observeRegistration(created, err)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, map[string]any{"id": request.PathValue("runID"), "created": created})
}

func (h *HTTPHandler) uploadShard(writer http.ResponseWriter, request *http.Request) {
	index, err := strconv.Atoi(request.PathValue("index"))
	if err != nil {
		h.metrics.observeShard(ShardResult{}, ShardUpload{}, validationError("shard index must be an integer"))
		writeError(writer, http.StatusBadRequest, "invalid_request", "shard index must be an integer")
		return
	}
	var upload ShardUpload
	if err := decodeJSON(writer, request, &upload); err != nil {
		h.metrics.observeShard(ShardResult{}, ShardUpload{}, validationError(err.Error()))
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	result, err := h.service.UploadShardWithLease(
		request.Context(), request.PathValue("runID"), index,
		request.Header.Get("Idempotency-Key"), request.Header.Get("X-AgentStorm-Queue-Lease"), upload,
	)
	h.metrics.observeShard(result, upload, err)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, map[string]any{"run_id": request.PathValue("runID"), "shard_index": index, "created": result.Created})
}

func (h *HTTPHandler) queueStatus(writer http.ResponseWriter, request *http.Request) {
	status, err := h.service.QueueStatus(request.Context(), request.PathValue("runID"))
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, status)
}

func (h *HTTPHandler) claimShard(writer http.ResponseWriter, request *http.Request) {
	var claimRequest QueueClaimRequest
	if err := decodeJSON(writer, request, &claimRequest); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	claim, err := h.service.ClaimShard(request.Context(), request.PathValue("runID"), claimRequest)
	h.metrics.observeQueueClaim(err)
	if errors.Is(err, ErrQueueEmpty) {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, claim)
}

func (h *HTTPHandler) renewShardLease(writer http.ResponseWriter, request *http.Request) {
	index, err := strconv.Atoi(request.PathValue("index"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "shard index must be an integer")
		return
	}
	var renewRequest QueueRenewRequest
	if err := decodeJSON(writer, request, &renewRequest); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	lease, err := h.service.RenewShardLease(request.Context(), request.PathValue("runID"), index, renewRequest)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, lease)
}

func (h *HTTPHandler) acquirePermit(writer http.ResponseWriter, request *http.Request) {
	var permitRequest PermitRequest
	if err := decodeJSON(writer, request, &permitRequest); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	grant, err := h.service.AcquirePermit(request.Context(), request.PathValue("runID"), permitRequest)
	h.metrics.observePermit(err)
	if err != nil {
		var capacity *CapacityError
		if errors.As(err, &capacity) {
			writer.Header().Set("Retry-After", strconv.FormatInt(max(1, int64(capacity.RetryAfter/time.Second)), 10))
			writeJSON(writer, http.StatusTooManyRequests, map[string]any{
				"code": "scheduler_busy", "message": "distributed limit is currently exhausted",
				"retry_after_ms": capacity.RetryAfter.Milliseconds(),
			})
			return
		}
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, grant)
}

func (h *HTTPHandler) renewPermit(writer http.ResponseWriter, request *http.Request) {
	h.permitLeaseAction(writer, request, true)
}

func (h *HTTPHandler) releasePermit(writer http.ResponseWriter, request *http.Request) {
	h.permitLeaseAction(writer, request, false)
}

func (h *HTTPHandler) permitLeaseAction(writer http.ResponseWriter, request *http.Request, renew bool) {
	var leaseRequest PermitLeaseRequest
	if err := decodeJSON(writer, request, &leaseRequest); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if renew {
		lease, err := h.service.RenewPermit(
			request.Context(), request.PathValue("runID"), request.PathValue("permitID"), leaseRequest,
		)
		if err != nil {
			writeServiceError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, lease)
		return
	}
	if err := h.service.ReleasePermit(
		request.Context(), request.PathValue("runID"), request.PathValue("permitID"), leaseRequest,
	); err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]bool{"released": true})
}

func (h *HTTPHandler) getRun(writer http.ResponseWriter, request *http.Request) {
	detail, err := h.service.GetRun(request.Context(), request.PathValue("runID"))
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, detail)
}

func (h *HTTPHandler) listCases(writer http.ResponseWriter, request *http.Request) {
	limit := 0
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "invalid_request", "limit must be an integer")
			return
		}
		limit = parsed
	}
	failedOnly, err := strconv.ParseBool(defaultString(request.URL.Query().Get("failed"), "false"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid_request", "failed must be true or false")
		return
	}
	page, err := h.service.ListCases(request.Context(), request.PathValue("runID"), request.URL.Query().Get("cursor"), limit, failedOnly)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (h *HTTPHandler) compare(writer http.ResponseWriter, request *http.Request) {
	comparison, err := h.service.Compare(request.Context(), request.URL.Query().Get("baseline"), request.URL.Query().Get("candidate"))
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, comparison)
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, destination any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return fmt.Errorf("request body exceeds %d bytes", maxRequestBytes)
		}
		return fmt.Errorf("decode JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeServiceError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(writer, http.StatusNotFound, "not_found", "requested result was not found")
	case errors.Is(err, ErrConflict):
		writeError(writer, http.StatusConflict, "idempotency_conflict", "idempotency key was reused with different content")
	case errors.Is(err, ErrNotReady):
		writeError(writer, http.StatusConflict, "run_not_complete", "both runs must be complete")
	case errors.Is(err, ErrLeaseLost):
		writeError(writer, http.StatusConflict, "lease_lost", "scheduler lease is no longer valid")
	default:
		var validation *ValidationError
		if errors.As(err, &validation) {
			writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(writer, http.StatusInternalServerError, "internal_error", "request could not be completed")
	}
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, apiError{Code: code, Message: message})
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
