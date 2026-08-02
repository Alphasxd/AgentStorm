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
	h.mux.HandleFunc("PUT /v1/runs/{runID}/shards/{index}", h.requireWrite(h.uploadShard))
	h.mux.HandleFunc("GET /v1/runs/{runID}", h.requireRead(h.getRun))
	h.mux.HandleFunc("GET /v1/runs/{runID}/cases", h.requireRead(h.listCases))
	h.mux.HandleFunc("GET /v1/comparisons", h.requireRead(h.compare))
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
		h.metrics.observeShard(false, ShardUpload{}, validationError("shard index must be an integer"))
		writeError(writer, http.StatusBadRequest, "invalid_request", "shard index must be an integer")
		return
	}
	var upload ShardUpload
	if err := decodeJSON(writer, request, &upload); err != nil {
		h.metrics.observeShard(false, ShardUpload{}, validationError(err.Error()))
		writeError(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	created, err := h.service.UploadShard(request.Context(), request.PathValue("runID"), index, request.Header.Get("Idempotency-Key"), upload)
	h.metrics.observeShard(created, upload, err)
	if err != nil {
		writeServiceError(writer, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(writer, status, map[string]any{"run_id": request.PathValue("runID"), "shard_index": index, "created": created})
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
