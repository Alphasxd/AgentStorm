package results

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$`)

type Service struct {
	repository Repository
	objects    ObjectStore
}

func NewService(repository Repository, objects ObjectStore) *Service {
	return &Service{repository: repository, objects: objects}
}

func (s *Service) Ready(ctx context.Context) error {
	if err := s.repository.Ready(ctx); err != nil {
		return fmt.Errorf("database: %w", err)
	}
	if err := s.objects.Ready(ctx); err != nil {
		return fmt.Errorf("object store: %w", err)
	}
	return nil
}

func (s *Service) RegisterRun(ctx context.Context, runID, idempotencyKey string, registration Registration) (bool, error) {
	if err := validateRegistration(runID, idempotencyKey, registration); err != nil {
		return false, err
	}
	payloadHash, err := contentHash(registration)
	if err != nil {
		return false, fmt.Errorf("hash registration: %w", err)
	}
	return s.repository.RegisterRun(ctx, runID, idempotencyKey, payloadHash, registration)
}

func (s *Service) UploadShard(ctx context.Context, runID string, shardIndex int, idempotencyKey string, upload ShardUpload) (bool, error) {
	if err := validateShard(runID, shardIndex, idempotencyKey, upload); err != nil {
		return false, err
	}

	payload, err := json.Marshal(upload)
	if err != nil {
		return false, fmt.Errorf("marshal shard: %w", err)
	}
	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])
	objectKey := fmt.Sprintf("runs/%s/shards/%d/%s.json.gz", url.PathEscape(runID), shardIndex, payloadHash)

	reservation, err := s.repository.ReserveShard(ctx, runID, shardIndex, idempotencyKey, payloadHash, objectKey, upload.Summary)
	if err != nil {
		return false, err
	}
	if reservation.AlreadyComplete {
		return false, nil
	}

	compressed, err := gzipJSON(payload)
	if err != nil {
		return false, fmt.Errorf("compress shard: %w", err)
	}
	if err := s.objects.Put(ctx, objectKey, compressed, "application/json", "gzip"); err != nil {
		return false, fmt.Errorf("store raw shard: %w", err)
	}
	return s.repository.FinalizeShard(ctx, runID, shardIndex, payloadHash, upload.Cases)
}

func (s *Service) GetRun(ctx context.Context, runID string) (RunDetail, error) {
	if err := validateRunID(runID); err != nil {
		return RunDetail{}, err
	}
	return s.repository.GetRun(ctx, runID)
}

func (s *Service) ListCases(ctx context.Context, runID, cursor string, limit int, failedOnly bool) (CasePage, error) {
	if err := validateRunID(runID); err != nil {
		return CasePage{}, err
	}
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 500 {
		return CasePage{}, validationError("limit must be between 1 and 500")
	}
	return s.repository.ListCases(ctx, runID, cursor, limit, failedOnly)
}

func (s *Service) Compare(ctx context.Context, baselineID, candidateID string) (Comparison, error) {
	if err := validateRunID(baselineID); err != nil {
		return Comparison{}, validationErrorf("invalid baseline: %s", err)
	}
	if err := validateRunID(candidateID); err != nil {
		return Comparison{}, validationErrorf("invalid candidate: %s", err)
	}
	return s.repository.Compare(ctx, baselineID, candidateID)
}

func validateRegistration(runID, idempotencyKey string, registration Registration) error {
	if err := validateRunID(runID); err != nil {
		return err
	}
	if idempotencyKey != "run/"+runID {
		return validationError("invalid registration idempotency key")
	}
	if registration.SchemaVersion != SchemaVersion {
		return validationErrorf("schema_version must be %q", SchemaVersion)
	}
	if registration.ExpectedShards < 1 || registration.ExpectedShards > 10000 {
		return validationError("expected_shards must be between 1 and 10000")
	}
	if strings.TrimSpace(registration.Source.Namespace) == "" || strings.TrimSpace(registration.Source.Name) == "" {
		return validationError("source namespace and name are required")
	}
	if invalidText(registration.Source.Namespace, 253) || invalidText(registration.Source.Name, 253) {
		return validationError("source namespace and name must be valid text no longer than 253 bytes")
	}
	if strings.TrimSpace(registration.Target.Provider) == "" {
		return validationError("target provider is required")
	}
	if invalidText(registration.Target.Provider, 128) || invalidText(registration.Target.Model, 512) {
		return validationError("target fields exceed their size limit or contain NUL")
	}
	if strings.TrimSpace(registration.Dataset.Name) == "" || strings.TrimSpace(registration.Dataset.Key) == "" {
		return validationError("dataset name and key are required")
	}
	if invalidText(registration.Dataset.Name, 253) || invalidText(registration.Dataset.Key, 1024) {
		return validationError("dataset fields exceed their size limit or contain NUL")
	}
	for name, value := range map[string]*float64{
		"min_success_rate": registration.Evaluation.MinSuccessRate,
		"max_failure_rate": registration.Evaluation.MaxFailureRate,
	} {
		if value != nil && (!validMetric(*value) || *value < 0 || *value > 1) {
			return validationErrorf("%s must be between 0 and 1", name)
		}
	}
	if registration.Evaluation.MaxP95MS != nil && (!validMetric(*registration.Evaluation.MaxP95MS) || *registration.Evaluation.MaxP95MS < 0) {
		return validationError("max_p95_ms must not be negative")
	}
	return nil
}

func validateShard(runID string, shardIndex int, idempotencyKey string, upload ShardUpload) error {
	if err := validateRunID(runID); err != nil {
		return err
	}
	if shardIndex < 0 {
		return validationError("shard index must not be negative")
	}
	if idempotencyKey != fmt.Sprintf("run/%s/shard/%d", runID, shardIndex) {
		return validationError("invalid shard idempotency key")
	}
	if upload.SchemaVersion != SchemaVersion {
		return validationErrorf("schema_version must be %q", SchemaVersion)
	}
	if upload.Summary.Total != len(upload.Cases) {
		return validationError("summary total does not match case count")
	}
	if upload.Summary.Succeeded+upload.Summary.Failed != upload.Summary.Total {
		return validationError("summary succeeded and failed counts do not match total")
	}
	if upload.Summary.Total < 0 || upload.Summary.Succeeded < 0 || upload.Summary.Failed < 0 ||
		!validMetric(upload.Summary.DurationMS) || upload.Summary.DurationMS < 0 ||
		upload.Summary.InputTokens < 0 || upload.Summary.OutputTokens < 0 {
		return validationError("summary contains a negative or non-finite metric")
	}
	if len(upload.Cases) > 100000 {
		return validationError("a shard cannot contain more than 100000 cases")
	}
	seen := make(map[string]struct{}, len(upload.Cases))
	for index, item := range upload.Cases {
		if strings.TrimSpace(item.CaseID) == "" {
			return validationErrorf("cases[%d].case_id is required", index)
		}
		if invalidText(item.CaseID, 1024) {
			return validationErrorf("cases[%d].case_id exceeds 1024 bytes or contains NUL", index)
		}
		if item.Iteration < 0 {
			return validationErrorf("cases[%d].iteration must not be negative", index)
		}
		expected := fmt.Sprintf("run/%s/case/%s/iteration/%d", runID, url.QueryEscape(item.CaseID), item.Iteration)
		if item.IdempotencyKey != expected {
			return validationErrorf("cases[%d].idempotency_key is invalid", index)
		}
		identity := fmt.Sprintf("%s\x00%d", item.CaseID, item.Iteration)
		if _, ok := seen[identity]; ok {
			return validationErrorf("duplicate case %q iteration %d", item.CaseID, item.Iteration)
		}
		seen[identity] = struct{}{}
		if !validMetric(item.LatencyMS) || item.LatencyMS < 0 || item.InputTokens < 0 || item.OutputTokens < 0 {
			return validationErrorf("cases[%d] contains a negative metric", index)
		}
		if invalidText(item.FailureKind, 128) ||
			(item.Output != nil && invalidText(*item.Output, maxRequestBytes)) ||
			(item.Error != nil && invalidText(*item.Error, maxRequestBytes)) {
			return validationErrorf("cases[%d] contains invalid text", index)
		}
		if !item.Success && strings.TrimSpace(item.FailureKind) == "" {
			return validationErrorf("cases[%d].failure_kind is required for failed results", index)
		}
	}
	return nil
}

func validateRunID(runID string) error {
	if !runIDPattern.MatchString(runID) {
		return validationError("run ID must use 1-200 letters, digits, dots, underscores, or hyphens")
	}
	return nil
}

func invalidText(value string, maxBytes int64) bool {
	return int64(len(value)) > maxBytes || strings.ContainsRune(value, '\x00')
}

func validMetric(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func contentHash(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func gzipJSON(payload []byte) ([]byte, error) {
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(payload); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
