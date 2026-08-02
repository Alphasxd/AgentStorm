package reliability

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	ScenarioAPIVersion = "agentstorm.io/v1alpha1"
	ScenarioKind       = "FaultScenario"
	MaxScenarioBytes   = 64 * 1024
	MaxRules           = 100
)

var supportedFaults = map[string]struct{}{
	"latency":            {},
	"timeout":            {},
	"http_error":         {},
	"malformed_response": {},
	"rate_limit":         {},
	"tool_error":         {},
}

// FaultScenario is the normalized, immutable fault document copied into worker configuration.
type FaultScenario struct {
	APIVersion string      `json:"apiVersion"`
	Kind       string      `json:"kind"`
	Rules      []FaultRule `json:"rules"`
}

type FaultRule struct {
	Name        string   `json:"name"`
	Fault       string   `json:"fault"`
	Probability *float64 `json:"probability"`
	CaseIDs     []string `json:"caseIDs,omitempty"`
	Iterations  []int64  `json:"iterations,omitempty"`
	Attempts    []int32  `json:"attempts,omitempty"`
	DelayMS     *int64   `json:"delayMs,omitempty"`
	StatusCode  *int     `json:"statusCode,omitempty"`
	ToolName    string   `json:"toolName,omitempty"`
}

type Snapshot struct {
	SourceName string        `json:"sourceName"`
	SourceKey  string        `json:"sourceKey"`
	Digest     string        `json:"digest"`
	Scenario   FaultScenario `json:"scenario"`
}

// ParseScenario rejects unknown fields, validates the fixed schema, and returns canonical JSON.
func ParseScenario(payload []byte) (FaultScenario, []byte, string, error) {
	if len(payload) > MaxScenarioBytes {
		return FaultScenario{}, nil, "", fmt.Errorf("scenario exceeds %d bytes", MaxScenarioBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var scenario FaultScenario
	if err := decoder.Decode(&scenario); err != nil {
		return FaultScenario{}, nil, "", fmt.Errorf("decode FaultScenario: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return FaultScenario{}, nil, "", err
	}
	if err := validateScenario(&scenario); err != nil {
		return FaultScenario{}, nil, "", err
	}
	canonical, err := json.Marshal(scenario)
	if err != nil {
		return FaultScenario{}, nil, "", fmt.Errorf("marshal normalized FaultScenario: %w", err)
	}
	digestBytes := sha256.Sum256(canonical)
	return scenario, canonical, "sha256:" + hex.EncodeToString(digestBytes[:]), nil
}

func requireEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("FaultScenario must contain exactly one JSON document")
		}
		return fmt.Errorf("decode trailing FaultScenario data: %w", err)
	}
	return nil
}

func validateScenario(scenario *FaultScenario) error {
	if scenario.APIVersion != ScenarioAPIVersion || scenario.Kind != ScenarioKind {
		return fmt.Errorf("FaultScenario requires apiVersion %q and kind %q", ScenarioAPIVersion, ScenarioKind)
	}
	if len(scenario.Rules) > MaxRules {
		return fmt.Errorf("FaultScenario cannot contain more than %d rules", MaxRules)
	}
	names := make(map[string]struct{}, len(scenario.Rules))
	for index := range scenario.Rules {
		rule := &scenario.Rules[index]
		if err := validateRule(rule); err != nil {
			return fmt.Errorf("rules[%d]: %w", index, err)
		}
		if _, exists := names[rule.Name]; exists {
			return fmt.Errorf("rules[%d]: duplicate rule name %q", index, rule.Name)
		}
		names[rule.Name] = struct{}{}
	}
	return nil
}

func validateRule(rule *FaultRule) error {
	if strings.TrimSpace(rule.Name) == "" || len([]byte(rule.Name)) > 128 {
		return fmt.Errorf("name must be a non-empty string of at most 128 bytes")
	}
	if _, supported := supportedFaults[rule.Fault]; !supported {
		return fmt.Errorf("unsupported fault %q", rule.Fault)
	}
	if rule.Probability == nil || *rule.Probability < 0 || *rule.Probability > 1 {
		return fmt.Errorf("probability must be between 0 and 1")
	}
	if err := validateSelectors(rule); err != nil {
		return err
	}
	if len(rule.Attempts) == 0 {
		rule.Attempts = []int32{1}
	}
	switch rule.Fault {
	case "latency", "timeout":
		if rule.DelayMS == nil || *rule.DelayMS < 0 {
			return fmt.Errorf("%s requires a non-negative delayMs", rule.Fault)
		}
		if rule.StatusCode != nil || rule.ToolName != "" {
			return fmt.Errorf("%s does not accept statusCode or toolName", rule.Fault)
		}
	case "http_error":
		if rule.StatusCode == nil || *rule.StatusCode < http.StatusBadRequest || *rule.StatusCode > 599 {
			return fmt.Errorf("http_error requires statusCode between 400 and 599")
		}
		if rule.DelayMS != nil || rule.ToolName != "" {
			return fmt.Errorf("http_error does not accept delayMs or toolName")
		}
	case "tool_error":
		if len([]byte(rule.ToolName)) > 512 {
			return fmt.Errorf("toolName must be at most 512 bytes")
		}
		if rule.DelayMS != nil || rule.StatusCode != nil {
			return fmt.Errorf("tool_error does not accept delayMs or statusCode")
		}
	default:
		if rule.DelayMS != nil || rule.StatusCode != nil || rule.ToolName != "" {
			return fmt.Errorf("%s does not accept delayMs, statusCode, or toolName", rule.Fault)
		}
	}
	return nil
}

func validateSelectors(rule *FaultRule) error {
	seenCases := map[string]struct{}{}
	for _, caseID := range rule.CaseIDs {
		if strings.TrimSpace(caseID) == "" || len([]byte(caseID)) > 512 {
			return fmt.Errorf("caseIDs must contain non-empty strings of at most 512 bytes")
		}
		if _, exists := seenCases[caseID]; exists {
			return fmt.Errorf("caseIDs contains duplicate %q", caseID)
		}
		seenCases[caseID] = struct{}{}
	}
	seenIterations := map[int64]struct{}{}
	for _, iteration := range rule.Iterations {
		if iteration < 0 {
			return fmt.Errorf("iterations must be non-negative")
		}
		if _, exists := seenIterations[iteration]; exists {
			return fmt.Errorf("iterations contains duplicate %d", iteration)
		}
		seenIterations[iteration] = struct{}{}
	}
	seenAttempts := map[int32]struct{}{}
	for _, attempt := range rule.Attempts {
		if attempt < 1 || attempt > 10 {
			return fmt.Errorf("attempts must be between 1 and 10")
		}
		if _, exists := seenAttempts[attempt]; exists {
			return fmt.Errorf("attempts contains duplicate %d", attempt)
		}
		seenAttempts[attempt] = struct{}{}
	}
	return nil
}
