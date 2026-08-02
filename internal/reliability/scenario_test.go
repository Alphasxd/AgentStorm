package reliability

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseScenarioNormalizesAndHashes(t *testing.T) {
	payload := []byte(`{
  "apiVersion":"agentstorm.io/v1alpha1",
  "kind":"FaultScenario",
  "rules":[{"name":"limit","fault":"rate_limit","probability":0.25,"caseIDs":["a"]}]
}`)
	scenario, canonical, digest, err := ParseScenario(payload)
	if err != nil {
		t.Fatalf("ParseScenario: %v", err)
	}
	if got := scenario.Rules[0].Attempts; len(got) != 1 || got[0] != 1 {
		t.Fatalf("default attempts = %v, want [1]", got)
	}
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		t.Fatalf("digest = %q", digest)
	}
	var normalized map[string]any
	if err := json.Unmarshal(canonical, &normalized); err != nil {
		t.Fatalf("canonical JSON: %v", err)
	}
	_, secondCanonical, secondDigest, err := ParseScenario(canonical)
	if err != nil || string(canonical) != string(secondCanonical) || digest != secondDigest {
		t.Fatalf("normalization is not stable: %v %q %q", err, digest, secondDigest)
	}
}

func TestParseScenarioRejectsInvalidDocuments(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		match   string
	}{
		{name: "unknown field", payload: `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","unknown":true,"rules":[]}`, match: "unknown field"},
		{name: "duplicate rule", payload: `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"same","fault":"rate_limit","probability":1},{"name":"same","fault":"timeout","probability":1,"delayMs":1}]}`, match: "duplicate rule"},
		{name: "invalid probability", payload: `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"bad","fault":"rate_limit","probability":1.1}]}`, match: "probability"},
		{name: "missing delay", payload: `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"bad","fault":"timeout","probability":1}]}`, match: "delayMs"},
		{name: "invalid status", payload: `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"bad","fault":"http_error","probability":1,"statusCode":200}]}`, match: "statusCode"},
		{name: "negative selector", payload: `{"apiVersion":"agentstorm.io/v1alpha1","kind":"FaultScenario","rules":[{"name":"bad","fault":"rate_limit","probability":1,"iterations":[-1]}]}`, match: "non-negative"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := ParseScenario([]byte(test.payload))
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("error = %v, want containing %q", err, test.match)
			}
		})
	}
}

func TestParseScenarioRejectsOversizeDocument(t *testing.T) {
	_, _, _, err := ParseScenario(make([]byte, MaxScenarioBytes+1))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want size rejection", err)
	}
}
