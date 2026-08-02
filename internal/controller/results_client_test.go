package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Alphasxd/AgentStorm/internal/results"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestHTTPResultWriterUsesBearerAndIdempotencyWithoutLeakingToken(t *testing.T) {
	token := []byte("result-test-only-value")
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPut || request.URL.Path != "/v1/runs/run-1/terminal" ||
			request.Header.Get("Authorization") != "Bearer "+string(token) ||
			request.Header.Get("Idempotency-Key") != "run/run-1/terminal/cancelled" {
			t.Fatalf("unexpected request: %s %s %#v", request.Method, request.URL.Path, request.Header)
		}
		var terminal results.TerminalRequest
		if err := json.NewDecoder(request.Body).Decode(&terminal); err != nil {
			t.Fatal(err)
		}
		if terminal.Status != results.RunCancelled || terminal.ReasonCode != "user_requested" {
			t.Fatalf("terminal = %#v", terminal)
		}
		return &http.Response{
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(strings.NewReader(`{"created":true}`)),
			Header:     make(http.Header),
		}, nil
	})}

	resultWriter := &HTTPResultWriter{baseURL: "http://results.example", client: client}
	if err := resultWriter.Terminal(context.Background(), "run-1", token, results.TerminalRequest{
		Status: results.RunCancelled, ReasonCode: "user_requested",
	}); err != nil {
		t.Fatal(err)
	}
}
