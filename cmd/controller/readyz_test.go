package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

func TestLeaderReadyCheckerFailsUntilElectionCloses(t *testing.T) {
	elected := make(chan struct{})
	checker := leaderReadyChecker(elected)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	if err := checker(request); !errors.Is(err, errNotElectedLeader) {
		t.Fatalf("before election: got %v", err)
	}

	close(elected)
	if err := checker(request); err != nil {
		t.Fatalf("after election: %v", err)
	}
	if err := checker(request); err != nil {
		t.Fatalf("closed election channel must stay ready: %v", err)
	}
}

func TestLeaderReadyCheckerDoesNotBlockTheProbe(t *testing.T) {
	checker := leaderReadyChecker(make(chan struct{}))
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	started := time.Now()
	err := checker(request)
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("checker blocked for %s", elapsed)
	}
	if !errors.Is(err, errNotElectedLeader) {
		t.Fatalf("open channel: got %v", err)
	}
}

func TestLeaderReadyCheckerTreatsNilChannelAsUnelected(t *testing.T) {
	started := time.Now()
	err := leaderReadyChecker(nil)(httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("nil channel blocked for %s", elapsed)
	}
	if !errors.Is(err, errNotElectedLeader) {
		t.Fatalf("nil channel: got %v", err)
	}
}

func TestNonLeaderPassesLivenessAndFailsReadiness(t *testing.T) {
	elected := make(chan struct{})
	liveness := &healthz.Handler{Checks: map[string]healthz.Checker{"healthz": healthz.Ping}}
	readiness := &healthz.Handler{Checks: map[string]healthz.Checker{"leader": leaderReadyChecker(elected)}}

	live := httptest.NewRecorder()
	liveness.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/", nil))
	if live.Code != http.StatusOK {
		t.Fatalf("liveness status = %d, body %s", live.Code, live.Body.String())
	}

	ready := httptest.NewRecorder()
	readiness.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/", nil))
	if ready.Code != http.StatusInternalServerError {
		t.Fatalf("readiness before election = %d, body %s", ready.Code, ready.Body.String())
	}

	close(elected)
	ready = httptest.NewRecorder()
	readiness.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("readiness after election = %d, body %s", ready.Code, ready.Body.String())
	}
}
