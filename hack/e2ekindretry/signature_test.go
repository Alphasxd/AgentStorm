package e2ekindretry_test

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"k8s.io/apimachinery/pkg/util/wait"
)

// kubectl rollout status (Kubernetes v1.33) returns wait.ErrWaitTimeout from
// watch.UntilWithoutRetry when --timeout expires. cmdutil.CheckErr prints that
// error with an "error: " prefix and no resource suffix. kubectl wait appends
// " on <resource>/<name>". hack/e2e-kind-retry.sh greps the rollout line exactly.
func TestRolloutStatusTimeoutMessage(t *testing.T) {
	const want = "timed out waiting for the condition"
	if got := wait.ErrWaitTimeout.Error(); got != want {
		t.Fatalf("wait.ErrWaitTimeout = %q, want %q", got, want)
	}
	printed := "error: " + wait.ErrWaitTimeout.Error()
	if printed != "error: timed out waiting for the condition" {
		t.Fatalf("rollout status stderr = %q", printed)
	}
}

func TestKindLockFlakeRetrySignature(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	script := filepath.Join(filepath.Dir(filename), "..", "e2e-kind-retry_test.sh")
	command := exec.Command("bash", script)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("kind lock retry signature tests: %v\n%s", err, output)
	}
}
