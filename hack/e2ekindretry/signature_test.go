package e2ekindretry_test

import (
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

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
