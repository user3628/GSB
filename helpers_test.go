package convergence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "convergence.db")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	service, _, err := OpenService(ctx, ServiceOptions{DBPath: dbPath})
	if err != nil {
		t.Fatalf("OpenService: %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func commitSample(t *testing.T, service *Service) *CommitResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := service.CommitFull(ctx, SampleSnapshotV1())
	if err != nil {
		t.Fatalf("CommitFull sample: %v", err)
	}
	return result
}

func waitCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fn()
}
