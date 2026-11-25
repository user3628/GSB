package schedule

import (
	"context"
	"errors"
	"testing"
	"time"

	"auditdrift/internal/model"
)

func TestBoundedQueueFull(t *testing.T) {
	s := New(context.Background(), Config{MaxConcurrency: 1, QueueSize: 1, RepairQuota: 100})
	block := make(chan struct{})
	t1 := Task{Key: "t1", Host: "h", BaselineID: "b1", Weight: 9, Execute: func(context.Context) (Outcome, error) {
		<-block
		return Outcome{}, nil
	}}
	if err := s.Submit(t1); err != nil {
		t.Fatal(err)
	}
	if err := s.Submit(Task{Key: "t2", Host: "h", BaselineID: "b2", Weight: 8, Execute: func(context.Context) (Outcome, error) { return Outcome{}, nil }}); err != nil {
		t.Fatal(err)
	}
	err := s.Submit(Task{Key: "t3", Host: "h", BaselineID: "b3", Weight: 7, Execute: func(context.Context) (Outcome, error) { return Outcome{}, nil }})
	if model.CodeOf(err) != model.ErrAuditQueueFull {
		t.Fatalf("expected queue full, got %v", err)
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(block)
	}()
	report := s.Wait()
	if len(report.Results) != 2 {
		t.Fatalf("results=%d want 2", len(report.Results))
	}
}

func TestQuotaDegradationIsPriorityDeterministic(t *testing.T) {
	tasks := make([]Task, 0)
	for _, tc := range []struct {
		key    string
		host   string
		weight int
	}{
		{"low-h1", "host-a", 1}, {"w9-a", "host-a", 9}, {"w9-b", "host-b", 9}, {"w9-c", "host-c", 9}, {"w8-b", "host-b", 8},
	} {
		tc := tc
		tasks = append(tasks, Task{Key: tc.key, Host: tc.host, BaselineID: tc.key, Weight: tc.weight, Execute: func(context.Context) (Outcome, error) {
			return Outcome{RepairNeeded: true}, nil
		}})
	}
	report, err := RunRound(context.Background(), Config{MaxConcurrency: 2, QueueSize: 10, RepairQuota: 20, RoundBudget: time.Second}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	byKey := make(map[string]Result)
	for _, result := range report.Results {
		byKey[result.Task.Key] = result
	}
	if !byKey["w9-a"].QuotaCharged || !byKey["w9-b"].QuotaCharged {
		t.Fatalf("first two priority repairs should be charged: %+v %+v", byKey["w9-a"], byKey["w9-b"])
	}
	if byKey["w9-c"].Status != model.TaskDeferred {
		t.Fatalf("third weight-9 task status=%s", byKey["w9-c"].Status)
	}
	if byKey["w8-b"].Status != model.TaskDeferred || byKey["low-h1"].Status != model.TaskDeferred {
		t.Fatalf("later repair tasks must be deferred: w8=%s low=%s", byKey["w8-b"].Status, byKey["low-h1"].Status)
	}
	if len(report.Degradations) != 3 {
		t.Fatalf("degradations=%d want 3: %+v", len(report.Degradations), report.Degradations)
	}
	if report.QuotaRemaining != 2 || report.QuotaConsumed != 18 {
		t.Fatalf("quota consumed=%d remaining=%d", report.QuotaConsumed, report.QuotaRemaining)
	}
	if report.Degradations[0].RemainingQuota != 2 {
		t.Fatalf("degradation basis should include remaining quota: %+v", report.Degradations[0])
	}
}

func TestRoundBudgetDefersQueuedAndRunning(t *testing.T) {
	tasks := make([]Task, 0, 5)
	for i := 0; i < 5; i++ {
		tasks = append(tasks, Task{
			Key: "t", Host: "h", BaselineID: string(rune('a' + i)), Weight: 5,
			Execute: func(ctx context.Context) (Outcome, error) {
				<-ctx.Done()
				return Outcome{}, ctx.Err()
			},
		})
	}
	start := time.Now()
	report, err := RunRound(context.Background(), Config{MaxConcurrency: 2, QueueSize: 3, RepairQuota: 100, RoundBudget: 80 * time.Millisecond}, tasks)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("round did not respect budget, elapsed=%s", elapsed)
	}
	deferred := 0
	for _, result := range report.Results {
		if result.Status == model.TaskDeferred && result.DegradationReason == "round_budget_exhausted" {
			deferred++
		}
	}
	if deferred != 5 {
		t.Fatalf("budget deferred=%d report=%+v", deferred, report.Results)
	}
	if report.PeakActive != 2 {
		t.Fatalf("peak active=%d want 2", report.PeakActive)
	}
}
