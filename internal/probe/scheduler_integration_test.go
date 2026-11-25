package probe_test

import (
	"context"
	"testing"
	"time"

	"auditdrift/internal/model"
	"auditdrift/internal/probe"
	"auditdrift/internal/schedule"
)

func TestProbeTimeoutReleasesSchedulerConcurrencySlot(t *testing.T) {
	endpoint := probe.NewTestEndpoint()
	defer endpoint.Close()
	endpoint.SetDelay("slow", time.Second)
	client := probe.NewClient(endpoint.URL(), 50*time.Millisecond)

	tasks := make([]schedule.Task, 0, 2)
	for _, id := range []string{"slow-1", "slow-2"} {
		baselineID := "slow"
		key := id
		tasks = append(tasks, schedule.Task{
			Key: key, Host: "127.0.0.1", BaselineID: baselineID, Weight: 5,
			Execute: func(ctx context.Context) (schedule.Outcome, error) {
				_, err := client.Probe(ctx, "127.0.0.1", baselineID)
				return schedule.Outcome{}, err
			},
		})
	}
	start := time.Now()
	report, err := schedule.RunRound(context.Background(), schedule.Config{
		MaxConcurrency: 1, QueueSize: 1, RoundBudget: 3 * time.Second, RepairQuota: 10,
	}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 2 || report.PeakActive != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	for _, result := range report.Results {
		if result.ErrorCode != model.ErrProbeTimeout {
			t.Fatalf("task %s code=%s err=%v", result.Task.Key, result.ErrorCode, result.Err)
		}
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("timed-out probe did not release slot promptly: %s", elapsed)
	}
}
