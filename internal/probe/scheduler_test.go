package probe_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"deployverify/internal/model"
	"deployverify/internal/probe"
)

func task(env, service string, priority int) probe.Task {
	return probe.Task{Ref: model.Ref{Environment: env, Service: service}, Priority: priority, URL: "http://example.invalid", Timeout: time.Second}
}

func TestSchedulerStableOrderAndDedupe(t *testing.T) {
	var mu sync.Mutex
	order := []string{}
	scheduler := &probe.Scheduler{Executor: func(ctx context.Context, task probe.Task) probe.Result {
		mu.Lock()
		order = append(order, task.Ref.String())
		mu.Unlock()
		return probe.Result{Ref: task.Ref, Status: "success"}
	}}
	tasks := []probe.Task{
		task("p", "zeta", 10), task("d", "b", 30), task("p", "alpha", 10),
		task("d", "a", 30), task("p", "alpha", 10),
	}
	report := scheduler.Run(context.Background(), "order", tasks, probe.Config{GlobalConcurrency: 1, EnvConcurrency: 1, QueueCapacity: 10, Budget: time.Second})
	if report.DuplicateCount != 1 {
		t.Fatalf("duplicates=%d", report.DuplicateCount)
	}
	want := []string{"p/alpha", "p/zeta", "d/a", "d/b"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("order=%v want=%v", order, want)
	}
	if report.Empty || report.Status != "completed" {
		t.Fatalf("report=%+v", report)
	}
}

func TestSchedulerEmptyRoundExplicitConclusion(t *testing.T) {
	report := (&probe.Scheduler{}).Run(context.Background(), "empty", nil, probe.Config{Budget: time.Second})
	if !report.Empty || report.Status != "empty_round" || !strings.Contains(report.Conclusion, "QUEUE_EMPTY") {
		t.Fatalf("empty report=%+v", report)
	}
}

func TestSchedulerRespectsGlobalAndEnvironmentSlots(t *testing.T) {
	scheduler := &probe.Scheduler{Executor: func(ctx context.Context, task probe.Task) probe.Result {
		time.Sleep(40 * time.Millisecond)
		return probe.Result{Ref: task.Ref, Status: "success"}
	}}
	tasks := []probe.Task{task("a", "1", 1), task("a", "2", 1), task("b", "1", 2), task("b", "2", 2)}
	report := scheduler.Run(context.Background(), "slots", tasks, probe.Config{
		GlobalConcurrency: 3, EnvConcurrency: 1, QueueCapacity: 10, Budget: 2 * time.Second,
	})
	if report.PeakGlobalActive > 2 {
		t.Fatalf("global peak=%d, env=1 should cap to 2 across two envs", report.PeakGlobalActive)
	}
	if report.PeakEnvActive["a"] > 1 || report.PeakEnvActive["b"] > 1 {
		t.Fatalf("env peaks=%v", report.PeakEnvActive)
	}
	if report.SucceededCount != 4 {
		t.Fatalf("succeeded=%d report=%+v", report.SucceededCount, report)
	}
}

func TestSchedulerBoundedQueueFull(t *testing.T) {
	release := make(chan struct{})
	scheduler := &probe.Scheduler{Executor: func(ctx context.Context, task probe.Task) probe.Result {
		select {
		case <-release:
			return probe.Result{Ref: task.Ref, Status: "success"}
		case <-ctx.Done():
			return probe.Result{Ref: task.Ref, Status: "skipped", Code: model.CodeProbeQueueFull}
		}
	}}
	tasks := []probe.Task{task("p", "a", 1), task("p", "b", 1), task("p", "c", 1), task("p", "d", 1)}
	done := make(chan probe.Report, 1)
	go func() {
		done <- scheduler.Run(context.Background(), "full", tasks, probe.Config{
			GlobalConcurrency: 1, EnvConcurrency: 1, QueueCapacity: 1, Budget: 5 * time.Second,
		})
	}()
	select {
	case report := <-done:
		if report.Code != model.CodeProbeQueueFull || len(report.Rejected) == 0 {
			t.Fatalf("report=%+v", report)
		}
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("queue-full round did not return")
	}
}

func TestSchedulerBudgetCutsOff(t *testing.T) {
	scheduler := &probe.Scheduler{Executor: func(ctx context.Context, task probe.Task) probe.Result {
		<-ctx.Done()
		return probe.Result{Ref: task.Ref, Status: "skipped", Code: model.CodeProbeBudget}
	}}
	start := time.Now()
	report := scheduler.Run(context.Background(), "budget", []probe.Task{task("a", "1", 1), task("a", "2", 1)}, probe.Config{
		GlobalConcurrency: 2, EnvConcurrency: 2, QueueCapacity: 0, Budget: 80 * time.Millisecond,
	})
	if time.Since(start) > time.Second {
		t.Fatalf("budget round took too long: %s", time.Since(start))
	}
	if report.SkippedCount != 2 {
		t.Fatalf("skipped=%d report=%+v", report.SkippedCount, report)
	}
}

func TestHTTPTimeoutClosesRequestContext(t *testing.T) {
	clientCanceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		clientCanceled <- struct{}{}
	}))
	defer server.Close()
	result := probe.ExecuteHTTP(context.Background(), probe.Task{
		Ref: model.Ref{Environment: "e", Service: "slow"}, URL: server.URL, Timeout: 40 * time.Millisecond,
	})
	if result.Code != model.CodeProbeTimeout || result.Status != "timeout" {
		t.Fatalf("result=%+v", result)
	}
	select {
	case <-clientCanceled:
	case <-time.After(time.Second):
		t.Fatal("server-side request context was not canceled after timeout")
	}
}
