package engine_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"local/windowengine/internal/domain"
	"local/windowengine/internal/engine"
	"local/windowengine/internal/sample"
	"local/windowengine/internal/store"
)

func TestFullRecalcAndVersionedReads(t *testing.T) {
	eng, database := newTestEngine(t)
	loadSample(t, eng)
	result := trigger(t, eng, engine.Budgets{
		GlobalConcurrency: 4, TenantConcurrency: 2, TimeBudgetMS: 10_000,
		QueueCapacity: 500, TaskDelayMS: 1,
	}, false)
	if result.Round.Status != "COMPLETED" {
		t.Fatalf("round status=%s errors=%+v", result.Round.Status, result.TaskErrors)
	}
	if result.Round.CommittedCount != len(result.Tasks) || result.Round.ShedCount != 0 || result.Round.FailedCount != 0 {
		t.Fatalf("unexpected round counts: %+v", result.Round)
	}
	if result.Round.MaxGlobalRunning > 4 || result.Round.MaxTenantRunning > 2 {
		t.Fatalf("concurrency exceeded: %+v", result.Round)
	}
	if result.Round.ConsumedBudgetMS > result.Round.TimeBudgetMS {
		t.Fatalf("accounted budget exceeded: %+v", result.Round)
	}
	for _, task := range result.Tasks {
		if task.Status != "COMMITTED" {
			t.Fatalf("task %s status=%s", task.TaskID, task.Status)
		}
	}

	query, business := database.QueryAggregates(context.Background(), "det-sample-seed-242", "tenant-b", "", "", "", nil)
	if business != nil {
		t.Fatalf("query: %v", business)
	}
	if query.State.Version <= 0 || len(query.Windows) == 0 {
		t.Fatalf("empty tenant state: %+v", query)
	}
	if _, staleErr := database.QueryAggregates(context.Background(), "det-sample-seed-242", "tenant-b", "", "", "", int64Pointer(0)); staleErr == nil || staleErr.Code != domain.CodeStaleVersion {
		t.Fatalf("expected E_STALE_VERSION, got %v", staleErr)
	}

	var foundMidnight bool
	var totalCount int64
	var totalSum int64
	for _, window := range query.Windows {
		if window.Aggregate == nil {
			t.Fatalf("committed window has no aggregate: %+v", window)
		}
		totalCount += window.Aggregate.Count
		sum, err := strconv.ParseInt(window.Aggregate.SumScaled, 10, 64)
		if err != nil {
			t.Fatalf("parse sum: %v", err)
		}
		totalSum += sum
		if window.SpecID == "tumble-1d-cross-year" && window.StartMS == 1704067200000 {
			foundMidnight = true
		}
	}
	if !foundMidnight {
		t.Fatal("cross-year midnight window was not materialized")
	}
	if query.State.TotalCount != totalCount {
		t.Fatalf("tenant count=%d, aggregate sum-of-windows=%d", query.State.TotalCount, totalCount)
	}
	stateSum, _ := strconv.ParseInt(query.State.TotalSumScaled, 10, 64)
	if stateSum != totalSum {
		t.Fatalf("tenant sum=%d, aggregate sum-of-windows=%d", stateSum, totalSum)
	}
}

func TestQueueFullPreflightLeavesNoRound(t *testing.T) {
	eng, database := newTestEngine(t)
	loadSample(t, eng)
	_, err := eng.Trigger(context.Background(), engine.TriggerOptions{
		SnapshotID: "det-sample-seed-242",
		Budgets: engine.Budgets{
			GlobalConcurrency: 1, TenantConcurrency: 1, TimeBudgetMS: 1000, QueueCapacity: 0,
		},
	})
	business, ok := domain.BusinessError(err)
	if !ok || business.Code != domain.CodeRecalcQueueFull {
		t.Fatalf("expected E_RECALC_QUEUE_FULL, got %v", err)
	}
	rounds, listErr := database.Rounds(context.Background(), 10)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(rounds) != 0 {
		t.Fatalf("queue-full preflight wrote %d rounds, want 0", len(rounds))
	}
}

func TestBudgetShedAndRequeue(t *testing.T) {
	eng, _ := newTestEngine(t)
	loadSample(t, eng)
	first := trigger(t, eng, engine.Budgets{
		GlobalConcurrency: 2, TenantConcurrency: 1, TimeBudgetMS: 5, QueueCapacity: 500,
	}, false)
	if first.Round.ShedCount == 0 {
		t.Fatalf("expected SHED tasks, round=%+v", first.Round)
	}
	if first.Round.ConsumedBudgetMS > 5 || first.Round.RemainingBudgetMS < 0 {
		t.Fatalf("budget accounting invalid: %+v", first.Round)
	}
	var shedTasks int
	for _, task := range first.Tasks {
		if task.Status == "SHED" {
			shedTasks++
			if task.ConsumedMS != 0 {
				t.Fatalf("shed task consumed budget: %+v", task)
			}
		}
	}
	if shedTasks != first.Round.ShedCount || len(first.Shed) != first.Round.ShedCount {
		t.Fatalf("shed task count mismatch: tasks=%d decisions=%d round=%d", shedTasks, len(first.Shed), first.Round.ShedCount)
	}
	for index, decision := range first.Shed {
		if decision.DecisionOrder != index+1 || decision.ReasonCode != "BUDGET_EXHAUSTED" {
			t.Fatalf("shed decision not deterministic: %+v", decision)
		}
	}

	second := trigger(t, eng, engine.Budgets{
		GlobalConcurrency: 3, TenantConcurrency: 2, TimeBudgetMS: 10_000, QueueCapacity: 500,
	}, false)
	if second.Round.ConsideredCount != len(first.Shed) {
		t.Fatalf("requeue considered=%d, want %d", second.Round.ConsideredCount, len(first.Shed))
	}
	if second.Round.ShedCount != 0 || second.Round.CommittedCount != second.Round.ConsideredCount {
		t.Fatalf("requeue did not commit all: %+v errors=%+v", second.Round, second.TaskErrors)
	}
}

func TestLowBudgetQueueCapacityAppliesToSelectedPrefixOnly(t *testing.T) {
	eng, _ := newTestEngine(t)
	loadSample(t, eng)
	result := trigger(t, eng, engine.Budgets{
		GlobalConcurrency: 1, TenantConcurrency: 1, TimeBudgetMS: 1, QueueCapacity: 0,
	}, false)
	if result.Round.ShedCount == 0 {
		t.Fatalf("expected the budget suffix to be shed: %+v", result.Round)
	}
	if result.Round.ConsumedBudgetMS > 1 {
		t.Fatalf("budget exceeded: %+v", result.Round)
	}
}

func TestReadersSeeOnlyWholeCommittedVersions(t *testing.T) {
	eng, database := newTestEngine(t)
	loadSample(t, eng)
	done := make(chan error, 1)
	go func() {
		_, err := eng.Trigger(context.Background(), engine.TriggerOptions{
			SnapshotID: "det-sample-seed-242",
			Budgets: engine.Budgets{
				GlobalConcurrency: 4, TenantConcurrency: 2, TimeBudgetMS: 10_000,
				QueueCapacity: 500, TaskDelayMS: 2,
			},
		})
		done <- err
	}()

	readErr := make(chan string, 1)
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			result, business := database.QueryAggregates(context.Background(), "det-sample-seed-242", "tenant-b", "", "", "", nil)
			if business != nil {
				readErr <- business.Error()
				return
			}
			var countSum, sumSum int64
			for _, window := range result.Windows {
				if window.Aggregate == nil {
					if window.Status == "COMMITTED" {
						readErr <- "COMMITTED window without aggregate: " + window.WindowID
						return
					}
					continue
				}
				if window.Aggregate.Version > result.Version {
					readErr <- "aggregate version ahead of tenant version"
					return
				}
				countSum += window.Aggregate.Count
				sum, err := strconv.ParseInt(window.Aggregate.SumScaled, 10, 64)
				if err != nil {
					readErr <- err.Error()
					return
				}
				sumSum += sum
			}
			if countSum != result.State.TotalCount {
				readErr <- "tenant state count and window aggregate set diverged"
				return
			}
			stateSum, _ := strconv.ParseInt(result.State.TotalSumScaled, 10, 64)
			if sumSum != stateSum {
				readErr <- "tenant state sum and window aggregate set diverged"
				return
			}
		}
	}()

	if err := <-done; err != nil {
		close(stop)
		<-readerDone
		t.Fatalf("trigger: %v", err)
	}
	close(stop)
	<-readerDone
	select {
	case message := <-readErr:
		t.Fatal(message)
	default:
	}
}

func TestSnapshotInvalidDoesNotChangeCommittedState(t *testing.T) {
	eng, database := newTestEngine(t)
	loadSample(t, eng)
	invalid := []byte(`{"snapshot_id":"bad","tenants":[{"id":"x","priority":1},{"id":"x","priority":2}],"window_specs":[{"id":"w","type":"tumbling","width_ms":1000}]}`)
	if _, _, err := eng.LoadSnapshot(context.Background(), invalid); err == nil {
		t.Fatal("invalid snapshot succeeded")
	} else if business, ok := domain.BusinessError(err); !ok || business.Code != domain.CodeSnapshotInvalid {
		t.Fatalf("expected E_SNAPSHOT_INVALID, got %v", err)
	}
	snapshots, err := database.ListSnapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 || snapshots[0].SnapshotID != "det-sample-seed-242" {
		t.Fatalf("committed snapshots changed: %+v", snapshots)
	}
}

func TestRecoveryRollsBackPendingAndStagedWork(t *testing.T) {
	eng, database := newTestEngine(t)
	snapshot := loadSample(t, eng)
	windows, _, err := database.WindowsAndSpecs(context.Background(), snapshot.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	window := windows[0]
	tenantPriorities := map[string]int{}
	for _, tenant := range snapshot.Tenants {
		tenantPriorities[tenant.ID] = tenant.Priority
	}
	round := store.Round{
		RoundID: "round_manual_crash", SnapshotID: snapshot.SnapshotID, Status: "RUNNING",
		GlobalConcurrency: 1, TenantConcurrency: 1, TimeBudgetMS: 100, QueueCapacity: 10,
		RemainingBudgetMS: 100, BudgetsJSON: "{}", StartedAtMS: time.Now().UnixMilli(),
	}
	task := store.Task{
		TaskID: "t_manual_crash", RoundID: round.RoundID, SnapshotID: snapshot.SnapshotID,
		TenantID: window.TenantID, Priority: tenantPriorities[window.TenantID],
		WindowID: window.WindowID, GroupKey: window.GroupKey, SpecID: window.SpecID,
		WindowType: string(window.WindowType), StartMS: window.StartMS, EndMS: window.EndMS,
		CostMS: 1,
	}
	if err := database.CreateRound(context.Background(), round, []store.Task{task}); err != nil {
		t.Fatalf("create round: %v", err)
	}
	if err := database.StartTask(context.Background(), task); err != nil {
		t.Fatalf("start: %v", err)
	}
	groupEvents := make([]domain.Event, 0)
	for _, event := range snapshot.Events {
		if event.TenantID == window.TenantID && event.GroupKey == window.GroupKey {
			groupEvents = append(groupEvents, event)
		}
	}
	aggregate, aggregateErr := domain.AggregateWindow(window, groupEvents)
	if aggregateErr != nil {
		t.Fatalf("aggregate: %v", aggregateErr)
	}
	attempt, err := database.StageAggregate(context.Background(), task.TaskID, aggregate)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err := database.PrepareCommitIntent(context.Background(), task.TaskID, attempt, aggregate); err != nil {
		t.Fatalf("commit intent: %v", err)
	}
	report, recoveryErr := eng.Recover(context.Background())
	if recoveryErr != nil {
		t.Fatalf("recover: %v", recoveryErr)
	}
	if report.PendingIntentCount != 2 {
		t.Fatalf("pending intents=%d, want 2: %+v", report.PendingIntentCount, report)
	}
	if !contains(report.AbortedTaskIDs, task.TaskID) || !contains(report.ResetWindowIDs, window.WindowID) {
		t.Fatalf("recovery report missed task/window: %+v", report)
	}
	loaded, err := database.Task(context.Background(), task.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != "ABORTED" {
		t.Fatalf("task status=%s, want ABORTED", loaded.Status)
	}
	query, business := database.QueryAggregates(context.Background(), snapshot.SnapshotID, window.TenantID, window.WindowID, "", "", nil)
	if business != nil {
		t.Fatal(business)
	}
	if query.Windows[0].Status != "PENDING" || query.Windows[0].Aggregate != nil {
		t.Fatalf("window was not rolled back: %+v", query.Windows[0])
	}
	pending, err := database.ListIntents(context.Background(), "PENDING")
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending intents remain: %+v", pending)
	}
	latest, reportErr := database.RecoveryReport(context.Background(), report.ReportID)
	if reportErr != nil || !latest.BasedOnDurableFacts {
		t.Fatalf("durable recovery report unavailable: %+v %v", latest, reportErr)
	}
}

func newTestEngine(t *testing.T) (*engine.Engine, *store.Store) {
	t.Helper()
	database, err := store.Open(filepath.Join(t.TempDir(), "window.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return engine.New(database, 8), database
}

func loadSample(t *testing.T, eng *engine.Engine) *domain.Snapshot {
	t.Helper()
	data, err := sample.Generate(242)
	if err != nil {
		t.Fatal(err)
	}
	_, snapshot, err := eng.LoadSnapshot(context.Background(), data)
	if err != nil {
		t.Fatalf("load sample: %v", err)
	}
	return snapshot
}

func trigger(t *testing.T, eng *engine.Engine, budgets engine.Budgets, force bool) *engine.TriggerResult {
	t.Helper()
	result, err := eng.Trigger(context.Background(), engine.TriggerOptions{
		SnapshotID: "det-sample-seed-242", Budgets: budgets, Force: force,
	})
	if err != nil {
		t.Fatalf("trigger: %v", err)
	}
	return result
}

func int64Pointer(value int64) *int64 {
	return &value
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
