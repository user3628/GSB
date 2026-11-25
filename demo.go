package feedbackstream

import (
	"context"
	"os"
	"path/filepath"
)

type BackpressureDrill struct {
	GroupID                  string              `json:"group_id"`
	PreparedBatches          []string            `json:"prepared_batches"`
	WatermarkAfterTwo        Watermark           `json:"watermark_after_two"`
	BlockedAdmission         *PushBufferResult   `json:"blocked_admission"`
	BlockedReport            *BackpressureReport `json:"blocked_report"`
	DrainAndRetry            []string            `json:"drain_and_retry"`
	AllDrillBatchesCompleted bool                `json:"all_drill_batches_completed"`
}

type DemoReport struct {
	DataDir               string             `json:"data_dir"`
	DatabasePath          string             `json:"database_path"`
	FusionPlan            *FusionPlan        `json:"fusion_plan"`
	StartupRecovery       *RecoveryReport    `json:"startup_recovery"`
	FirstRound            *RoundReport       `json:"first_round"`
	BackpressureDrill     *BackpressureDrill `json:"backpressure_drill"`
	RetryRounds           []*RoundReport     `json:"retry_rounds"`
	CompletedBatchCount   int                `json:"completed_batch_count"`
	WindowResultsV1       int                `json:"window_results_v1"`
	PolicyV2              *PolicySnapshot    `json:"policy_v2"`
	PolicyV2Plan          *FusionPlan        `json:"policy_v2_plan"`
	WindowResultsV2       int                `json:"window_results_v2"`
	StaleBaseErrorCode    string             `json:"stale_base_error_code,omitempty"`
	StaleBaseErrorMessage string             `json:"stale_base_error_message,omitempty"`
	FinalRecovery         *RecoveryReport    `json:"final_recovery"`
	SQLiteForReview       string             `json:"sqlite_for_review"`
}

type DemoOptions struct {
	DataDir string
	Reset   bool
}

func RunDemo(ctx context.Context, opts DemoOptions) (*DemoReport, error) {
	dir := opts.DataDir
	if dir == "" {
		dir = DefaultDataDir()
	}
	if opts.Reset {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(filepath.Join(dir, "stream.db"+suffix))
		}
	}
	if _, err := EnsureInitialSample(dir); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(dir, "stream.db")
	store, err := OpenStore(ctx, dbPath)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	startupRecovery, err := store.Recover(ctx)
	if err != nil {
		return nil, err
	}
	policy, err := ReadPolicyFile(filepath.Join(dir, "policy-v1.json"))
	if err != nil {
		return nil, err
	}
	if err := store.SaveInitialPolicy(ctx, policy); err != nil {
		return nil, err
	}
	service := NewService(store)
	_, plan, err := service.ActivePlan(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := service.EnqueueSampleData(ctx, dir); err != nil {
		return nil, err
	}

	firstRound, err := service.RunNextRound(ctx, "round-budget-1")
	if err != nil {
		return nil, err
	}

	drill, err := runBackpressureDrill(ctx, service)
	if err != nil {
		return nil, err
	}

	retryRounds := []*RoundReport{}
	for roundNo := 2; roundNo <= 8; roundNo++ {
		requeued, err := service.RequeueShed(ctx)
		if err != nil {
			return nil, err
		}
		queued, err := store.ListBatchesByStatus(ctx, StatusQueued)
		if err != nil {
			return nil, err
		}
		if requeued == 0 && len(queued) == 0 {
			break
		}
		round, err := service.RunNextRound(ctx, "round-retry-"+itoa(roundNo))
		if err != nil {
			return nil, err
		}
		retryRounds = append(retryRounds, round)
	}

	completed, err := store.ListBatchesByStatus(ctx, StatusCompleted)
	if err != nil {
		return nil, err
	}
	resultsV1, err := store.ListWindowResults(ctx, 1, false)
	if err != nil {
		return nil, err
	}

	threshold := int64(9_000_000)
	patch := PolicyPatch{
		BaseVersion: 1,
		Reason:      "demo: tighten filter and aggregate buffer after deterministic review",
		Operators: []OperatorPatch{{
			ID:     "filter",
			Config: &OperatorConfig{MetricGTE: &threshold},
		}},
		Buffers: map[string]BufferLimit{
			"fg-2": {MaxInflightBatches: 3, MaxBytes: 8192, HighWatermarkRatio: 0.8},
		},
	}
	policyV2, planV2, patchErr := service.ApplyPolicyPatch(ctx, patch)
	if patchErr != nil {
		return nil, patchErr
	}
	_, _, staleErr := service.ApplyPolicyPatch(ctx, PolicyPatch{BaseVersion: 1, Reason: "must fail"})
	staleCode := ""
	staleMessage := ""
	if staleErr != nil {
		staleCode = CodeConflictRequest
		if coded, ok := AsError(staleErr); ok {
			staleCode = coded.Code
			staleMessage = coded.Message
		} else {
			staleMessage = staleErr.Error()
		}
	}
	resultsV2, err := store.ListWindowResults(ctx, policyV2.Version, false)
	if err != nil {
		return nil, err
	}
	finalRecovery, err := store.Recover(ctx)
	if err != nil {
		return nil, err
	}

	return &DemoReport{
		DataDir: dir, DatabasePath: dbPath, FusionPlan: plan,
		StartupRecovery: startupRecovery, FirstRound: firstRound,
		BackpressureDrill: drill, RetryRounds: retryRounds,
		CompletedBatchCount: len(completed), WindowResultsV1: len(resultsV1),
		PolicyV2: policyV2, PolicyV2Plan: planV2, WindowResultsV2: len(resultsV2),
		StaleBaseErrorCode: staleCode, StaleBaseErrorMessage: staleMessage,
		FinalRecovery:   finalRecovery,
		SQLiteForReview: "sqlite3 " + dbPath + " \"select policy_version,status,count(*) from window_results group by 1,2;\"",
	}, nil
}

func runBackpressureDrill(ctx context.Context, service *Service) (*BackpressureDrill, error) {
	drill := &BackpressureDrill{
		GroupID:         "fg-2",
		PreparedBatches: []string{"batch-p0-w1", "batch-p1-w1", "batch-p2-w0"},
		DrainAndRetry:   []string{},
	}
	for _, batchID := range drill.PreparedBatches {
		step, err := service.RunGroup(ctx, batchID, "fg-1")
		if err != nil {
			return nil, err
		}
		if step.Status != StatusCompleted {
			return nil, NewError(CodeStateInvalid, "source fusion group did not complete").
				With("batch_id", batchID).With("status", step.Status)
		}
	}

	first, err := service.AdmitGroup(ctx, drill.PreparedBatches[0], "fg-2")
	if err != nil {
		return nil, err
	}
	if !first.Admitted {
		return nil, NewError(CodeStateInvalid, "first aggregate admission was blocked")
	}
	second, err := service.AdmitGroup(ctx, drill.PreparedBatches[1], "fg-2")
	if err != nil {
		return nil, err
	}
	if !second.Admitted {
		return nil, NewError(CodeStateInvalid, "second aggregate admission was blocked")
	}
	drill.WatermarkAfterTwo = second.Watermark

	blocked, err := service.AdmitGroup(ctx, drill.PreparedBatches[2], "fg-2")
	if err != nil {
		return nil, err
	}
	if blocked.Admitted {
		return nil, NewError(CodeStateInvalid, "third aggregate admission unexpectedly admitted")
	}
	drill.BlockedAdmission = blocked
	policy, _, err := service.ActivePlan(ctx)
	if err != nil {
		return nil, err
	}
	drill.BlockedReport = backpressureReport(blocked, policy, "fg-2")

	if _, err := service.ExecuteAdmittedGroup(ctx, drill.PreparedBatches[0], "fg-2"); err != nil {
		return nil, err
	}
	drill.DrainAndRetry = append(drill.DrainAndRetry, "completed "+drill.PreparedBatches[0])
	retry, err := service.AdmitGroup(ctx, drill.PreparedBatches[2], "fg-2")
	if err != nil {
		return nil, err
	}
	if !retry.Admitted {
		return nil, NewError(CodeStateInvalid, "blocked batch was not admitted after drain").
			With("watermark", retry.Watermark)
	}
	if _, err := service.ExecuteAdmittedGroup(ctx, drill.PreparedBatches[1], "fg-2"); err != nil {
		return nil, err
	}
	if _, err := service.ExecuteAdmittedGroup(ctx, drill.PreparedBatches[2], "fg-2"); err != nil {
		return nil, err
	}
	drill.DrainAndRetry = append(drill.DrainAndRetry,
		"admitted retry "+drill.PreparedBatches[2],
		"completed "+drill.PreparedBatches[1],
		"completed "+drill.PreparedBatches[2])

	store := service.Store()
	allCompleted := true
	for _, batchID := range drill.PreparedBatches {
		batch, err := store.GetBatch(ctx, batchID)
		if err != nil {
			return nil, err
		}
		if batch == nil || batch.Status != StatusCompleted {
			allCompleted = false
		}
	}
	drill.AllDrillBatchesCompleted = allCompleted
	return drill, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
