package store

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gatewayfreshness.local/workbench/internal/freshness"
)

const (
	testKey           = "local-deterministic-demo-key-2026-not-for-production"
	testConfigVersion = "cfg-test-version"
)

type requestSpec struct {
	id        string
	ts        int64
	client    string
	nonce     string
	tolerance int64
	window    int64
	badSig    bool
}

func testOptions(path string) Options {
	return Options{
		Path: path, ConfigVersion: testConfigVersion, ConfigSnapshotJSON: `{"version":"` + testConfigVersion + `"}`,
		SigningKey: []byte(testKey), SigningKeyID: "test-key", FastPrecheckLimit: 2,
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(context.Background(), testOptions(filepath.Join(t.TempDir(), "workbench.db")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	return st
}

func makeBatch(batchID, mode string, reference int64, specs ...requestSpec) (*freshness.ValidatedBatch, error) {
	input := freshness.BatchInput{SchemaVersion: 1, BatchID: batchID, Mode: mode, GatewayReferenceTimeMS: &reference}
	for _, sp := range specs {
		req := freshness.Request{
			RequestID: sp.id, TimestampMS: sp.ts, RouteKey: "orders.create", ClientID: sp.client,
			Nonce: sp.nonce, Weight: 1, ToleranceMS: sp.tolerance, WindowMS: sp.window,
		}
		signature := freshness.Signature([]byte(testKey), req)
		if sp.badSig {
			signature = strings.Repeat("0", 64)
		}
		input.Requests = append(input.Requests, freshness.RequestInput{
			RequestID: req.RequestID, TimestampMS: req.TimestampMS, RouteKey: req.RouteKey,
			ClientID: req.ClientID, Nonce: req.Nonce, Signature: signature, Weight: 1,
			ToleranceMS: sp.tolerance, WindowMS: sp.window,
		})
	}
	validated, appErr := freshness.ValidateBatch(input, freshness.ValidationPolicy{
		MaxToleranceMS: 5000, MaxWindowMS: 60000, MaxRequestsPerBatch: 100, AllowReferenceOverride: true,
	}, reference)
	if appErr != nil {
		return nil, appErr
	}
	return validated, nil
}

func mustBatch(t *testing.T, batchID, mode string, reference int64, specs ...requestSpec) *freshness.ValidatedBatch {
	t.Helper()
	b, err := makeBatch(batchID, mode, reference, specs...)
	if err != nil {
		t.Fatalf("validate fixture: %v", err)
	}
	return b
}

func adjudicate(t *testing.T, st *Store, batch *freshness.ValidatedBatch) *freshness.BatchDecision {
	t.Helper()
	d, _, err := st.Adjudicate(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestBoundarySignatureAndSharedVersion(t *testing.T) {
	st := openTestStore(t)
	batch := mustBatch(t, "boundary-batch", freshness.ModeFull, 1000,
		requestSpec{"req-upper-pass", 1500, "client-upper", "nonce-upper-00000001", 500, 2000, false},
		requestSpec{"req-upper-fail", 1501, "client-upper", "nonce-upper-00000002", 500, 2000, false},
		requestSpec{"req-lower-pass", 500, "client-lower", "nonce-lower-00000001", 500, 2000, false},
		requestSpec{"req-lower-fail", 499, "client-lower", "nonce-lower-00000002", 500, 2000, false},
		requestSpec{"req-bad-signature", 1000, "client-sig", "nonce-signature-00001", 500, 2000, true},
	)
	decision := adjudicate(t, st, batch)
	if decision.Outcome != freshness.OutcomeRejected || decision.Code != freshness.CodeTimestampAhead {
		t.Fatalf("unexpected batch outcome: %s %s", decision.Outcome, decision.Code)
	}
	want := []struct {
		code     string
		offset   int64
		boundary string
	}{
		{freshness.CodeOK, 500, freshness.BoundaryNone},
		{freshness.CodeTimestampAhead, 501, freshness.BoundaryUpper},
		{freshness.CodeOK, -500, freshness.BoundaryNone},
		{freshness.CodeTimestampTooOld, -501, freshness.BoundaryLower},
		{freshness.CodeSignatureMismatch, 0, freshness.BoundaryNone},
	}
	if len(decision.Items) != len(want) {
		t.Fatalf("items=%d", len(decision.Items))
	}
	for i, expected := range want {
		item := decision.Items[i]
		if item.Code != expected.code || item.OffsetMS != expected.offset || item.TriggeredBoundary != expected.boundary {
			t.Fatalf("item %d got code=%s offset=%d boundary=%s", i, item.Code, item.OffsetMS, item.TriggeredBoundary)
		}
		if item.WindowVersion != decision.WindowVersion || item.ToleranceConfigVersion != decision.ToleranceConfigVersion {
			t.Fatalf("item %d version drift", i)
		}
		for _, stage := range []freshness.StageResult{item.SignatureStage, item.TimestampStage, item.NonceStage} {
			if stage.WindowVersion != decision.WindowVersion || stage.ToleranceConfigVersion != decision.ToleranceConfigVersion {
				t.Fatalf("item %d stage version drift: %#v", i, stage)
			}
		}
	}
	read, err := st.GetDecision(context.Background(), batch.BatchID)
	if err != nil {
		t.Fatal(err)
	}
	if read.WindowVersion != decision.WindowVersion || read.Items[1].TriggeredBoundary != freshness.BoundaryUpper {
		t.Fatalf("persisted decision mismatch: %#v", read)
	}
}

func TestNonceReplayAndReclaim(t *testing.T) {
	st := openTestStore(t)
	first := adjudicate(t, st, mustBatch(t, "nonce-a", freshness.ModeFull, 1000,
		requestSpec{"req-a", 1000, "client-a", "nonce-replay-0000001", 100, 1000, false},
	))
	if first.Items[0].Code != freshness.CodeOK {
		t.Fatalf("first use: %s", first.Items[0].Code)
	}
	replay := adjudicate(t, st, mustBatch(t, "nonce-b", freshness.ModeFull, 1200,
		requestSpec{"req-b", 1200, "client-a", "nonce-replay-0000001", 100, 1000, false},
		requestSpec{"req-c", 1200, "client-c", "nonce-replay-0000001", 100, 1000, false},
	))
	if replay.Items[0].Code != freshness.CodeNonceReplayed || replay.Items[0].FirstOccupiedAtMS == nil || *replay.Items[0].FirstOccupiedAtMS != 1000 {
		t.Fatalf("expected replay with first occupancy 1000: %#v", replay.Items[0])
	}
	if replay.Items[0].RemainingWindowMS == nil || *replay.Items[0].RemainingWindowMS != 800 {
		t.Fatalf("remaining window=%v", replay.Items[0].RemainingWindowMS)
	}
	if replay.Items[1].Code != freshness.CodeOK {
		t.Fatalf("same nonce for another client must be independent: %s", replay.Items[1].Code)
	}
	reused := adjudicate(t, st, mustBatch(t, "nonce-c", freshness.ModeFull, 2000,
		requestSpec{"req-d", 2000, "client-a", "nonce-replay-0000001", 100, 1000, false},
	))
	if reused.Items[0].Code != freshness.CodeOK {
		t.Fatalf("expired nonce should be reclaimed before dedupe: %s", reused.Items[0].Code)
	}
	snapshot, err := st.Snapshot(context.Background(), SnapshotOptions{Version: 1, Budget: time.Second, Include: []string{"occupancy"}, ClientID: "client-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Occupancy.Records) != 1 || snapshot.Occupancy.Records[0].FirstOccupiedAtMS != 1000 {
		t.Fatalf("historical occupancy mismatch: %#v", snapshot.Occupancy)
	}
	latest, err := st.Snapshot(context.Background(), SnapshotOptions{Budget: time.Second, Include: []string{"occupancy"}, ClientID: "client-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(latest.Occupancy.Records) != 1 || latest.Occupancy.Records[0].FirstOccupiedAtMS != 2000 {
		t.Fatalf("latest occupancy mismatch: %#v", latest.Occupancy)
	}
}

func TestReclaimRespectsInFlightIntentReference(t *testing.T) {
	st := openTestStore(t)
	adjudicate(t, st, mustBatch(t, "inflight-base", freshness.ModeFull, 1000,
		requestSpec{"req-inflight", 1000, "client-inflight", "nonce-inflight-000001", 100, 100, false},
	))
	now := time.Now().UnixMilli()
	result, err := st.DB().Exec(`INSERT INTO intents(batch_id,planned_window_version,config_version,content_hash,mode,state,detail_json,created_at_ms,updated_at_ms)
		VALUES('other-pending',2,?,'hash','full','pending','{}',?,?)`, testConfigVersion, now, now)
	if err != nil {
		t.Fatal(err)
	}
	intentID, _ := result.LastInsertId()
	if _, err := st.DB().Exec(`INSERT INTO intent_nonce_refs(intent_id,client_id,nonce) VALUES(?,?,?)`, intentID, "client-inflight", "nonce-inflight-000001"); err != nil {
		t.Fatal(err)
	}

	tx, err := st.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.reclaimExpiredNonces(context.Background(), tx, 999, 3, "new-batch", 2000); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM nonces WHERE client_id='client-inflight' AND nonce='nonce-inflight-000001'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("in-flight referenced nonce was reclaimed")
	}

	if _, err := st.DB().Exec(`UPDATE intents SET state='aborted' WHERE id=?`, intentID); err != nil {
		t.Fatal(err)
	}
	tx, err = st.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.reclaimExpiredNonces(context.Background(), tx, 999, 4, "new-batch-2", 2000); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM nonces WHERE client_id='client-inflight'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("nonce was not reclaimed after the in-flight intent ended")
	}
}

func TestIdempotencyAndConflict(t *testing.T) {
	st := openTestStore(t)
	batch := mustBatch(t, "idem-batch", freshness.ModeFull, 1000,
		requestSpec{"req-1", 1000, "client-a", "nonce-idem-000000001", 100, 5000, false},
	)
	const workers = 20
	results := make([]*freshness.BatchDecision, workers)
	replayFlags := make([]bool, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], replayFlags[index], errs[index] = st.Adjudicate(context.Background(), batch)
		}(i)
	}
	close(start)
	wg.Wait()
	replays := 0
	for i := 0; i < workers; i++ {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if replayFlags[i] {
			replays++
		}
		if results[i].WindowVersion != 1 {
			t.Fatalf("worker %d saw version %d", i, results[i].WindowVersion)
		}
	}
	if replays != workers-1 {
		t.Fatalf("idempotent replays=%d, want %d", replays, workers-1)
	}
	retryWithLaterGatewayClock, err := makeBatch("idem-batch", freshness.ModeFull, 2500,
		requestSpec{"req-1", 1000, "client-a", "nonce-idem-000000001", 100, 5000, false},
	)
	if err != nil {
		t.Fatal(err)
	}
	replayed, wasReplay, err := st.Adjudicate(context.Background(), retryWithLaterGatewayClock)
	if err != nil {
		t.Fatal(err)
	}
	if !wasReplay || replayed.WindowVersion != 1 || replayed.ReferenceTimeMS != 1000 {
		t.Fatalf("gateway-clock retry should reuse the first decision: replay=%v decision=%#v", wasReplay, replayed)
	}

	// requestSpec slices cannot express a different weight, so sign a custom batch.
	input := freshness.BatchInput{SchemaVersion: 1, BatchID: "idem-batch", Mode: freshness.ModeFull, GatewayReferenceTimeMS: int64Ptr(1000)}
	req := freshness.Request{RequestID: "req-1", TimestampMS: 1000, RouteKey: "orders.create", ClientID: "client-a", Nonce: "nonce-idem-000000001", Weight: 9, ToleranceMS: 100, WindowMS: 5000}
	input.Requests = []freshness.RequestInput{{RequestID: req.RequestID, TimestampMS: req.TimestampMS, RouteKey: req.RouteKey, ClientID: req.ClientID, Nonce: req.Nonce, Signature: freshness.Signature([]byte(testKey), req), Weight: 9, ToleranceMS: 100, WindowMS: 5000}}
	changed, validationErr := freshness.ValidateBatch(input, freshness.ValidationPolicy{MaxToleranceMS: 5000, MaxWindowMS: 60000, MaxRequestsPerBatch: 100, AllowReferenceOverride: true}, 1000)
	if validationErr != nil {
		t.Fatal(validationErr)
	}
	_, _, err = st.Adjudicate(context.Background(), changed)
	var appErr *freshness.AppError
	if !errors.As(err, &appErr) || appErr.Code != freshness.CodeConflictingRequest {
		t.Fatalf("expected conflict, got %v", err)
	}
	persisted, err := st.GetDecision(context.Background(), "idem-batch")
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Items[0].Weight != 1 {
		t.Fatalf("conflict changed persisted decision: %d", persisted.Items[0].Weight)
	}
}

func int64Ptr(v int64) *int64 { return &v }

func TestFastModeDoesNotSilentlyPassSuffix(t *testing.T) {
	st := openTestStore(t)
	fast := adjudicate(t, st, mustBatch(t, "fast-batch", freshness.ModeFast, 1000,
		requestSpec{"req-fast-1", 1000, "client-f", "nonce-fast-000000001", 100, 5000, false},
		requestSpec{"req-fast-2", 1000, "client-f", "nonce-fast-000000002", 100, 5000, false},
		requestSpec{"req-fast-3", 1000, "client-f", "nonce-fast-000000003", 100, 5000, true},
		requestSpec{"req-fast-4", 1000, "client-f", "nonce-fast-000000004", 100, 5000, false},
	))
	if fast.Outcome != freshness.OutcomePartial || fast.Code != freshness.CodePrecheckIncomplete || fast.SampleRate != 0.5 {
		t.Fatalf("fast outcome=%s code=%s sample=%f", fast.Outcome, fast.Code, fast.SampleRate)
	}
	if fast.Items[2].Prechecked || fast.Items[2].Result != freshness.ResultNotPrechecked || fast.Items[2].Code != freshness.CodePrecheckIncomplete {
		t.Fatalf("suffix item was silently passed: %#v", fast.Items[2])
	}
	if fast.Items[2].SignatureStage.Status != freshness.StageNotPrechecked {
		t.Fatalf("suffix signature stage=%s", fast.Items[2].SignatureStage.Status)
	}
	snapshot, err := st.Snapshot(context.Background(), SnapshotOptions{Budget: time.Second, Include: []string{"occupancy"}})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Occupancy.ActiveCount != 2 {
		t.Fatalf("fast mode must only occupy prechecked nonces, got %d", snapshot.Occupancy.ActiveCount)
	}

	full := adjudicate(t, st, mustBatch(t, "full-batch", freshness.ModeFull, 1000,
		requestSpec{"req-fast-1", 1000, "client-g", "nonce-full-000000001", 100, 5000, false},
		requestSpec{"req-fast-2", 1000, "client-g", "nonce-full-000000002", 100, 5000, false},
		requestSpec{"req-fast-3", 1000, "client-g", "nonce-full-000000003", 100, 5000, false},
		requestSpec{"req-fast-4", 1000, "client-g", "nonce-full-000000004", 100, 5000, false},
	))
	modeReport, err := st.ModeDifferences(context.Background(), full.BatchID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !modeReport.FullResultAvailable || modeReport.Differences != 2 || modeReport.Items[2].FastResult != freshness.ResultNotPrechecked {
		t.Fatalf("mode difference report=%#v", modeReport)
	}
	fastReport, err := st.ModeDifferences(context.Background(), fast.BatchID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if fastReport.FullResultAvailable || fastReport.Items[2].FullCode != "NOT_PRE_EVALUATED" {
		t.Fatalf("fast full-result availability must be explicit: %#v", fastReport)
	}
}

func TestSnapshotBudgetPartialNeverMixesVersions(t *testing.T) {
	st := openTestStore(t)
	for i := 0; i < 4; i++ {
		adjudicate(t, st, mustBatch(t, "snap-"+string(rune('a'+i)), freshness.ModeFull, int64(1000+i*10),
			requestSpec{"req", int64(1000 + i*10), "client-s", "nonce-snapshot-00000" + string(rune('1'+i)), 100, 5000, false},
		))
	}
	partial, err := st.Snapshot(context.Background(), SnapshotOptions{Budget: 0, Include: []string{"occupancy", "nonces", "trails"}})
	if err != nil {
		t.Fatal(err)
	}
	if !partial.Partial || partial.Complete || partial.SnapshotWindowVersion != 4 || len(partial.CompletedSections) != 0 {
		t.Fatalf("partial snapshot=%#v", partial)
	}
	full, err := st.Snapshot(context.Background(), SnapshotOptions{Budget: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !full.Complete || full.SnapshotWindowVersion != 4 || full.Occupancy == nil || full.Nonces == nil || full.Trails == nil {
		t.Fatalf("full snapshot=%#v", full)
	}
	for _, record := range full.Occupancy.Records {
		if record.WindowVersion > full.SnapshotWindowVersion {
			t.Fatalf("cross-version occupancy record: %#v", record)
		}
	}
	historical, err := st.Snapshot(context.Background(), SnapshotOptions{Version: 1, Budget: time.Second, Include: []string{"occupancy"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(historical.Occupancy.Records) != 1 || historical.Occupancy.Records[0].WindowVersion != 1 {
		t.Fatalf("historical snapshot leaked later versions: %#v", historical.Occupancy)
	}
	if _, err := st.Snapshot(context.Background(), SnapshotOptions{Version: 99, Budget: time.Second}); err == nil {
		t.Fatal("future version should be rejected")
	}
}

func TestInconsistencyDetailsAcrossPickedVersions(t *testing.T) {
	st := openTestStore(t)
	base := adjudicate(t, st, mustBatch(t, "inc-base", freshness.ModeFull, 1000,
		requestSpec{"req-inc", 1000, "client-inc", "nonce-inconsistency-01", 100, 1000, false},
	))
	adjudicate(t, st, mustBatch(t, "inc-reclaim", freshness.ModeFull, 2000,
		requestSpec{"req-other", 2000, "client-other", "nonce-inc-other-000001", 100, 1000, false},
	))
	adjudicate(t, st, mustBatch(t, "inc-reoccupy", freshness.ModeFull, 3000,
		requestSpec{"req-inc-2", 3000, "client-inc", "nonce-inconsistency-01", 100, 1000, false},
	))
	report, err := st.Inconsistencies(context.Background(), base.BatchID, []freshness.ClientView{
		{ClientID: "reader-old", PickedWindowVersion: 1},
		{ClientID: "reader-current", PickedWindowVersion: 3},
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Views) != 2 || report.Views[0].Differs || !report.Views[1].Differs {
		t.Fatalf("unexpected inconsistency report: %#v", report)
	}
	if report.Views[1].ClientID != "reader-current" || report.Views[1].PickedWindowVersion != 3 || report.Views[1].ViewedBatchCode != freshness.CodeNonceReplayed {
		t.Fatalf("missing client/version/difference detail: %#v", report.Views[1])
	}
	if len(report.Views[1].Differences) == 0 || report.Views[1].Differences[0].Field != "items[0].code" {
		t.Fatalf("difference fields missing: %#v", report.Views[1].Differences)
	}
}

func TestCrashHelperProcess(t *testing.T) {
	if os.Getenv("WORKBENCH_TEST_CRASH_HELPER") != "1" {
		return
	}
	path := os.Getenv("WORKBENCH_TEST_CRASH_DB")
	os.Setenv("WORKBENCH_CRASH_POINT", os.Getenv("WORKBENCH_TEST_CRASH_POINT"))
	st, err := Open(context.Background(), testOptions(path))
	if err != nil {
		os.Exit(90)
	}
	batch, err := makeBatch("crash-batch", freshness.ModeFull, 1000,
		requestSpec{"req-crash", 1000, "client-crash", "nonce-crash-000000001", 100, 1000, false},
	)
	if err != nil {
		os.Exit(91)
	}
	_, _, _ = st.Adjudicate(context.Background(), batch)
	os.Exit(92)
}

func TestForcedTerminationRecovery(t *testing.T) {
	for _, point := range []string{"after_intent", "after_writes"} {
		t.Run(point, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "crash.db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelperProcess$", "--")
			cmd.Env = append(os.Environ(),
				"WORKBENCH_TEST_CRASH_HELPER=1",
				"WORKBENCH_TEST_CRASH_DB="+dbPath,
				"WORKBENCH_TEST_CRASH_POINT="+point,
			)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				t.Fatalf("child did not terminate abruptly: err=%v output=%s", err, output)
			}
			wantCode := 70
			if point == "after_writes" {
				wantCode = 71
			}
			if exitErr.ExitCode() != wantCode {
				t.Fatalf("child exit=%d want=%d output=%s", exitErr.ExitCode(), wantCode, output)
			}

			st, err := Open(context.Background(), testOptions(dbPath))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			report, err := st.Recover(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if report.PendingIntents != 1 || report.Aborted != 1 || len(report.Details) != 1 {
				t.Fatalf("recovery report=%#v", report)
			}
			if _, err := st.GetDecision(context.Background(), "crash-batch"); !errors.Is(err, ErrBatchNotFound) {
				t.Fatalf("uncommitted decision became visible: %v", err)
			}
			var state string
			if err := st.DB().QueryRow(`SELECT state FROM intents WHERE batch_id='crash-batch'`).Scan(&state); err != nil {
				t.Fatal(err)
			}
			if state != "aborted" {
				t.Fatalf("intent state=%s", state)
			}
			snapshot, err := st.Snapshot(context.Background(), SnapshotOptions{Budget: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.SnapshotWindowVersion != 0 || snapshot.Occupancy.ActiveCount != 0 {
				t.Fatalf("partial nonce/version survived recovery: %#v", snapshot)
			}
			if _, err := st.Snapshot(context.Background(), SnapshotOptions{Version: 1, Budget: time.Second}); err == nil {
				t.Fatal("aborted planned version must not be queryable as a committed version")
			}
			retryBatch, err := makeBatch("crash-batch", freshness.ModeFull, 1000,
				requestSpec{"req-crash", 1000, "client-crash", "nonce-crash-000000001", 100, 1000, false},
			)
			if err != nil {
				t.Fatal(err)
			}
			retried := adjudicate(t, st, retryBatch)
			if retried.WindowVersion != 2 || retried.Code != freshness.CodeOK {
				t.Fatalf("retry after recovery=%#v", retried)
			}
			if _, err := st.Snapshot(context.Background(), SnapshotOptions{Version: 1, Budget: time.Second}); err == nil {
				t.Fatal("version gap became queryable after a later commit")
			}
			if committed, err := st.Snapshot(context.Background(), SnapshotOptions{Version: 2, Budget: time.Second}); err != nil || committed.SnapshotWindowVersion != 2 {
				t.Fatalf("committed retry version query failed: %v %#v", err, committed)
			}
		})
	}
}
