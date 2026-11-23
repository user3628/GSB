package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"local/objectstore/internal/model"
	"local/objectstore/internal/store"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.UnixMilli(1_800_000_000_000)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type testHarness struct {
	engine *Engine
	store  *store.Store
	clock  *testClock
	dbPath string
}

func newHarness(t *testing.T, options Options) *testHarness {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "objectstore.db")
	database, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	clock := newTestClock()
	if options.Now == nil {
		options.Now = clock.Now
	}
	if options.VerifyQueueCapacity == 0 {
		options.VerifyQueueCapacity = 32
	}
	service := New(database, options)
	t.Cleanup(func() {
		service.Close()
		_ = database.Close()
	})
	return &testHarness{engine: service, store: database, clock: clock, dbPath: dbPath}
}

func reopenHarness(t *testing.T, dbPath string, options Options) *testHarness {
	t.Helper()
	database, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	clock := newTestClock()
	if options.Now == nil {
		options.Now = clock.Now
	}
	if options.VerifyQueueCapacity == 0 {
		options.VerifyQueueCapacity = 32
	}
	service := New(database, options)
	t.Cleanup(func() {
		service.Close()
		_ = database.Close()
	})
	return &testHarness{engine: service, store: database, clock: clock, dbPath: dbPath}
}

func shaOf(content string) string {
	digest := sha256.Sum256([]byte(content))
	return hex.EncodeToString(digest[:])
}

func createSession(t *testing.T, service *Engine, key string, lowerBound int64) CreateSessionOutput {
	t.Helper()
	result, err := service.CreateSession(context.Background(), CreateSessionInput{
		RequestID:  "create-" + key + "-" + fmt.Sprint(time.Now().UnixNano()),
		ObjectKey:  key,
		LowerBound: lowerBound,
		TTLMS:      3_600_000,
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	return result
}

func submitPart(t *testing.T, service *Engine, sessionID string, partNumber int64, content string, arrivedAt int64) SubmitPartOutput {
	t.Helper()
	result, err := service.SubmitPart(context.Background(), SubmitPartInput{
		RequestID:   fmt.Sprintf("part-%s-%d-%d", sessionID, partNumber, time.Now().UnixNano()),
		SessionID:   sessionID,
		PartNumber:  partNumber,
		Size:        int64(len(content)),
		SHA256:      shaOf(content),
		ArrivedAtMS: &arrivedAt,
		Content:     []byte(content),
	})
	if err != nil {
		t.Fatalf("submit part %d: %v", partNumber, err)
	}
	return result
}

func int64p(value int64) *int64 { return &value }

func TestOutOfOrderIdempotentMerge(t *testing.T) {
	harness := newHarness(t, Options{VerifyBudget: 5 * time.Second, VerifyBudgetSet: true})
	session := createSession(t, harness.engine, "orders/out-of-order.json", 1)

	submitPart(t, harness.engine, session.Session.SessionID, 3, "gamma", 300)
	submitPart(t, harness.engine, session.Session.SessionID, 1, "alpha", 100)
	firstPartTwo, err := harness.engine.SubmitPart(context.Background(), SubmitPartInput{
		RequestID: "first-part-2", SessionID: session.Session.SessionID, PartNumber: 2,
		Size: 5, SHA256: shaOf("beta!"), ArrivedAtMS: int64p(250), Content: []byte("beta!"),
	})
	if err != nil {
		t.Fatalf("submit part 2: %v", err)
	}
	if firstPartTwo.Duplicate {
		t.Fatal("first submission must not be reported as duplicate")
	}
	duplicate, err := harness.engine.SubmitPart(context.Background(), SubmitPartInput{
		RequestID: "duplicate-part-2-retry", SessionID: session.Session.SessionID, PartNumber: 2,
		Size: 5, SHA256: shaOf("beta!"), ArrivedAtMS: int64p(999), Content: []byte("beta!"),
	})
	if err != nil {
		t.Fatalf("duplicate part 2: %v", err)
	}
	if !duplicate.Duplicate || duplicate.Part.DuplicateCount != 1 {
		t.Fatalf("duplicate result = %+v", duplicate)
	}

	parts, err := harness.engine.ListParts(context.Background(), session.Session.SessionID)
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	var numbers []int64
	for _, part := range parts {
		numbers = append(numbers, part.PartNumber)
	}
	if !reflect.DeepEqual(numbers, []int64{1, 2, 3}) {
		t.Fatalf("part order = %v", numbers)
	}

	expectedSHA := shaOf("alphabeta!gamma")
	result, mergeErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-out-of-order", SessionID: session.Session.SessionID,
		ExpectedTotalSize: int64p(int64(len("alphabeta!gamma"))), ExpectedPartCount: int64p(3),
	})
	if mergeErr != nil {
		t.Fatalf("merge: %v", mergeErr)
	}
	if result.Object.SHA256 != expectedSHA {
		t.Fatalf("object digest = %s, want %s", result.Object.SHA256, expectedSHA)
	}
	if result.Object.Generation != 1 || result.Object.PartCount != 3 || result.Object.TotalSize != 15 {
		t.Fatalf("object metadata = %+v", result.Object)
	}
	if !reflect.DeepEqual(result.Verification.VerifiedPartNumbers, []int64{1, 2, 3}) || len(result.Verification.SkippedPartNumbers) != 0 {
		t.Fatalf("verification report = %+v", result.Verification)
	}

	repeated, repeatedErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-out-of-order-repeat", SessionID: session.Session.SessionID,
	})
	if repeatedErr != nil {
		t.Fatalf("repeated merge: %v", repeatedErr)
	}
	if !repeated.AlreadyMerged || repeated.Object.SHA256 != expectedSHA {
		t.Fatalf("repeated merge result = %+v", repeated)
	}

	object, objectErr := harness.engine.GetObject(context.Background(), "orders/out-of-order.json")
	if objectErr != nil || object.SHA256 != expectedSHA {
		t.Fatalf("get object: object=%+v err=%v", object, objectErr)
	}
	var committedSubmitIntents int
	if err := harness.store.DB().QueryRow(`
SELECT COUNT(*) FROM intents WHERE action = 'submit_part' AND status = 'committed'`).Scan(&committedSubmitIntents); err != nil {
		t.Fatalf("count intents: %v", err)
	}
	if committedSubmitIntents != 4 {
		t.Fatalf("committed submit intents = %d, want 4", committedSubmitIntents)
	}
}

func TestPartMismatchPreservesFirstSubmission(t *testing.T) {
	harness := newHarness(t, Options{})
	session := createSession(t, harness.engine, "documents/conflict.txt", 1)
	submitPart(t, harness.engine, session.Session.SessionID, 1, "first", 1)

	_, mismatch := harness.engine.SubmitPart(context.Background(), SubmitPartInput{
		RequestID: "conflicting-part", SessionID: session.Session.SessionID, PartNumber: 1,
		Size: 6, SHA256: shaOf("second"), ArrivedAtMS: int64p(2), Content: []byte("second"),
	})
	if mismatch == nil || mismatch.Code != model.CodePartMismatch {
		t.Fatalf("expected E_PART_MISMATCH, got %v", mismatch)
	}
	if mismatch.Context["preserved"] != true {
		t.Fatalf("mismatch context = %+v", mismatch.Context)
	}
	parts, err := harness.engine.ListParts(context.Background(), session.Session.SessionID)
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	if len(parts) != 1 || parts[0].SHA256 != shaOf("first") || parts[0].DuplicateCount != 0 {
		t.Fatalf("first submission was not preserved: %+v", parts)
	}
}

func TestConcurrentSessionCreationOneWinner(t *testing.T) {
	harness := newHarness(t, Options{})
	const callers = 24
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan struct {
		session CreateSessionOutput
		err     *model.Error
	}, callers)
	for index := 0; index < callers; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			session, err := harness.engine.CreateSession(context.Background(), CreateSessionInput{
				RequestID: fmt.Sprintf("concurrent-create-%d", index),
				ObjectKey: "race/same-key.bin", LowerBound: 1, TTLMS: 3_600_000,
			})
			results <- struct {
				session CreateSessionOutput
				err     *model.Error
			}{session: session, err: err}
		}(index)
	}
	close(start)
	wg.Wait()
	close(results)

	var winnerID string
	winners := 0
	conflicts := 0
	for result := range results {
		if result.err == nil {
			winners++
			winnerID = result.session.Session.SessionID
			continue
		}
		if result.err.Code != model.CodeSessionConflict {
			t.Fatalf("unexpected error: %v", result.err)
		}
		conflicts++
		if result.err.Context["existing_session_id"] == "" {
			t.Fatalf("conflict did not include existing_session_id: %+v", result.err.Context)
		}
	}
	if winners != 1 || conflicts != callers-1 || winnerID == "" {
		t.Fatalf("winners=%d conflicts=%d winner=%s", winners, conflicts, winnerID)
	}
	var sessions int
	if err := harness.store.DB().QueryRow(`SELECT COUNT(*) FROM sessions WHERE object_key = 'race/same-key.bin'`).Scan(&sessions); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if sessions != 1 {
		t.Fatalf("persisted sessions = %d, want 1", sessions)
	}
}

func TestCancelInvalidatesPartsAndAdvancesGeneration(t *testing.T) {
	harness := newHarness(t, Options{})
	first := createSession(t, harness.engine, "generations/object.bin", 1)
	submitPart(t, harness.engine, first.Session.SessionID, 1, "old", 1)

	cancelled, cancelErr := harness.engine.CancelSession(context.Background(), CancelSessionInput{
		RequestID: "cancel-first", SessionID: first.Session.SessionID, Reason: "operator request",
	})
	if cancelErr != nil {
		t.Fatalf("cancel: %v", cancelErr)
	}
	if cancelled.Session.Status != model.SessionCancelled || cancelled.VoidedPartCount != 1 || !cancelled.ReleasedObjectSlot {
		t.Fatalf("cancel result = %+v", cancelled)
	}
	parts, err := harness.engine.ListParts(context.Background(), first.Session.SessionID)
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	if len(parts) != 1 || parts[0].Status != model.PartVoid {
		t.Fatalf("cancelled parts were not voided: %+v", parts)
	}

	second := createSession(t, harness.engine, "generations/object.bin", 1)
	if second.Session.Generation != 2 {
		t.Fatalf("second generation = %d, want 2", second.Session.Generation)
	}
	submitPart(t, harness.engine, second.Session.SessionID, 1, "new", 2)
	merged, mergeErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-second", SessionID: second.Session.SessionID,
	})
	if mergeErr != nil {
		t.Fatalf("merge second generation: %v", mergeErr)
	}
	if merged.Object.Generation != 2 || merged.Object.SHA256 != shaOf("new") {
		t.Fatalf("second generation object = %+v", merged.Object)
	}
	_, mergeOldErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-cancelled", SessionID: first.Session.SessionID,
	})
	if mergeOldErr == nil || mergeOldErr.Code != model.CodeSessionState {
		t.Fatalf("cancelled merge error = %v", mergeOldErr)
	}
}

func TestExpiredSessionIsReclaimedBeforeNewGeneration(t *testing.T) {
	harness := newHarness(t, Options{})
	first, err := harness.engine.CreateSession(context.Background(), CreateSessionInput{
		RequestID: "expiring-create", ObjectKey: "expiry/object.bin", LowerBound: 1, TTLMS: 10,
	})
	if err != nil {
		t.Fatalf("create expiring session: %v", err)
	}
	submitPart(t, harness.engine, first.Session.SessionID, 1, "temporary", 1)
	harness.clock.Advance(20 * time.Millisecond)
	second := createSession(t, harness.engine, "expiry/object.bin", 1)
	if second.ReclaimedExpiredSessionID != first.Session.SessionID {
		t.Fatalf("reclaimed session = %q, want %q", second.ReclaimedExpiredSessionID, first.Session.SessionID)
	}
	if second.Session.Generation != 2 {
		t.Fatalf("new generation = %d, want 2", second.Session.Generation)
	}
	old, getErr := harness.engine.GetSession(context.Background(), first.Session.SessionID)
	if getErr != nil {
		t.Fatalf("get expired session: %v", getErr)
	}
	if old.Status != model.SessionExpired {
		t.Fatalf("old status = %s", old.Status)
	}
	parts, listErr := harness.engine.ListParts(context.Background(), first.Session.SessionID)
	if listErr != nil {
		t.Fatalf("list expired parts: %v", listErr)
	}
	if len(parts) != 1 || parts[0].Status != model.PartVoid {
		t.Fatalf("expired parts = %+v", parts)
	}
}

func TestMergeValidationAndAtomicFailure(t *testing.T) {
	harness := newHarness(t, Options{})
	session := createSession(t, harness.engine, "validation/object.bin", 1)
	submitPart(t, harness.engine, session.Session.SessionID, 1, "aa", 1)
	submitPart(t, harness.engine, session.Session.SessionID, 3, "cc", 3)

	_, missingErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-missing", SessionID: session.Session.SessionID,
	})
	if missingErr == nil || missingErr.Code != model.CodePartMissing {
		t.Fatalf("expected E_PART_MISSING, got %v", missingErr)
	}
	if !reflect.DeepEqual(missingErr.Context["missing_part_numbers"], []int64{2}) {
		t.Fatalf("missing context = %+v", missingErr.Context)
	}

	submitPart(t, harness.engine, session.Session.SessionID, 2, "bb", 2)
	_, sizeErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-size", SessionID: session.Session.SessionID,
		ExpectedTotalSize: int64p(99), ExpectedPartCount: int64p(3),
	})
	if sizeErr == nil || sizeErr.Code != model.CodeSizeMismatch {
		t.Fatalf("expected E_SIZE_MISMATCH, got %v", sizeErr)
	}
	if _, objectErr := harness.engine.GetObject(context.Background(), "validation/object.bin"); objectErr == nil || objectErr.Code != model.CodeObjectNotFound {
		t.Fatalf("object metadata was partially written: %v", objectErr)
	}
	current, getErr := harness.engine.GetSession(context.Background(), session.Session.SessionID)
	if getErr != nil || current.Status != model.SessionActive {
		t.Fatalf("session after failed merges = %+v, err=%v", current, getErr)
	}
}

func TestDigestVerificationFailureAbortsMerge(t *testing.T) {
	harness := newHarness(t, Options{VerifyBudget: 5 * time.Second, VerifyBudgetSet: true})
	session := createSession(t, harness.engine, "verify/object.bin", 1)
	submitPart(t, harness.engine, session.Session.SessionID, 1, "aaaa", 1)
	submitPart(t, harness.engine, session.Session.SessionID, 2, "bbbb", 2)
	if _, err := harness.store.DB().Exec(`UPDATE parts SET content = ? WHERE session_id = ? AND part_number = 2`, []byte("cccc"), session.Session.SessionID); err != nil {
		t.Fatalf("corrupt stored part: %v", err)
	}
	_, mergeErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-corrupt", SessionID: session.Session.SessionID,
	})
	if mergeErr == nil || mergeErr.Code != model.CodePartMismatch {
		t.Fatalf("expected E_PART_MISMATCH, got %v", mergeErr)
	}
	if _, objectErr := harness.engine.GetObject(context.Background(), "verify/object.bin"); objectErr == nil || objectErr.Code != model.CodeObjectNotFound {
		t.Fatalf("object metadata was partially written: %v", objectErr)
	}
	current, getErr := harness.engine.GetSession(context.Background(), session.Session.SessionID)
	if getErr != nil || current.Status != model.SessionActive {
		t.Fatalf("session status after failed verify = %+v err=%v", current, getErr)
	}
	var intentStatus string
	if err := harness.store.DB().QueryRow(`
SELECT status FROM intents WHERE action = 'merge' ORDER BY seq DESC LIMIT 1`).Scan(&intentStatus); err != nil {
		t.Fatalf("read merge intent: %v", err)
	}
	if intentStatus != model.IntentRejected {
		t.Fatalf("merge intent status = %s, want rejected", intentStatus)
	}
}

func TestZeroBudgetDegradesToDeterministicSkipList(t *testing.T) {
	harness := newHarness(t, Options{VerifyBudget: 0, VerifyBudgetSet: true})
	session := createSession(t, harness.engine, "budget/object.bin", 0)
	submitPart(t, harness.engine, session.Session.SessionID, 2, "two", 2)
	submitPart(t, harness.engine, session.Session.SessionID, 0, "zero", 0)
	submitPart(t, harness.engine, session.Session.SessionID, 1, "one", 1)
	result, mergeErr := harness.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-zero-budget", SessionID: session.Session.SessionID,
	})
	if mergeErr != nil {
		t.Fatalf("merge: %v", mergeErr)
	}
	if len(result.Verification.VerifiedPartNumbers) != 0 || !reflect.DeepEqual(result.Verification.SkippedPartNumbers, []int64{0, 1, 2}) {
		t.Fatalf("verification report = %+v", result.Verification)
	}
	if !result.Verification.Degraded {
		t.Fatal("zero budget report must be degraded")
	}
}

func TestVerifierBoundedQueue(t *testing.T) {
	verifier := NewVerifier(1, 0, time.Now)
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	verifier.verifyFn = func(context.Context, model.Part) error {
		started <- struct{}{}
		<-gate
		return nil
	}
	first, err := verifier.Submit(context.Background(), model.Part{PartNumber: 1})
	if err != nil {
		t.Fatalf("submit first: %v", err)
	}
	<-started
	_, queueErr := verifier.Submit(context.Background(), model.Part{PartNumber: 2})
	if queueErr == nil || queueErr.(*model.Error).Code != model.CodeVerifyQueueFull {
		t.Fatalf("expected E_VERIFY_QUEUE_FULL, got %v", queueErr)
	}
	close(gate)
	if result := <-first; result.err != nil {
		t.Fatalf("first verification: %v", result.err)
	}
	verifier.Close()
}

func TestVerifierDeadlineSkipsQueuedSuffix(t *testing.T) {
	clock := newTestClock()
	verifier := NewVerifier(1, 2, clock.Now)
	gate := make(chan struct{})
	started := make(chan struct{}, 1)
	verifier.verifyFn = func(_ context.Context, part model.Part) error {
		if part.PartNumber == 1 {
			started <- struct{}{}
			<-gate
		}
		return nil
	}
	type verificationOutcome struct {
		report model.VerificationReport
		err    error
	}
	done := make(chan verificationOutcome, 1)
	go func() {
		report, err := verifier.Verify(context.Background(), []model.Part{
			{PartNumber: 1}, {PartNumber: 2}, {PartNumber: 3},
		}, 10*time.Millisecond)
		done <- verificationOutcome{report: report, err: err}
	}()
	<-started
	clock.Advance(20 * time.Millisecond)
	close(gate)
	outcome := <-done
	if outcome.err != nil {
		t.Fatalf("verify: %v", outcome.err)
	}
	if !reflect.DeepEqual(outcome.report.VerifiedPartNumbers, []int64{1}) {
		t.Fatalf("verified = %v", outcome.report.VerifiedPartNumbers)
	}
	if !reflect.DeepEqual(outcome.report.SkippedPartNumbers, []int64{2, 3}) {
		t.Fatalf("skipped = %v", outcome.report.SkippedPartNumbers)
	}
	if !outcome.report.Degraded {
		t.Fatal("deadline-truncated report must be degraded")
	}
	verifier.Close()
}

func TestRecoveryUsesOnlyCompleteCommitsAndReportsPendingIntent(t *testing.T) {
	harness := newHarness(t, Options{})
	session := createSession(t, harness.engine, "recovery/object.bin", 1)
	submitPart(t, harness.engine, session.Session.SessionID, 2, "two", 2)
	submitPart(t, harness.engine, session.Session.SessionID, 1, "one", 1)
	_, pendingErr := harness.store.AppendIntent(context.Background(), model.Intent{
		RequestID: "crashed-merge", Action: model.ActionMerge,
		SessionID: session.Session.SessionID, ObjectKey: "recovery/object.bin",
		PayloadJSON: `{"simulated":"crash_after_intent"}`,
	}, harness.clock.Now().UnixMilli())
	if pendingErr != nil {
		t.Fatalf("append pending intent: %v", pendingErr)
	}
	harness.engine.Close()
	if err := harness.store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := reopenHarness(t, harness.dbPath, Options{})
	report, recoverErr := reopened.engine.Recover(context.Background())
	if recoverErr != nil {
		t.Fatalf("recover: %v", recoverErr)
	}
	if len(report.InterruptedIntents) != 1 || report.InterruptedIntents[0].Applied {
		t.Fatalf("interrupted intents = %+v", report.InterruptedIntents)
	}
	if report.InterruptedIntents[0].RecoveryStatus != model.IntentAbandoned {
		t.Fatalf("recovery status = %s", report.InterruptedIntents[0].RecoveryStatus)
	}
	if len(report.Objects) != 0 {
		t.Fatalf("pending merge intent leaked into object metadata: %+v", report.Objects)
	}
	if len(report.Sessions) != 1 || report.Sessions[0].Status != model.SessionActive {
		t.Fatalf("recovered sessions = %+v", report.Sessions)
	}
	if len(report.Parts) != 2 {
		t.Fatalf("recovered parts = %+v", report.Parts)
	}
	if !report.Checks.Passed {
		t.Fatalf("recovery checks failed: %+v", report.Checks)
	}

	merged, mergeErr := reopened.engine.MergeSession(context.Background(), MergeSessionInput{
		RequestID: "merge-after-recovery", SessionID: session.Session.SessionID,
	})
	if mergeErr != nil {
		t.Fatalf("merge after recovery: %v", mergeErr)
	}
	if merged.Object.SHA256 != shaOf("onetwo") {
		t.Fatalf("merged object sha = %s", merged.Object.SHA256)
	}

	reopened.engine.Close()
	if err := reopened.store.Close(); err != nil {
		t.Fatalf("close reopened store: %v", err)
	}
	final := reopenHarness(t, harness.dbPath, Options{})
	finalReport, finalErr := final.engine.Recover(context.Background())
	if finalErr != nil {
		t.Fatalf("final recovery: %v", finalErr)
	}
	if len(finalReport.Objects) != 1 || finalReport.Objects[0].SHA256 != shaOf("onetwo") {
		t.Fatalf("final objects = %+v", finalReport.Objects)
	}
	if finalReport.Sessions[0].Status != model.SessionCompleted {
		t.Fatalf("final session = %+v", finalReport.Sessions[0])
	}
	if !finalReport.Checks.Passed {
		t.Fatalf("final checks = %+v", finalReport.Checks)
	}
	loaded, loadErr := final.engine.GetRecoveryReport(context.Background(), finalReport.ReportID)
	if loadErr != nil || loaded.ReportID != finalReport.ReportID || len(loaded.Objects) != 1 {
		t.Fatalf("load recovery report: report=%+v err=%v", loaded, loadErr)
	}
}

func TestRecoveryWaitsForInFlightMutations(t *testing.T) {
	harness := newHarness(t, Options{})
	const creates = 30
	const recoveries = 6
	start := make(chan struct{})
	var wg sync.WaitGroup
	createErrors := make(chan *model.Error, creates)
	recoveryErrors := make(chan *model.Error, recoveries)
	for index := 0; index < creates; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			_, err := harness.engine.CreateSession(context.Background(), CreateSessionInput{
				RequestID: fmt.Sprintf("recovery-race-create-%d", index),
				ObjectKey: fmt.Sprintf("recovery-race/%d.bin", index), LowerBound: 1, TTLMS: 3_600_000,
			})
			createErrors <- err
		}(index)
	}
	for index := 0; index < recoveries; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := harness.engine.Recover(context.Background())
			recoveryErrors <- err
		}()
	}
	close(start)
	wg.Wait()
	close(createErrors)
	close(recoveryErrors)
	for err := range createErrors {
		if err != nil {
			t.Fatalf("concurrent create failed: %v", err)
		}
	}
	for err := range recoveryErrors {
		if err != nil {
			t.Fatalf("concurrent recovery failed: %v", err)
		}
	}
	report, recoverErr := harness.engine.Recover(context.Background())
	if recoverErr != nil {
		t.Fatalf("final recovery: %v", recoverErr)
	}
	if len(report.InterruptedIntents) != 0 {
		t.Fatalf("in-flight intents were treated as interrupted: %+v", report.InterruptedIntents)
	}
	if report.IntentCounts[model.IntentAbandoned] != 0 {
		t.Fatalf("abandoned intent count = %d", report.IntentCounts[model.IntentAbandoned])
	}
}
