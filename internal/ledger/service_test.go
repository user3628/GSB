package ledger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

type testEnvelope struct {
	OK            bool            `json:"ok"`
	RequestID     string          `json:"request_id"`
	LedgerVersion int64           `json:"ledger_version"`
	Data          json.RawMessage `json:"data"`
	Error         *ErrorPayload   `json:"error"`
}

func decodeEnvelope(t *testing.T, resp StoredResponse) testEnvelope {
	t.Helper()
	var env testEnvelope
	if err := json.Unmarshal(resp.Body, &env); err != nil {
		t.Fatalf("decode response body %s: %v", string(resp.Body), err)
	}
	return env
}

func testStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()
	if _, err := s.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	return s, ctx
}

func readSnapshotFile(t *testing.T, name string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "samples", name))
	if err != nil {
		t.Fatalf("read sample %s: %v", name, err)
	}
	var snap Snapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		t.Fatalf("decode sample %s: %v", name, err)
	}
	return snap
}

func submitSampleSnapshot(t *testing.T, s *Store, ctx context.Context, requestID, file string) SnapshotResult {
	t.Helper()
	resp, err := s.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: requestID, Snapshot: readSnapshotFile(t, file)})
	if err != nil {
		t.Fatalf("submit snapshot: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("snapshot status=%d body=%s", resp.Status, resp.Body)
	}
	env := decodeEnvelope(t, resp)
	var data SnapshotResult
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("decode snapshot data: %v", err)
	}
	return data
}

func seedSampleThroughSettlement(t *testing.T, s *Store, ctx context.Context) (*SettlementReport, *LedgerState) {
	t.Helper()
	submitSampleSnapshot(t, s, ctx, "snapshot-v1", "snapshot-v1.json")
	submitSampleSnapshot(t, s, ctx, "snapshot-v2", "snapshot-v2.json")
	eventsBody, err := os.ReadFile(filepath.Join("..", "..", "samples", "events.json"))
	if err != nil {
		t.Fatal(err)
	}
	var events EventSubmission
	if err := json.Unmarshal(eventsBody, &events); err != nil {
		t.Fatal(err)
	}
	eventResp, err := s.SubmitEvents(ctx, events)
	if err != nil {
		t.Fatal(err)
	}
	if eventResp.Status != 200 {
		t.Fatalf("events status=%d body=%s", eventResp.Status, eventResp.Body)
	}
	settleBody, _ := os.ReadFile(filepath.Join("..", "..", "samples", "settle.json"))
	var settleReq SettlementRequest
	if err := json.Unmarshal(settleBody, &settleReq); err != nil {
		t.Fatal(err)
	}
	settleResp, err := s.Settle(ctx, settleReq)
	if err != nil {
		t.Fatal(err)
	}
	if settleResp.Status != 200 {
		t.Fatalf("settle status=%d body=%s", settleResp.Status, settleResp.Body)
	}
	env := decodeEnvelope(t, settleResp)
	var report SettlementReport
	if err := json.Unmarshal(env.Data, &report); err != nil {
		t.Fatal(err)
	}
	state, err := s.LastState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return &report, state
}

func TestSampleSegmentedInterestAndPoolRisk(t *testing.T) {
	s, ctx := testStore(t)
	report, state := seedSampleThroughSettlement(t, s, ctx)
	if state.AsOfMinute != MinuteOf(1767226200000) {
		t.Fatalf("as_of_minute=%d", state.AsOfMinute)
	}
	if state.LedgerVersion != 4 {
		t.Fatalf("ledger version=%d", state.LedgerVersion)
	}
	a, ok := FindTenant(state, "tenant-a")
	if !ok {
		t.Fatal("tenant-a missing")
	}
	if a.PrincipalOutstandingMilli != 475004 || a.UnsettledInterestMicros != 77349 || a.RoundingErrorNanos != 1100 {
		t.Fatalf("tenant-a state unexpected: principal=%d interest=%d rounding=%d", a.PrincipalOutstandingMilli, a.UnsettledInterestMicros, a.RoundingErrorNanos)
	}
	if len(a.RateSegments) != 2 {
		t.Fatalf("tenant-a rate segments=%+v", a.RateSegments)
	}
	if a.RateSegments[0].RateTier != "standard" || a.RateSegments[0].StartMinute != MinuteOf(1767225600000) || a.RateSegments[0].EndMinuteExclusive != MinuteOf(1767225600000)+6 {
		t.Fatalf("tenant-a first segment=%+v", a.RateSegments[0])
	}
	if a.RateSegments[1].RateTier != "peak" || a.RateSegments[1].StartMinute != MinuteOf(1767225600000)+6 || a.RateSegments[1].EndMinuteExclusive != MinuteOf(1767225600000)+10 {
		t.Fatalf("tenant-a second segment=%+v", a.RateSegments[1])
	}
	b, _ := FindTenant(state, "tenant-b")
	if b.PrincipalOutstandingMilli != 294998 || b.UnsettledInterestMicros != 26950 {
		t.Fatalf("tenant-b state unexpected: principal=%d interest=%d", b.PrincipalOutstandingMilli, b.UnsettledInterestMicros)
	}
	c, _ := FindTenant(state, "tenant-c")
	if c.PrincipalOutstandingMilli != 80003 || c.UnsettledInterestMicros != 6400 {
		t.Fatalf("tenant-c state unexpected: principal=%d interest=%d", c.PrincipalOutstandingMilli, c.UnsettledInterestMicros)
	}
	if len(c.RateSegments) != 1 || c.RateSegments[0].StartMinute != MinuteOf(1767225720000) || c.RateSegments[0].EndMinuteExclusive != MinuteOf(1767225600000)+10 {
		t.Fatalf("tenant-c segments=%+v", c.RateSegments)
	}
	if len(a.ReturnAllocations) != 2 {
		t.Fatalf("tenant-a return allocations=%+v", a.ReturnAllocations)
	}
	last := a.ReturnAllocations[1]
	if last.EventSettleSeq != "e0006" || len(last.Payments) == 0 || last.Payments[0].LotID != "e0001" || last.Payments[0].PrincipalPaidMilli != 15001 {
		t.Fatalf("priority repayment did not target old high-priority lot: %+v", last)
	}
	p, _ := FindPool(state, "pool-a")
	if p.UsedMicros != 850115699 || p.UsedRatioFixed != "0.944573" || p.UsedRatioPPM != 944573 {
		t.Fatalf("pool used unexpected: used=%d ratio=%s ppm=%d", p.UsedMicros, p.UsedRatioFixed, p.UsedRatioPPM)
	}
	if !p.OvercommitRisk || p.ExcessOverThresholdMicros != 40115699 || p.TriggerTenantID != "tenant-a" {
		t.Fatalf("pool risk unexpected: %+v", p)
	}
	if p.CrossTenantExposureMicros != 420106299 {
		t.Fatalf("cross exposure=%d", p.CrossTenantExposureMicros)
	}
	if report.LedgerVersion != state.LedgerVersion || report.StateDigest != state.StateDigest {
		t.Fatalf("report/state mismatch")
	}
	if report.StateDigest != "sha256:4f1c07087de7a1d3b85f19f07da8dc8852b54c810122c86d49f542ab5d40117b" {
		t.Fatalf("sample state digest drifted: %s", report.StateDigest)
	}
	if report.CalculationDigest != "sha256:3c20404ce24ca9d3708c59511aab74c296ec8152d233d5fadff4b12953a150a1" {
		t.Fatalf("sample calculation digest drifted: %s", report.CalculationDigest)
	}
	calculated, err := StateDigest(state)
	if err != nil || calculated != state.StateDigest {
		t.Fatalf("state digest mismatch calculated=%s stored=%s err=%v", calculated, state.StateDigest, err)
	}
	for _, detail := range report.Segments {
		if detail.TenantID == "tenant-a" && len(detail.InterestSteps) != 12 {
			t.Fatalf("tenant-a steps=%d, want 12", len(detail.InterestSteps))
		}
	}
}

func TestBorrowOvercommitRejectedAndIdempotent(t *testing.T) {
	s, ctx := testStore(t)
	_, stateBefore := seedSampleThroughSettlement(t, s, ctx)
	body, _ := os.ReadFile(filepath.Join("..", "..", "samples", "borrow-overcommit.json"))
	var req BorrowRequest
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatal(err)
	}
	resp, err := s.Borrow(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 409 {
		t.Fatalf("status=%d body=%s", resp.Status, resp.Body)
	}
	env := decodeEnvelope(t, resp)
	if env.Error == nil || env.Error.Code != CodePoolOvercommit {
		t.Fatalf("error=%+v", env.Error)
	}
	if env.Error.Context["trigger_tenant_id"] != "tenant-c" {
		t.Fatalf("trigger tenant=%v", env.Error.Context["trigger_tenant_id"])
	}
	stateAfter, _ := s.LastState(ctx)
	if stateAfter.LedgerVersion != stateBefore.LedgerVersion || stateAfter.StateDigest != stateBefore.StateDigest {
		t.Fatalf("rejected borrow changed ledger")
	}
	resp2, err := s.Borrow(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body) != string(resp2.Body) || resp2.Status != 409 {
		t.Fatalf("idempotent borrow replay mismatch\n1=%s\n2=%s", resp.Body, resp2.Body)
	}
	conflicting := req
	conflicting.SharesMilli++
	resp3, err := s.Borrow(ctx, conflicting)
	if err != nil {
		t.Fatal(err)
	}
	env3 := decodeEnvelope(t, resp3)
	if resp3.Status != 409 || env3.Error == nil || env3.Error.Code != CodeConflictingRequest {
		t.Fatalf("conflicting request response=%d %s", resp3.Status, resp3.Body)
	}
}

func TestBorrowDuplicateSettleSeqIsCountedOnce(t *testing.T) {
	s, ctx := testStore(t)
	submitSampleSnapshot(t, s, ctx, "snapshot-v1", "snapshot-v1.json")
	req := BorrowRequest{
		RequestID:   "borrow-duplicate-1",
		SettleSeq:   "duplicate-borrow",
		TenantID:    "tenant-a",
		SharesMilli: 1000,
		TimestampMS: 1767225600000,
	}
	resp, err := s.Borrow(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 200 {
		t.Fatalf("first borrow status=%d body=%s", resp.Status, resp.Body)
	}
	firstEnv := decodeEnvelope(t, resp)
	var first BorrowDecision
	if err := json.Unmarshal(firstEnv.Data, &first); err != nil {
		t.Fatal(err)
	}
	if !first.Accepted {
		t.Fatalf("first borrow was not accepted: %+v", first)
	}
	stateAfterFirst, err := s.LastState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stateAfterFirst.LedgerVersion != 2 || stateAfterFirst.EventCount != 1 {
		t.Fatalf("first borrow state version=%d events=%d", stateAfterFirst.LedgerVersion, stateAfterFirst.EventCount)
	}

	replay := req
	replay.RequestID = "borrow-duplicate-2"
	replayResp, err := s.Borrow(ctx, replay)
	if err != nil {
		t.Fatal(err)
	}
	if replayResp.Status != 200 {
		t.Fatalf("duplicate borrow status=%d body=%s", replayResp.Status, replayResp.Body)
	}
	replayEnv := decodeEnvelope(t, replayResp)
	var repeated BorrowDecision
	if err := json.Unmarshal(replayEnv.Data, &repeated); err != nil {
		t.Fatal(err)
	}
	if !repeated.Accepted || replayEnv.LedgerVersion != stateAfterFirst.LedgerVersion {
		t.Fatalf("duplicate borrow response version=%d decision=%+v", replayEnv.LedgerVersion, repeated)
	}
	stateAfterReplay, err := s.LastState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stateAfterReplay.LedgerVersion != stateAfterFirst.LedgerVersion ||
		stateAfterReplay.StateDigest != stateAfterFirst.StateDigest ||
		stateAfterReplay.EventCount != 1 {
		t.Fatalf("duplicate borrow changed ledger: before=%d/%s after=%d/%s events=%d",
			stateAfterFirst.LedgerVersion, stateAfterFirst.StateDigest,
			stateAfterReplay.LedgerVersion, stateAfterReplay.StateDigest, stateAfterReplay.EventCount)
	}

	conflicting := replay
	conflicting.RequestID = "borrow-duplicate-3"
	conflicting.SharesMilli++
	conflictResp, err := s.Borrow(ctx, conflicting)
	if err != nil {
		t.Fatal(err)
	}
	conflictEnv := decodeEnvelope(t, conflictResp)
	if conflictResp.Status != 409 || conflictEnv.Error == nil || conflictEnv.Error.Code != CodeConflictingSettle {
		t.Fatalf("conflicting settle response=%d %s", conflictResp.Status, conflictResp.Body)
	}
}

func TestRepeatedSettlementHasStableCalculationDigest(t *testing.T) {
	s, ctx := testStore(t)
	first, _ := seedSampleThroughSettlement(t, s, ctx)
	resp, err := s.Settle(ctx, SettlementRequest{RequestID: "settle-repeat", ThroughMS: 1767226200000})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 200 {
		t.Fatalf("repeat settle status=%d body=%s", resp.Status, resp.Body)
	}
	var repeat SettlementReport
	if err := json.Unmarshal(decodeEnvelope(t, resp).Data, &repeat); err != nil {
		t.Fatal(err)
	}
	if repeat.CalculationDigest == "" || repeat.CalculationDigest != first.CalculationDigest {
		t.Fatalf("calculation digest changed: first=%s repeat=%s", first.CalculationDigest, repeat.CalculationDigest)
	}
	t.Logf("settlement calculation digest=%s", first.CalculationDigest)
	for i := range first.Segments {
		if first.Segments[i].UnsettledInterestMicros != repeat.Segments[i].UnsettledInterestMicros ||
			first.Segments[i].CumulativeRoundingNanos != repeat.Segments[i].CumulativeRoundingNanos {
			t.Fatalf("tenant settlement changed on repeat: %+v vs %+v", first.Segments[i], repeat.Segments[i])
		}
	}
}

func TestProjectionIndependentOfFactOrdering(t *testing.T) {
	snap1 := readSnapshotFile(t, "snapshot-v1.json")
	snap2 := readSnapshotFile(t, "snapshot-v2.json")
	body, err := os.ReadFile(filepath.Join("..", "..", "samples", "events.json"))
	if err != nil {
		t.Fatal(err)
	}
	var submission EventSubmission
	if err := json.Unmarshal(body, &submission); err != nil {
		t.Fatal(err)
	}
	snapsA := []FactSnapshot{{Snapshot: snap1, CommitVersion: 1}, {Snapshot: snap2, CommitVersion: 2}}
	snapsB := []FactSnapshot{{Snapshot: snap2, CommitVersion: 2}, {Snapshot: snap1, CommitVersion: 1}}
	eventsA := make([]FactEvent, 0, len(submission.Events))
	for _, ev := range submission.Events {
		d, _ := EventDigest(ev)
		eventsA = append(eventsA, FactEvent{Event: ev, ContentDigest: d, CommitVersion: 3})
	}
	eventsB := append([]FactEvent(nil), eventsA...)
	for i, j := 0, len(eventsB)-1; i < j; i, j = i+1, j-1 {
		eventsB[i], eventsB[j] = eventsB[j], eventsB[i]
	}
	asOf := MinuteOf(1767226200000)
	meta := ProjectionMeta{LedgerVersion: 4, LastSettledThroughMS: 1767226200000}
	a, err := BuildProjection(snapsA, eventsA, asOf, meta, true)
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildProjection(snapsB, eventsB, asOf, meta, true)
	if err != nil {
		t.Fatal(err)
	}
	if a.State.StateDigest != b.State.StateDigest {
		t.Fatalf("state digest depends on fact ordering: %s vs %s", a.State.StateDigest, b.State.StateDigest)
	}
	reportA := BuildSettlementReport(SettlementRequest{RequestID: "a", ThroughMS: 1767226200000}, a.State, a)
	reportB := BuildSettlementReport(SettlementRequest{RequestID: "b", ThroughMS: 1767226200000}, b.State, b)
	if reportA.CalculationDigest != reportB.CalculationDigest {
		t.Fatalf("calculation digest depends on fact ordering: %s vs %s", reportA.CalculationDigest, reportB.CalculationDigest)
	}
}

func simpleSnapshot(version, generated int64, total, reserve, threshold, tolerance int64, tenants []Tenant) Snapshot {
	return Snapshot{Version: version, GeneratedAtMS: generated, Pools: []Pool{{ID: "p", TotalSharesMilli: total, ReserveSharesMilli: reserve, OvercommitThresholdPPM: threshold, RelativeTolerancePPM: tolerance}}, Tenants: tenants, RateTiers: map[string]RateTier{"r": {RatePPM: 1}}}
}

func TestSnapshotValidationStaleAndAtomic(t *testing.T) {
	s, ctx := testStore(t)
	snap := simpleSnapshot(1, 0, 1000, 0, 900000, 0, []Tenant{{ID: "a", PoolID: "p", BaseSharesMilli: 100, CreditLimitMilli: 1000, RateTier: "r", RepaymentPriority: 1}})
	first, err := s.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: "snap-1", Snapshot: snap})
	if err != nil || first.Status != 200 {
		t.Fatalf("first snapshot err=%v status=%d body=%s", err, first.Status, first.Body)
	}
	before, _ := s.LastState(ctx)
	stale, err := s.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: "snap-stale", Snapshot: snap})
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnvelope(t, stale)
	if env.Error == nil || env.Error.Code != CodeStaleSnapshot {
		t.Fatalf("stale response=%s", stale.Body)
	}
	after, _ := s.LastState(ctx)
	if after.LedgerVersion != before.LedgerVersion || after.StateDigest != before.StateDigest {
		t.Fatal("stale snapshot changed state")
	}
	bad := snap
	bad.Version = 2
	bad.Digest = "sha256:deadbeef"
	badResp, err := s.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: "snap-bad", Snapshot: bad})
	if err != nil {
		t.Fatal(err)
	}
	envBad := decodeEnvelope(t, badResp)
	if envBad.Error == nil || envBad.Error.Code != CodeLedgerInvalid {
		t.Fatalf("bad digest response=%s", badResp.Body)
	}
	after2, _ := s.LastState(ctx)
	if after2.LedgerVersion != before.LedgerVersion || after2.StateDigest != before.StateDigest {
		t.Fatal("invalid snapshot changed state")
	}
}

func TestPriorityConflictIsDistinctFromPoolOvercommit(t *testing.T) {
	s, ctx := testStore(t)
	tenants := []Tenant{
		{ID: "high", PoolID: "p", BaseSharesMilli: 0, CreditLimitMilli: 100000, RateTier: "r", RepaymentPriority: 1},
		{ID: "low", PoolID: "p", BaseSharesMilli: 0, CreditLimitMilli: 1000000, RateTier: "r", RepaymentPriority: 2},
	}
	snap := simpleSnapshot(1, 0, 1000000, 0, 1000000, 0, tenants)
	if resp, err := s.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: "snap-priority", Snapshot: snap}); err != nil || resp.Status != 200 {
		t.Fatalf("snapshot err=%v body=%s", err, resp.Body)
	}
	req := BorrowRequest{RequestID: "borrow-priority", SettleSeq: "bp1", TenantID: "low", SharesMilli: 950000, TimestampMS: 0}
	resp, err := s.Borrow(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnvelope(t, resp)
	if resp.Status != 409 || env.Error == nil || env.Error.Code != CodePriorityConflict {
		t.Fatalf("priority response=%d %s", resp.Status, resp.Body)
	}
}

func TestPoolToleranceBoundaryAndLexicographicTrigger(t *testing.T) {
	pools := map[string]Pool{"p": {
		ID:                     "p",
		TotalSharesMilli:       1000000,
		ReserveSharesMilli:     0,
		OvercommitThresholdPPM: 900000,
		RelativeTolerancePPM:   1000,
	}}
	atLimit := []TenantState{
		{TenantID: "z", PoolID: "p", PrincipalOutstandingMilli: 450450, CreditLimitMilli: 1000000},
		{TenantID: "a", PoolID: "p", PrincipalOutstandingMilli: 450450, CreditLimitMilli: 1000000},
	}
	states, err := buildPoolStates(pools, atLimit, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if states[0].ToleranceLimitMicros != 900900000 {
		t.Fatalf("tolerance limit=%d", states[0].ToleranceLimitMicros)
	}
	if states[0].UsedMicros != 900900000 || states[0].OvercommitRisk {
		t.Fatalf("equality at relative tolerance boundary must not overcommit: %+v", states[0])
	}
	over := []TenantState{
		{TenantID: "z", PoolID: "p", PrincipalOutstandingMilli: 450451, CreditLimitMilli: 1000000},
		{TenantID: "a", PoolID: "p", PrincipalOutstandingMilli: 450451, CreditLimitMilli: 1000000},
	}
	states, err = buildPoolStates(pools, over, Snapshot{})
	if err != nil {
		t.Fatal(err)
	}
	if !states[0].OvercommitRisk || states[0].TriggerTenantID != "a" {
		t.Fatalf("overcommit/tie-break unexpected: %+v", states[0])
	}
}

func TestEventsRollbackDuplicateAndCredit(t *testing.T) {
	s, ctx := testStore(t)
	snap := simpleSnapshot(1, 0, 100000, 0, 1000000, 0, []Tenant{{ID: "a", PoolID: "p", BaseSharesMilli: 0, CreditLimitMilli: 5000, RateTier: "r", RepaymentPriority: 1}})
	if resp, err := s.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: "snap-events", Snapshot: snap}); err != nil || resp.Status != 200 {
		t.Fatalf("snapshot: %v %s", err, resp.Body)
	}
	before, _ := s.LastState(ctx)
	badBatch := EventSubmission{RequestID: "events-bad", Events: []Event{
		{SettleSeq: "x1", TenantID: "a", Type: "borrow", SharesMilli: 100, TimestampMS: 0},
		{SettleSeq: "x2", TenantID: "missing", Type: "borrow", SharesMilli: 100, TimestampMS: 1},
	}}
	badResp, err := s.SubmitEvents(ctx, badBatch)
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnvelope(t, badResp)
	if env.Error == nil || env.Error.Code != CodeEventInvalid {
		t.Fatalf("bad batch=%s", badResp.Body)
	}
	after, _ := s.LastState(ctx)
	if after.LedgerVersion != before.LedgerVersion {
		t.Fatal("bad batch changed ledger version")
	}
	var count int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM events`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("event count=%d err=%v", count, err)
	}
	good := EventSubmission{RequestID: "events-good", Events: []Event{{SettleSeq: "x1", TenantID: "a", Type: "borrow", SharesMilli: 100, TimestampMS: 0}}}
	goodResp, err := s.SubmitEvents(ctx, good)
	if err != nil || decodeEnvelope(t, goodResp).Error != nil {
		t.Fatalf("good events: %v %s", err, goodResp.Body)
	}
	sameAgain, err := s.SubmitEvents(ctx, good)
	if err != nil {
		t.Fatal(err)
	}
	if string(goodResp.Body) != string(sameAgain.Body) {
		t.Fatal("request idempotency body mismatch")
	}
	duplicate := EventSubmission{RequestID: "events-duplicate", Events: good.Events}
	dupResp, err := s.SubmitEvents(ctx, duplicate)
	if err != nil {
		t.Fatal(err)
	}
	var ingest EventIngestResult
	if err := json.Unmarshal(decodeEnvelope(t, dupResp).Data, &ingest); err != nil {
		t.Fatal(err)
	}
	if ingest.NewEventCount != 0 || len(ingest.DuplicateSettleSeqs) != 1 {
		t.Fatalf("duplicate result=%+v", ingest)
	}
	conflict := EventSubmission{RequestID: "events-conflict", Events: []Event{{SettleSeq: "x1", TenantID: "a", Type: "borrow", SharesMilli: 101, TimestampMS: 0}}}
	conflictResp, err := s.SubmitEvents(ctx, conflict)
	if err != nil {
		t.Fatal(err)
	}
	envConflict := decodeEnvelope(t, conflictResp)
	if envConflict.Error == nil || envConflict.Error.Code != CodeConflictingSettle {
		t.Fatalf("conflicting settle=%s", conflictResp.Body)
	}
	credit := EventSubmission{RequestID: "events-credit", Events: []Event{{SettleSeq: "x2", TenantID: "a", Type: "borrow", SharesMilli: 6000, TimestampMS: 1}}}
	creditResp, err := s.SubmitEvents(ctx, credit)
	if err != nil {
		t.Fatal(err)
	}
	envCredit := decodeEnvelope(t, creditResp)
	if envCredit.Error == nil || envCredit.Error.Code != CodeCreditExceeded {
		t.Fatalf("credit exceeded=%s", creditResp.Body)
	}
	if _, ok := envCredit.Error.Context["remaining_credit_micros"]; !ok {
		t.Fatalf("missing remaining credit context: %+v", envCredit.Error.Context)
	}
}

func TestEventBeforeDeclarationIsEventInvalid(t *testing.T) {
	s, ctx := testStore(t)
	resp, err := s.SubmitEvents(ctx, EventSubmission{RequestID: "events-no-declaration", Events: []Event{{
		SettleSeq: "nodecl", TenantID: "missing", Type: "borrow", SharesMilli: 1, TimestampMS: 0,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	env := decodeEnvelope(t, resp)
	if env.Error == nil || env.Error.Code != CodeEventInvalid {
		t.Fatalf("event without declaration response=%d %s", resp.Status, resp.Body)
	}
}

func TestConcurrentIdempotentSettlementAndReaders(t *testing.T) {
	s, ctx := testStore(t)
	submitSampleSnapshot(t, s, ctx, "snapshot-v1", "snapshot-v1.json")
	submitSampleSnapshot(t, s, ctx, "snapshot-v2", "snapshot-v2.json")
	eventsBody, _ := os.ReadFile(filepath.Join("..", "..", "samples", "events.json"))
	var events EventSubmission
	_ = json.Unmarshal(eventsBody, &events)
	if resp, err := s.SubmitEvents(ctx, events); err != nil || resp.Status != 200 {
		t.Fatalf("events: %v %s", err, resp.Body)
	}
	req := SettlementRequest{RequestID: "settle-concurrent", ThroughMS: 1767226200000}
	const writers = 8
	var wg sync.WaitGroup
	responses := make([]StoredResponse, writers)
	errs := make([]error, writers)
	stopReaders := make(chan struct{})
	errorCh := make(chan error, 16)
	var readerWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				state, err := s.LastState(ctx)
				if err != nil {
					errorCh <- err
					return
				}
				calc, err := StateDigest(state)
				if err != nil || calc != state.StateDigest {
					errorCh <- fmt.Errorf("digest invariant failed: stored=%s calc=%s err=%v", state.StateDigest, calc, err)
					return
				}
			}
		}()
	}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			responses[i], errs[i] = s.Settle(ctx, req)
		}(i)
	}
	wg.Wait()
	close(stopReaders)
	readerWG.Wait()
	select {
	case err := <-errorCh:
		t.Fatal(err)
	default:
	}
	for i := 0; i < writers; i++ {
		if errs[i] != nil {
			t.Fatalf("writer %d: %v", i, errs[i])
		}
		if responses[i].Status != 200 || string(responses[i].Body) != string(responses[0].Body) {
			t.Fatalf("writer %d mismatch status=%d body=%s", i, responses[i].Status, responses[i].Body)
		}
	}
	var requestCount int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM requests WHERE request_id='settle-concurrent'`).Scan(&requestCount); err != nil || requestCount != 1 {
		t.Fatalf("request count=%d err=%v", requestCount, err)
	}
}

func TestRecoveryAbortsDurablePendingIntent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = s.Recover(ctx)
	submitSampleSnapshot(t, s, ctx, "snapshot-v1", "snapshot-v1.json")
	submitSampleSnapshot(t, s, ctx, "snapshot-v2", "snapshot-v2.json")
	before, _ := s.LastState(ctx)
	_, err = s.DB().ExecContext(ctx, `INSERT INTO intents(intent_id, request_id, kind, payload_digest, target_ledger_version, status, phase, detail_json, created_ms, updated_ms)
		VALUES('intent-pending','pending-request','settle','x',999,'pending','crash_rebuild_started','{}',1,1)`)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, err := reopened.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.AbortedIntentCount != 1 || len(report.Actions) != 1 || report.Actions[0].Action != "aborted_uncommitted_intent" {
		t.Fatalf("recovery report=%+v", report)
	}
	after, _ := reopened.LastState(ctx)
	if after.LedgerVersion != before.LedgerVersion || after.StateDigest != before.StateDigest {
		t.Fatal("recovery changed last complete ledger state")
	}
}

func TestActualProcessKillRecovery(t *testing.T) {
	if os.Getenv("GO_WANT_CRASH_HELPER") == "1" {
		path := os.Getenv("LEDGER_CRASH_DB")
		s, err := Open(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		err = s.ForceCrashDuringSettlement(context.Background(), CrashRequest{RequestID: "crash-test", ThroughMS: 1767226320000})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		os.Exit(4)
	}
	path := filepath.Join(t.TempDir(), "ledger.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = s.Recover(ctx)
	_, before := seedSampleThroughSettlement(t, s, ctx)
	_ = s.Close()
	cmd := exec.Command(os.Args[0], "-test.run=TestActualProcessKillRecovery")
	cmd.Env = append(os.Environ(), "GO_WANT_CRASH_HELPER=1", "LEDGER_CRASH_DB="+path)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("helper did not crash; output=%s", out)
	}
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() == 0 {
		t.Fatalf("unexpected helper error %v output=%s", err, out)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	report, err := reopened.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.AbortedIntentCount != 1 {
		t.Fatalf("recovery=%+v output=%s", report, out)
	}
	after, _ := reopened.LastState(ctx)
	if after.LedgerVersion != before.LedgerVersion || after.StateDigest != before.StateDigest {
		t.Fatalf("state changed across crash: before=%d/%s after=%d/%s", before.LedgerVersion, before.StateDigest, after.LedgerVersion, after.StateDigest)
	}
	var count int
	if err := reopened.DB().QueryRow(`SELECT COUNT(*) FROM tenant_states WHERE ledger_version=?`, before.LedgerVersion+1).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("partial tenant rows persisted: %d", count)
	}
	postResp, err := reopened.Settle(ctx, SettlementRequest{RequestID: "settle-after-crash-test", ThroughMS: 1767226320000})
	if err != nil || postResp.Status != 200 {
		t.Fatalf("post-recovery settle: err=%v status=%d body=%s", err, postResp.Status, postResp.Body)
	}
	var post SettlementReport
	if err := json.Unmarshal(decodeEnvelope(t, postResp).Data, &post); err != nil {
		t.Fatal(err)
	}
	if post.LedgerVersion != 5 || post.StateDigest != "sha256:95703e1907e69f95eeb0e253c03a8334dba740309b9970d21ba2a0b07666fc8d" {
		t.Fatalf("post-recovery settlement drifted: version=%d digest=%s", post.LedgerVersion, post.StateDigest)
	}
	if post.CalculationDigest != "sha256:afcf246383963fb922e27849735ef4d30e8dce36f6050a0dade96ebd972b2e9d" {
		t.Fatalf("post-recovery calculation digest drifted: %s", post.CalculationDigest)
	}
}

func TestHTTPUnifiedJSONErrors(t *testing.T) {
	s, _ := testStore(t)
	ts := httptest.NewServer(NewHTTPHandler(s, true))
	defer ts.Close()
	post := func(path, body string) (*http.Response, []byte) {
		t.Helper()
		resp, err := http.Post(ts.URL+path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp, b
	}
	resp, body := post("/v1/events", "{")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad JSON status=%d body=%s", resp.StatusCode, body)
	}
	var env testEnvelope
	if err := json.Unmarshal(body, &env); err != nil || env.Error == nil || env.Error.Code != CodeEventInvalid {
		t.Fatalf("bad JSON body=%s", body)
	}
	resp, body = post("/v1/snapshots", "{")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad snapshot status=%d body=%s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Error == nil || env.Error.Code != CodeLedgerInvalid {
		t.Fatalf("bad snapshot body=%s", body)
	}
	resp, body = post("/v1/nope", "{}")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown route status=%d body=%s", resp.StatusCode, body)
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Error == nil || env.Error.Code != CodeNotFound {
		t.Fatalf("unknown route body=%s", body)
	}
}

func TestOpenPathWithSpacesAndMemory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "database dir with spaces", "ledger.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("open path with spaces: %v", err)
	}
	ctx := context.Background()
	if _, err := s.Recover(ctx); err != nil {
		t.Fatalf("recover path with spaces: %v", err)
	}
	snap := simpleSnapshot(1, 0, 1000, 0, 900000, 0, []Tenant{{ID: "a", PoolID: "p", BaseSharesMilli: 1, CreditLimitMilli: 1, RateTier: "r", RepaymentPriority: 1}})
	if resp, err := s.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: "space-snap", Snapshot: snap}); err != nil || resp.Status != 200 {
		t.Fatalf("snapshot with spaces path: err=%v body=%s", err, resp.Body)
	}
	_ = s.Close()

	mem, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open memory: %v", err)
	}
	defer mem.Close()
	if _, err := mem.Recover(ctx); err != nil {
		t.Fatalf("recover memory: %v", err)
	}
	if resp, err := mem.SubmitSnapshot(ctx, SnapshotSubmission{RequestID: "memory-snap", Snapshot: snap}); err != nil || resp.Status != 200 {
		t.Fatalf("snapshot memory: err=%v body=%s", err, resp.Body)
	}
}
