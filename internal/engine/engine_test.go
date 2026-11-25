package engine_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"dialogalign/internal/domain"
	"dialogalign/internal/engine"
	"dialogalign/internal/generator"
	"dialogalign/internal/store"
)

func preparedWorkspace(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := generator.Generate(root); err != nil {
		t.Fatal(err)
	}
	return root
}

func mustRun(t *testing.T, root string, opts engine.RunOptions) domain.RunReport {
	t.Helper()
	report, err := engine.New(root).Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

func mustExport(t *testing.T, root string) domain.ExportBundle {
	t.Helper()
	bundle, err := engine.New(root).Export(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func canonicalBytes(t *testing.T, v any) []byte {
	t.Helper()
	blob, err := domain.CanonicalJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	return blob
}

func TestDeterministicRunAndReuse(t *testing.T) {
	rootA := preparedWorkspace(t)
	rootB := preparedWorkspace(t)
	reportA := mustRun(t, rootA, engine.RunOptions{ClearStop: true, Reset: true, RunMode: "full"})
	reportB := mustRun(t, rootB, engine.RunOptions{ClearStop: true, Reset: true, RunMode: "full"})
	if reportA.CompletedTurns != 16 || reportB.CompletedTurns != 16 {
		t.Fatalf("expected 16 turns, got %d/%d", reportA.CompletedTurns, reportB.CompletedTurns)
	}
	bundleA := mustExport(t, rootA)
	bundleB := mustExport(t, rootB)
	if got, want := string(canonicalBytes(t, bundleA)), string(canonicalBytes(t, bundleB)); got != want {
		t.Fatal("deterministic workspaces produced different exports")
	}
	reuse := mustRun(t, rootA, engine.RunOptions{ClearStop: true, RunMode: "incremental"})
	if reuse.ReusedTurns != 16 || reuse.RebuiltTurns != 0 || len(reuse.Invalidations) != 0 {
		t.Fatalf("unchanged run should reuse all turns: %+v", reuse)
	}
}

func TestAlignmentAmbiguityDriftAndPending(t *testing.T) {
	root := preparedWorkspace(t)
	mustRun(t, root, engine.RunOptions{ClearStop: true, Reset: true})
	v1 := mustExport(t, root)
	assertV1Findings(t, v1)

	if err := engine.ActivateMap(root, "v2"); err != nil {
		t.Fatal(err)
	}
	v2Report := mustRun(t, root, engine.RunOptions{ClearStop: true})
	if len(v2Report.Invalidations) == 0 {
		t.Fatal("map replacement must invalidate checkpoints")
	}
	foundUpgradeBoundary := false
	for _, inv := range v2Report.Invalidations {
		if inv.Stage == store.StageUpgrade && inv.TurnIndex == 1 {
			foundUpgradeBoundary = true
			break
		}
	}
	if !foundUpgradeBoundary {
		t.Fatalf("expected upgrade-stage invalidation at turn 1: %+v", v2Report.Invalidations)
	}
	v2 := mustExport(t, root)
	if v2.MapVersion != "v2" {
		t.Fatalf("map version = %s", v2.MapVersion)
	}
	driftBySlot := map[string]domain.DriftFinding{}
	for _, report := range v2.Alignments {
		for _, drift := range report.RoleDrifts {
			key := report.SessionID + "/" + drift.Field
			if existing, ok := driftBySlot[key]; ok && existing.Role == drift.Role && existing.PreviousRole == drift.PreviousRole {
				t.Fatalf("duplicate cumulative drift for %s", key)
			}
			driftBySlot[key] = drift
		}
	}
	s03, ok := driftBySlot["S03/subscription_date"]
	if !ok {
		t.Fatalf("missing S03 drift: %+v", driftBySlot)
	}
	if s03.PreviousRole != "delivery.date" || s03.Role != "subscription.start_date" || s03.LegacyCode != 301 {
		t.Fatalf("unexpected drift: %+v", s03)
	}
	if s03.CharOffset != 39 {
		t.Fatalf("drift char offset = %d, want 39", s03.CharOffset)
	}
	if s03.Reason == "" || !containsAll(s03.Evidence, "declared_map=v1", "active_map=v2", "prior_state_digest=") {
		t.Fatalf("drift evidence is incomplete: %+v", s03)
	}
}

func assertV1Findings(t *testing.T, bundle domain.ExportBundle) {
	t.Helper()
	var s01t3, s02t1, s04t4, s04t5 *domain.AlignmentReport
	for i := range bundle.Alignments {
		report := &bundle.Alignments[i]
		switch {
		case report.SessionID == "S01" && report.TurnIndex == 3:
			s01t3 = report
		case report.SessionID == "S02" && report.TurnIndex == 1:
			s02t1 = report
		case report.SessionID == "S04" && report.TurnIndex == 4:
			s04t4 = report
		case report.SessionID == "S04" && report.TurnIndex == 5:
			s04t5 = report
		}
	}
	if s01t3 == nil || len(s01t3.OutOfRange) != 1 {
		t.Fatalf("S01 turn 3 must report one out-of-range slot: %+v", s01t3)
	}
	finding := s01t3.OutOfRange[0]
	if finding.Role != "order.priority" || finding.CharOffset != 30 || finding.Value.Canonical != "9" {
		t.Fatalf("unexpected range finding: %+v", finding)
	}
	if s02t1 == nil || len(s02t1.Ambiguous) != 1 || len(s02t1.Ambiguous[0].Candidates) != 2 {
		t.Fatalf("S02 turn 1 must retain both ambiguity candidates: %+v", s02t1)
	}
	if s04t4 == nil || !hasDiagnostic(s04t4.Diagnostics, "INTENT_TRANSITION_LOOP") {
		t.Fatalf("S04 turn 4 must report an intent cycle: %+v", s04t4)
	}
	if s04t5 == nil {
		t.Fatal("missing S04 turn 5")
	}
	if countDiagnostic(s04t5.Diagnostics, "PENDING_UNANSWERED") != 2 {
		t.Fatalf("S04 final turn must report two unanswered pending slots: %+v", s04t5.Diagnostics)
	}
}

func TestIncrementalInvalidationReusesUnaffectedTurns(t *testing.T) {
	root := preparedWorkspace(t)
	mustRun(t, root, engine.RunOptions{ClearStop: true, Reset: true})
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	beforeS01, ok, err := st.ReadCheckpoint(context.Background(), "S01", 3, store.StageAlign)
	if err != nil || !ok {
		t.Fatalf("read S01 align: %v", err)
	}
	beforeS02t1, ok, err := st.ReadCheckpoint(context.Background(), "S02", 1, store.StageParse)
	if err != nil || !ok {
		t.Fatalf("read S02 t1 parse: %v", err)
	}
	beforeS02t2, ok, err := st.ReadCheckpoint(context.Background(), "S02", 2, store.StageParse)
	if err != nil || !ok {
		t.Fatalf("read S02 t2 parse: %v", err)
	}
	_ = st.Close()

	turns, err := generator.LoadTurns(filepath.Join(root, "data", "sessions.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range turns {
		if turns[i].SessionID == "S02" && turns[i].TurnIndex == 2 {
			turns[i].UserText += " updated"
		}
	}
	var blob []byte
	for _, turn := range turns {
		line, err := domain.CanonicalJSON(turn)
		if err != nil {
			t.Fatal(err)
		}
		blob = append(blob, line...)
		blob = append(blob, '\n')
	}
	if err := os.WriteFile(filepath.Join(root, "data", "sessions.jsonl"), blob, 0o644); err != nil {
		t.Fatal(err)
	}
	report := mustRun(t, root, engine.RunOptions{ClearStop: true})
	if len(report.Invalidations) != 1 {
		t.Fatalf("expected one root invalidation, got %+v", report.Invalidations)
	}
	inv := report.Invalidations[0]
	if inv.SessionID != "S02" || inv.TurnIndex != 2 || inv.Stage != store.StageParse {
		t.Fatalf("unexpected invalidation boundary: %+v", inv)
	}
	st, err = store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	afterS01, ok, err := st.ReadCheckpoint(context.Background(), "S01", 3, store.StageAlign)
	if err != nil || !ok || afterS01.Seq != beforeS01.Seq {
		t.Fatalf("unaffected S01 checkpoint was rebuilt: before=%d after=%+v err=%v", beforeS01.Seq, afterS01, err)
	}
	afterS02t1, ok, err := st.ReadCheckpoint(context.Background(), "S02", 1, store.StageParse)
	if err != nil || !ok || afterS02t1.Seq != beforeS02t1.Seq {
		t.Fatalf("S02 turn 1 should be reused: before=%d after=%+v err=%v", beforeS02t1.Seq, afterS02t1, err)
	}
	afterS02t2, ok, err := st.ReadCheckpoint(context.Background(), "S02", 2, store.StageParse)
	if err != nil || !ok || afterS02t2.Seq <= beforeS02t2.Seq {
		t.Fatalf("S02 turn 2 should be rebuilt with a newer seq: before=%d after=%+v err=%v", beforeS02t2.Seq, afterS02t2, err)
	}
}

func TestStopResumeProducesByteIdenticalExport(t *testing.T) {
	full := preparedWorkspace(t)
	mustRun(t, full, engine.RunOptions{ClearStop: true, Reset: true})
	fullBytes := canonicalBytes(t, mustExport(t, full))

	partial := preparedWorkspace(t)
	first := mustRun(t, partial, engine.RunOptions{ClearStop: true, Reset: true, StopAfterTurns: 5})
	if !first.Stopped || first.CompletedTurns != 5 {
		t.Fatalf("controlled stop did not land on a turn boundary: %+v", first)
	}
	if _, err := engine.New(partial).Export(context.Background()); err == nil {
		t.Fatal("partial workspace must not export as complete")
	}
	second := mustRun(t, partial, engine.RunOptions{ClearStop: true, RunMode: "resume"})
	if second.Stopped || second.CompletedTurns != 16 {
		t.Fatalf("resume did not complete all turns: %+v", second)
	}
	resumedBytes := canonicalBytes(t, mustExport(t, partial))
	if string(fullBytes) != string(resumedBytes) {
		t.Fatal("resumed export differs from a from-scratch run")
	}
}

func TestSlotConflictKeepsPriorConfirmation(t *testing.T) {
	root := preparedWorkspace(t)
	dataset, err := engine.LoadDataset(root)
	if err != nil {
		t.Fatal(err)
	}
	turn1 := domain.TurnInput{SessionID: "SC", TurnIndex: 1, UserText: "first", BotReply: "ok", Language: "en"}
	artifact1 := domain.UpgradeArtifact{SessionID: "SC", TurnIndex: 1, MapVersion: "v1", Slots: []domain.UpgradedSlot{{
		Role: "order.order_id", Range: dataset.Catalog.Roles[0].Range,
		Value:  domain.SlotValue{Kind: domain.KindInteger, Canonical: "12345", Raw: "12345"},
		Source: domain.ValueSource{Kind: "new", Field: "order_id", CharOffset: 0}, Order: 0,
	}}}
	state1, err := engine.Evolve(nil, turn1, artifact1, dataset.Config, dataset.Catalog, false)
	if err != nil {
		t.Fatal(err)
	}
	turn2 := domain.TurnInput{SessionID: "SC", TurnIndex: 2, UserText: "second", BotReply: "ok", Language: "en"}
	artifact2 := artifact1
	artifact2.TurnIndex = 2
	artifact2.Slots = []domain.UpgradedSlot{{
		Role: "order.order_id", Range: dataset.Catalog.Roles[0].Range,
		Value:  domain.SlotValue{Kind: domain.KindInteger, Canonical: "54321", Raw: "54321"},
		Source: domain.ValueSource{Kind: "new", Field: "order_id", CharOffset: 0}, Order: 0,
	}}
	state2, err := engine.Evolve(&state1, turn2, artifact2, dataset.Config, dataset.Catalog, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(state2.Confirmed) != 1 || state2.Confirmed[0].Value.Canonical != "12345" {
		t.Fatalf("slot conflict overwrote confirmed history: %+v", state2.Confirmed)
	}
	if !hasDiagnostic(state2.Diagnostics, "SLOT_CONFLICT") {
		t.Fatalf("missing conflict diagnostic: %+v", state2.Diagnostics)
	}
	foundChange := false
	for _, change := range state2.Changes {
		if change.Kind == "slot_conflict" && change.OldValue == "12345" && change.NewValue == "54321" {
			foundChange = true
		}
	}
	if !foundChange {
		t.Fatalf("missing conflict change set: %+v", state2.Changes)
	}
}

func TestConfigChangesInvalidateEarliestStage(t *testing.T) {
	cases := []struct {
		name  string
		stage string
		edit  func(*domain.WorkbenchConfig)
	}{
		{"normalization", store.StageUpgrade, func(cfg *domain.WorkbenchConfig) { cfg.Normalization.IntegerPrecision = 2 }},
		{"rules", store.StageEvolve, func(cfg *domain.WorkbenchConfig) { cfg.Rules.Rules[0].Priority++ }},
		{"alignment_threshold", store.StageAlign, func(cfg *domain.WorkbenchConfig) { cfg.Alignment.MaxFindingsPerCategory = 99 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := preparedWorkspace(t)
			mustRun(t, root, engine.RunOptions{ClearStop: true, Reset: true})
			configPath := filepath.Join(root, "data", "config.json")
			blob, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatal(err)
			}
			var cfg domain.WorkbenchConfig
			if err := json.Unmarshal(blob, &cfg); err != nil {
				t.Fatal(err)
			}
			tc.edit(&cfg)
			blob, err = json.MarshalIndent(cfg, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			blob = append(blob, '\n')
			if err := os.WriteFile(configPath, blob, 0o644); err != nil {
				t.Fatal(err)
			}
			report := mustRun(t, root, engine.RunOptions{ClearStop: true})
			if len(report.Invalidations) == 0 {
				t.Fatalf("config change produced no invalidation")
			}
			for _, inv := range report.Invalidations {
				if inv.TurnIndex < 1 || inv.Stage != tc.stage {
					t.Fatalf("unexpected invalidation stage: want %s, got %+v", tc.stage, report.Invalidations)
				}
			}
		})
	}
}

func TestActivateMapRefreshesManifest(t *testing.T) {
	root := preparedWorkspace(t)
	if err := engine.ActivateMap(root, "v2"); err != nil {
		t.Fatal(err)
	}
	manifestBlob, err := os.ReadFile(filepath.Join(root, "data", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest map[string]string
	if err := json.Unmarshal(manifestBlob, &manifest); err != nil {
		t.Fatal(err)
	}
	activeBlob, err := os.ReadFile(filepath.Join(root, "data", "active-map.json"))
	if err != nil {
		t.Fatal(err)
	}
	if manifest["active-map.json"] != domain.DigestBytes(activeBlob) {
		t.Fatal("active map manifest digest was not refreshed")
	}
}

func hasDiagnostic(diags []domain.Diagnostic, code string) bool {
	return countDiagnostic(diags, code) > 0
}

func countDiagnostic(diags []domain.Diagnostic, code string) int {
	count := 0
	for _, diag := range diags {
		if diag.Code == code {
			count++
		}
	}
	return count
}

func containsAll(value string, parts ...string) bool {
	for _, part := range parts {
		if !contains(value, part) {
			return false
		}
	}
	return true
}

func contains(value, part string) bool {
	return len(part) == 0 || (len(value) >= len(part) && indexOf(value, part) >= 0)
}

func indexOf(value, part string) int {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return i
		}
	}
	return -1
}
