package cockpitaudio

import (
	"context"
	"encoding/json"
	"testing"
)

func acceptanceDataset(t *testing.T) *Dataset {
	t.Helper()
	ds, err := GenerateDataset(GenerateOptions{Root: t.TempDir() + "/samples"})
	if err != nil {
		t.Fatalf("generate dataset: %v", err)
	}
	return ds
}

func TestFourZoneVADAndAttribution(t *testing.T) {
	ds := acceptanceDataset(t)
	cfg := DefaultConfig()
	result, err := BuildSession(ds, cfg, nil)
	if err != nil {
		t.Fatalf("build session: %v", err)
	}
	rc, rerr := cfg.ResolveVAD("test")
	if rerr != nil {
		t.Fatal(rerr)
	}
	if rc.FrameLengthSamples != 400 || rc.FrameShiftSamples != 160 {
		t.Fatalf("sample conversion = %d/%d, want 400/160", rc.FrameLengthSamples, rc.FrameShiftSamples)
	}
	if len(result.Shards) != DefaultShardCount {
		t.Fatalf("shards = %d", len(result.Shards))
	}

	first := result.Shards[0].VAD
	if len(first.Zones) != 4 {
		t.Fatalf("zones = %d", len(first.Zones))
	}
	for _, zone := range first.Zones {
		if zone.FrameLengthSamples != 400 || zone.FrameShiftSamples != 160 {
			t.Fatalf("zone conversion not recorded: %+v", zone)
		}
		if len(zone.Intervals) != 2 {
			t.Fatalf("zone %s intervals = %d (%+v), want 2", zone.ZoneID, len(zone.Intervals), zone.Intervals)
		}
		for _, in := range zone.Intervals {
			if in.StartMS >= in.EndMS || in.StartMS < 0 || in.EndMS > ShardTimelineMS+50 {
				t.Fatalf("bad interval %+v", in)
			}
		}
	}

	open := firstAttribution(t, result, "cmd-open-window")
	if open.Ambiguous || open.ResponsibleZoneID != ZoneDriver {
		t.Fatalf("open command = amb=%v responsible=%q top=%q gap=%v", open.Ambiguous, open.ResponsibleZoneID, open.TopZoneID, open.ScoreGap)
	}
	if len(open.Candidates) != 4 || len(open.PairDelays) != 6 {
		t.Fatalf("open evidence candidates=%d pairs=%d", len(open.Candidates), len(open.PairDelays))
	}

	temp := firstAttribution(t, result, "cmd-temperature")
	if !temp.Ambiguous || temp.ResponsibleZoneID != "" || len(temp.Candidates) != 4 {
		t.Fatalf("temperature command should preserve ambiguity: %+v", temp)
	}
	if temp.ScoreGap >= cfg.AttributionGapThreshold {
		t.Fatalf("gap %v not below threshold %v", temp.ScoreGap, cfg.AttributionGapThreshold)
	}

	again, berr := BuildSession(ds, cfg, nil)
	if berr != nil {
		t.Fatal(berr)
	}
	a, _ := CanonicalJSON(result)
	b, _ := CanonicalJSON(again)
	if string(a) != string(b) {
		t.Fatal("full session result is not byte-deterministic")
	}
}

func firstAttribution(t *testing.T, r *SessionResult, suffix string) Attribution {
	t.Helper()
	for _, s := range r.Shards {
		for _, a := range s.Attribution.Items {
			if len(a.CommandID) >= len(suffix) && a.CommandID[len(a.CommandID)-len(suffix):] == suffix {
				return a
			}
		}
	}
	t.Fatalf("attribution suffix %s not found", suffix)
	return Attribution{}
}

func TestEditSplitMergePriorityAndHit(t *testing.T) {
	ds := acceptanceDataset(t)
	cfg := DefaultConfig()
	base, err := BuildSession(ds, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	edit := NewEditState(collectIntervals(base), cfg.Priorities)
	original := edit.Intervals[0]
	mid := (original.StartMS + original.EndMS) / 2

	split, serr := edit.Apply(EditOp{
		Type: "split_interval", Operator: "tester", Reason: "split at midpoint",
		IntervalID: original.ID, SplitAtMS: mid,
	})
	if serr != nil || split.Version != 1 || len(split.AffectedIDs) != 3 {
		t.Fatalf("split record=%+v err=%v", split, serr)
	}
	leftID, rightID := split.AffectedIDs[1], split.AffectedIDs[2]

	merged, merr := edit.Apply(EditOp{
		Type: "merge_intervals", Operator: "tester", Reason: "restore adjacent pieces",
		IntervalIDs: []string{leftID, rightID},
	})
	if merr != nil || merged.Version != 2 {
		t.Fatalf("merge record=%+v err=%v", merged, merr)
	}
	found := false
	for _, in := range edit.Intervals {
		if in.StartMS == original.StartMS && in.EndMS == original.EndMS && in.ZoneID == original.ZoneID {
			found = true
		}
	}
	if !found {
		t.Fatal("merged interval does not restore original span")
	}

	prio, perr := edit.Apply(EditOp{Type: "set_priority", Operator: "tester", ZoneID: ZoneRearLeft, Priority: 4, Reason: "focus rear"})
	if perr != nil || prio.Version != 3 {
		t.Fatal(prio, perr)
	}
	hit, herr := edit.Apply(EditOp{
		Type: "upsert_hit", Operator: "tester", CommandID: "x",
		Hit: &CandidateHit{ZoneID: ZoneRearLeft, Word: "词", Strength: 5},
	})
	if herr != nil || hit.Version != 4 || len(edit.Log) != 4 {
		t.Fatal(hit, herr)
	}
	if _, berr := BuildSession(ds, cfg, edit); berr != nil {
		t.Fatalf("recompute after edits: %v", berr)
	}
}

func TestQueryCacheAndBudget(t *testing.T) {
	ds := acceptanceDataset(t)
	cfg := DefaultConfig()
	cfg.LatencyBudgetMS = 100
	result, err := BuildSession(ds, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	svc, qerr := NewQueryService(cfg, result)
	if qerr != nil {
		t.Fatal(qerr)
	}

	active, qerr := svc.Query(context.Background(), QueryRequest{QueryID: "q1", Type: QueryActiveZones, AtMS: 250})
	if qerr != nil {
		t.Fatal(qerr)
	}
	if len(mustActive(t, active.Result).Zones) != 4 {
		t.Fatalf("active zones at 250 = %+v", mustActive(t, active.Result))
	}

	req := QueryRequest{QueryID: "q2", Type: QueryAttributionBasis, CommandID: ds.Manifest.Shards[0].Commands[1].ID}
	first, qerr := svc.Query(context.Background(), req)
	if qerr != nil {
		t.Fatal(qerr)
	}
	second, qerr := svc.Query(context.Background(), req)
	if qerr != nil {
		t.Fatal(qerr)
	}
	if !second.Cached || string(first.Result) != string(second.Result) {
		t.Fatal("cached query result differs from first computation")
	}
	fresh, rerr := RecomputeQueryCanonical(cfg, result, req)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if string(fresh) != string(second.Result) {
		t.Fatal("cache result differs from recomputation")
	}

	budgetCfg := cfg
	budgetCfg.LatencyBudgetMS = 0.000001
	budgetSvc, _ := NewQueryService(budgetCfg, result)
	_, berr := budgetSvc.Query(context.Background(), QueryRequest{QueryID: "budget-q", Type: QueryDelayDetails, CommandID: req.CommandID})
	ae, ok := AsError(berr)
	if !ok || ae.Code != CodeLatencyBudget || ae.QueryID != "budget-q" {
		t.Fatalf("budget error = %#v", berr)
	}
	if ae.BudgetMS != budgetCfg.LatencyBudgetMS {
		t.Fatalf("budget field = %v", ae.BudgetMS)
	}
}

func mustActive(t *testing.T, raw []byte) activeZonesResult {
	t.Helper()
	var out activeZonesResult
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestLocalVerificationAndConcurrency(t *testing.T) {
	report, err := RunLocalVerification(t.TempDir())
	if err != nil {
		t.Fatalf("verification failed: %v\nreport=%+v", err, report)
	}
	if report == nil || !report.OK || report.Concurrency == nil || report.Concurrency.QueriesChecked == 0 {
		t.Fatalf("bad verification report: %+v", report)
	}
	if len(report.Concurrency.UniqueVersions) < 2 {
		t.Fatalf("expected multiple observed edit versions: %+v", report.Concurrency)
	}
}

func TestInjectionErrorCodes(t *testing.T) {
	tests := []struct {
		name  string
		inj   Injection
		mut   func(*Config)
		code  ErrorCode
		stage string
	}{
		{"sample count", Injection{Kind: "sample_count"}, nil, CodeSampleCountMismatch, StageIngest},
		{"rate conflict", Injection{Kind: "rate_conflict"}, nil, CodeRateConflict, StageIngest},
		{"offset overlap", Injection{OverlapZoneOffsets: true}, nil, CodeInvalidParam, StageIngest},
		{"max speakers zero", Injection{}, func(c *Config) { c.MaxSpeakers = 0 }, CodeInvalidParam, StageAttribution},
		{"mask negative", Injection{}, func(c *Config) { c.MaskIterations = -2 }, CodeInvalidParam, StageVAD},
		{"speed nonpositive", Injection{}, func(c *Config) { c.SpeedMPS = -1 }, CodeInvalidParam, StageAttribution},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds, err := GenerateDataset(GenerateOptions{Root: t.TempDir() + "/samples", Injection: tc.inj})
			if err != nil {
				t.Fatal(err)
			}
			cfg := DefaultConfig()
			if tc.mut != nil {
				tc.mut(&cfg)
			}
			_, berr := BuildSession(ds, cfg, nil)
			ae, ok := AsError(berr)
			if !ok || ae.Code != tc.code || ae.Stage != tc.stage || ae.ShardID == "" {
				t.Fatalf("error=%#v want code=%s stage=%s with shard", berr, tc.code, tc.stage)
			}
		})
	}
}
