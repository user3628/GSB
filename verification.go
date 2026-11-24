package cockpitaudio

import (
	"context"
	"path/filepath"
)

type VerificationCase struct {
	Name            string `json:"name"`
	OK              bool   `json:"ok"`
	EarliestStage   string `json:"earliest_stage,omitempty"`
	Detail          string `json:"detail,omitempty"`
	FirstDifference string `json:"first_difference,omitempty"`
}

type VerificationReport struct {
	OK                 bool               `json:"ok"`
	Workdir            string             `json:"workdir"`
	Cases              []VerificationCase `json:"cases"`
	Concurrency        *ConcurrencyAudit  `json:"concurrency,omitempty"`
	LatencyBudgetError *Error             `json:"latency_budget_error,omitempty"`
}

func RunLocalVerification(workdir string) (*VerificationReport, *Error) {
	report := &VerificationReport{OK: true, Workdir: workdir, Cases: []VerificationCase{}}
	ds, err := GenerateDataset(GenerateOptions{Root: filepath.Join(workdir, "samples")})
	if err != nil {
		return report, NewError(CodeInternal, StageIngest, "", "generate: %v", err)
	}
	cfg := DefaultConfig()
	cfg.LatencyBudgetMS = 1000

	first, berr := BuildSession(ds, cfg, nil)
	if berr != nil {
		return report, berr
	}
	second, berr := BuildSession(ds, cfg, nil)
	if berr != nil {
		return report, berr
	}
	if cerr := CompareSessionResults(first, second); cerr != nil {
		report.fail("deterministic_full_build", cerr)
	} else {
		report.pass("deterministic_full_build", "", "two full builds are byte-consistent")
	}

	incrementalCases := []struct {
		name   string
		mutate func(*Config, *EditState)
	}{
		{"priority_change", func(c *Config, e *EditState) {
			_, _ = e.Apply(EditOp{Type: "set_priority", Operator: "verify", ZoneID: ZoneDriver, Priority: 2.5, Reason: "verification"})
		}},
		{"vocabulary_hit_change", func(c *Config, e *EditState) {
			cmd := ds.Manifest.Shards[0].Commands[0].ID
			_, _ = e.Apply(EditOp{Type: "upsert_hit", Operator: "verify", CommandID: cmd, Reason: "verification",
				Hit: &CandidateHit{ZoneID: ZoneDriver, Word: "打开主驾车窗", Strength: 6.0}})
		}},
		{"frame_parameters", func(c *Config, e *EditState) {
			c.FrameLengthMS = 20
			c.FrameShiftMS = 10
		}},
		{"hysteresis_thresholds", func(c *Config, e *EditState) {
			c.HighThreshold = 0.64
			c.LowThreshold = 0.42
		}},
		{"attribution_gap_threshold", func(c *Config, e *EditState) {
			c.AttributionGapThreshold = 0.02
		}},
		{"speed_parameter", func(c *Config, e *EditState) {
			c.SpeedMPS = 355
		}},
	}
	for _, tc := range incrementalCases {
		caseCfg := cfg.Clone()
		cache, _, cerr := NewPipelineCache(ds, caseCfg, nil)
		if cerr != nil {
			return report, cerr
		}
		edit := cache.EditState()
		tc.mutate(&caseCfg, edit)
		inc, plan, rerr := cache.Refresh(caseCfg, edit)
		if rerr != nil {
			report.fail(tc.name, rerr)
			continue
		}
		full, ferr := BuildSession(ds, caseCfg, edit)
		if ferr != nil {
			report.fail(tc.name, ferr)
			continue
		}
		if diff := CompareSessionResults(inc, full); diff != nil {
			diff.FirstDifference = plan.EarliestStage + ": " + diff.FirstDifference
			report.Cases = append(report.Cases, VerificationCase{Name: tc.name, OK: false, EarliestStage: plan.EarliestStage, FirstDifference: diff.FirstDifference, Detail: diff.Message})
			report.OK = false
		} else {
			report.Cases = append(report.Cases, VerificationCase{Name: tc.name, OK: true, EarliestStage: plan.EarliestStage,
				Detail: "reused=" + joinStrings(plan.ReusedStages) + " recomputed=" + joinStrings(plan.Recomputed)})
		}
	}

	qcfg := DefaultConfig()
	svc, qerr := NewQueryService(qcfg, first)
	if qerr != nil {
		return report, qerr
	}
	req := QueryRequest{QueryID: "verify-cache", Type: QueryAttributionBasis, CommandID: ds.Manifest.Shards[0].Commands[1].ID}
	r1, qerr := svc.Query(context.Background(), req)
	if qerr != nil {
		return report, qerr
	}
	r2, qerr := svc.Query(context.Background(), req)
	if qerr != nil {
		return report, qerr
	}
	if !r2.Cached || string(r1.Result) != string(r2.Result) {
		report.OK = false
		report.Cases = append(report.Cases, VerificationCase{Name: "query_cache_byte_consistency", OK: false, Detail: "cache miss or bytes differ"})
	} else {
		report.pass("query_cache_byte_consistency", StageQuery, "second response served from versioned cache")
	}

	budgetCfg := DefaultConfig()
	budgetCfg.LatencyBudgetMS = 0.000001
	budgetSvc, _ := NewQueryService(budgetCfg, first)
	_, berr2 := budgetSvc.Query(context.Background(), QueryRequest{QueryID: "verify-budget", Type: QueryActiveZones, AtMS: 250})
	if ae, ok := AsError(berr2); ok && ae.Code == CodeLatencyBudget && ae.QueryID == "verify-budget" {
		report.LatencyBudgetError = ae
		report.pass("latency_budget_structured_error", StageQuery, ae.Error())
	} else {
		report.OK = false
		report.Cases = append(report.Cases, VerificationCase{Name: "latency_budget_structured_error", OK: false, Detail: "expected E_LATENCY_BUDGET"})
	}

	ops := defaultConcurrentEdits(ds)
	requests := defaultQueryRequests(ds)
	audit, aerr := RunConcurrentQueryAudit(ds, cfg, ops, requests, 6)
	report.Concurrency = audit
	if aerr != nil {
		report.OK = false
		report.Cases = append(report.Cases, VerificationCase{Name: "concurrent_edit_settlement_consistency", OK: false, FirstDifference: aerr.Error()})
	} else {
		report.pass("concurrent_edit_settlement_consistency", StageQuery,
			"all observed versioned responses match full settlement")
	}

	if !report.OK {
		return report, NewError(CodeIncrementalMismatch, "verification", "", "one or more verification cases failed")
	}
	return report, nil
}

func (r *VerificationReport) pass(name, stage, detail string) {
	r.Cases = append(r.Cases, VerificationCase{Name: name, OK: true, EarliestStage: stage, Detail: detail})
}

func (r *VerificationReport) fail(name string, err *Error) {
	r.OK = false
	vc := VerificationCase{Name: name, OK: false, Detail: err.Message, FirstDifference: err.FirstDifference}
	r.Cases = append(r.Cases, vc)
}

func joinStrings(in []string) string {
	out := ""
	for i, s := range in {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}

func defaultConcurrentEdits(ds *Dataset) []EditOp {
	ops := []EditOp{}
	for i := 0; i < 8; i++ {
		zone := ZoneOrder[i%len(ZoneOrder)]
		ops = append(ops, EditOp{
			Type: "set_priority", Operator: "concurrent-editor",
			ZoneID: zone, Priority: 1 + float64(i+1)*0.17, Reason: "concurrent priority update",
		})
	}
	for si, shard := range ds.Manifest.Shards {
		for ci, cmd := range shard.Commands {
			zone := ZoneOrder[(si+ci)%len(ZoneOrder)]
			ops = append(ops, EditOp{
				Type: "upsert_hit", Operator: "concurrent-editor", CommandID: cmd.ID,
				Reason: "concurrent vocabulary edit",
				Hit:    &CandidateHit{ZoneID: zone, Word: cmd.Word, Strength: 3.5 + float64(si)*0.2 + float64(ci)*0.1},
			})
		}
	}
	return ops
}

func defaultQueryRequests(ds *Dataset) []QueryRequest {
	reqs := []QueryRequest{
		{Type: QueryActiveZones, AtMS: 250},
		{Type: QueryActiveZones, AtMS: 650},
	}
	for _, shard := range ds.Manifest.Shards {
		for _, cmd := range shard.Commands {
			reqs = append(reqs,
				QueryRequest{Type: QueryAttributionBasis, CommandID: cmd.ID, ShardID: shard.ShardID},
				QueryRequest{Type: QueryDelayDetails, CommandID: cmd.ID, ShardID: shard.ShardID},
			)
		}
	}
	return reqs
}
