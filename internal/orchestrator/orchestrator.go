package orchestrator

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"multirecall/internal/model"
	"multirecall/internal/recall"
	"multirecall/internal/store"
)

type Orchestrator struct {
	Engine *recall.Engine
	Store  *store.Store
}

func New(engine *recall.Engine, st *store.Store) *Orchestrator {
	return &Orchestrator{Engine: engine, Store: st}
}

func stageFingerprint(base, stage, previousOutput string, cfgVersion int64, cfgDigest string) string {
	return model.Digest(struct {
		BaseFingerprint      string `json:"base_fingerprint"`
		Stage                string `json:"stage"`
		PreviousOutputDigest string `json:"previous_output_digest"`
		ConfigVersion        int64  `json:"config_version"`
		ConfigDigest         string `json:"config_digest"`
	}{base, stage, previousOutput, cfgVersion, cfgDigest})
}

func (o *Orchestrator) runQuery(ctx context.Context, q model.Query, plan recall.BudgetPlan) (recall.QueryReport, error) {
	cfg := o.Engine.Config
	rep := recall.QueryReport{
		Query: q, InputFingerprint: o.Engine.BaseFingerprint(q),
		ConfigVersion: cfg.ConfigVersion, ConfigDigest: cfg.ContentDigest, Hits: []recall.Hit{},
	}
	previousDigest := rep.InputFingerprint

	for _, stage := range recall.StageOrder {
		inputFP := stageFingerprint(rep.InputFingerprint, stage, previousDigest, cfg.ConfigVersion, cfg.ContentDigest)
		existing, err := o.Store.GetValid(ctx, q.QueryID, stage)
		if err != nil {
			return rep, err
		}
		if existing != nil && existing.ConfigVersion == cfg.ConfigVersion && existing.InputFingerprint == inputFP {
			if got := model.DigestBytes("multirecall:stage-output:v1:", existing.OutputJSON); got != existing.OutputDigest {
				return rep, &model.APIError{Code: model.CodeCheckpoint, QueryID: q.QueryID, Message: "checkpoint output digest mismatch at " + stage}
			}
			var recovered recall.QueryReport
			if err := json.Unmarshal(existing.OutputJSON, &recovered); err != nil {
				return rep, err
			}
			rep = recovered
			previousDigest = existing.OutputDigest
			if rep.Error != nil {
				return rep, nil
			}
			continue
		}

		stageErr := o.Engine.ApplyStage(stage, &rep, &plan)
		raw := model.CanonicalJSON(rep)
		outputDigest := model.DigestBytes("multirecall:stage-output:v1:", raw)
		if _, err := o.Store.CommitStage(ctx, store.StageRecord{
			Stage: stage, QueryID: q.QueryID, InputFingerprint: inputFP,
			ConfigVersion: cfg.ConfigVersion, OutputDigest: outputDigest, OutputJSON: raw,
		}); err != nil {
			return rep, err
		}
		previousDigest = outputDigest
		if stageErr != nil {
			return rep, nil
		}
	}
	return rep, nil
}

func (o *Orchestrator) RunAll(ctx context.Context, workers int) (*recall.Report, error) {
	cfg := o.Engine.Config
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Budget.RoundTimeBudgetMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Budget.RoundTimeBudgetMS)*time.Millisecond)
		defer cancel()
	}
	if workers <= 0 {
		workers = cfg.Budget.QueryConcurrency
	}
	if workers <= 0 {
		workers = 1
	}
	queries := o.Engine.Dataset.Queries
	if workers > len(queries) && len(queries) > 0 {
		workers = len(queries)
	}
	plan := recall.AdjudicateBudget(cfg, len(queries))
	results := make([]recall.QueryReport, len(queries))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for i, q := range queries {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			results[i] = recall.StoppedReport(q, o.Engine, &plan, ctx.Err())
			continue
		}
		if ctx.Err() != nil {
			<-sem
			results[i] = recall.StoppedReport(q, o.Engine, &plan, ctx.Err())
			continue
		}
		wg.Add(1)
		go func(index int, query model.Query) {
			defer wg.Done()
			defer func() { <-sem }()
			// A query which has passed the boundary runs to completion and its
			// transactional commits are not interrupted by the round deadline.
			rep, err := o.runQuery(context.Background(), query, plan)
			if err != nil {
				rep = recall.QueryReport{
					Query: query, InputFingerprint: o.Engine.BaseFingerprint(query),
					ConfigVersion: cfg.ConfigVersion, ConfigDigest: cfg.ContentDigest, Hits: []recall.Hit{},
					Error: &model.APIError{Code: model.CodeCheckpoint, QueryID: query.QueryID, Message: err.Error()},
				}
			}
			results[index] = rep
		}(i, q)
	}
	wg.Wait()

	sort.SliceStable(results, func(i, j int) bool { return results[i].Query.QueryID < results[j].Query.QueryID })
	report := &recall.Report{
		Schema: "multirecall.report.v1", DatasetSeed: o.Engine.Dataset.Seed,
		DatasetDigest: o.Engine.DatasetDigest, Config: cfg, QueryCount: len(results), Queries: results,
	}
	report.ResultDigest = recall.ReportDigest(results)
	return report, nil
}
