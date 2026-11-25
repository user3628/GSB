package recall

import (
	"context"
	"sort"
	"sync"
	"time"

	"multirecall/internal/configx"
	"multirecall/internal/model"
)

type Engine struct {
	Dataset       model.Dataset
	Config        configx.Snapshot
	DatasetDigest string
	Lex           *LexicalIndex
	lexDocByID    map[string]lexicalDoc
}

func NewEngine(ds model.Dataset, cfg configx.Snapshot) *Engine {
	idx := NewLexicalIndex(ds.Items)
	docs := make(map[string]lexicalDoc, len(idx.Docs))
	for _, doc := range idx.Docs {
		docs[doc.ItemID] = doc
	}
	return &Engine{Dataset: ds, Config: cfg, DatasetDigest: model.Digest(ds), Lex: idx, lexDocByID: docs}
}

func (e *Engine) WithConfig(cfg configx.Snapshot) *Engine {
	cp := *e
	cp.Config = cfg
	return &cp
}

func (e *Engine) BaseFingerprint(q model.Query) string {
	return model.Digest(struct {
		Query         model.Query `json:"query"`
		DatasetDigest string      `json:"dataset_digest"`
		ConfigVersion int64       `json:"config_version"`
		ConfigDigest  string      `json:"config_digest"`
	}{q, e.DatasetDigest, e.Config.ConfigVersion, e.Config.ContentDigest})
}

func (e *Engine) ProcessQuery(q model.Query, globalPlan *BudgetPlan) QueryReport {
	report := QueryReport{
		Query: q, InputFingerprint: e.BaseFingerprint(q),
		ConfigVersion: e.Config.ConfigVersion, ConfigDigest: e.Config.ContentDigest,
		Hits: []Hit{},
	}
	for _, stage := range StageOrder {
		if apiErr := e.ApplyStage(stage, &report, globalPlan); apiErr != nil {
			report.Error = apiErr
			return report
		}
	}
	return report
}

type RunOptions struct {
	Workers int
	Context context.Context
}

func (e *Engine) RunAll(opts RunOptions) (*Report, error) {
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if e.Config.Budget.RoundTimeBudgetMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(e.Config.Budget.RoundTimeBudgetMS)*time.Millisecond)
		defer cancel()
	}
	workers := opts.Workers
	if workers <= 0 {
		workers = e.Config.Budget.QueryConcurrency
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > len(e.Dataset.Queries) && len(e.Dataset.Queries) > 0 {
		workers = len(e.Dataset.Queries)
	}

	plan := AdjudicateBudget(e.Config, len(e.Dataset.Queries))
	results := make([]QueryReport, len(e.Dataset.Queries))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for i, q := range e.Dataset.Queries {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			results[i] = StoppedReport(q, e, &plan, ctx.Err())
			continue
		}
		if ctx.Err() != nil {
			<-sem
			results[i] = StoppedReport(q, e, &plan, ctx.Err())
			continue
		}
		wg.Add(1)
		go func(index int, query model.Query) {
			defer wg.Done()
			defer func() { <-sem }()
			results[index] = e.ProcessQuery(query, &plan)
		}(i, q)
	}
	wg.Wait()

	queries := append([]QueryReport(nil), results...)
	sort.SliceStable(queries, func(i, j int) bool { return queries[i].Query.QueryID < queries[j].Query.QueryID })
	report := &Report{
		Schema: "multirecall.report.v1", DatasetSeed: e.Dataset.Seed, DatasetDigest: e.DatasetDigest,
		Config: e.Config, QueryCount: len(queries), Queries: queries,
	}
	report.ResultDigest = ReportDigest(queries)
	return report, nil
}

func StoppedReport(q model.Query, e *Engine, plan *BudgetPlan, cause error) QueryReport {
	message := "round budget context stopped before query boundary; no running query goroutine was killed"
	if cause != nil {
		message += ": " + cause.Error()
	}
	return QueryReport{
		Query: q, InputFingerprint: e.BaseFingerprint(q), ConfigVersion: e.Config.ConfigVersion,
		ConfigDigest: e.Config.ContentDigest, Budget: plan, Hits: []Hit{},
		Error: &model.APIError{Code: "E_ROUND_BUDGET_STOP", QueryID: q.QueryID, Message: message},
	}
}

func CanonicalReport(report *Report) []byte {
	return model.CanonicalJSON(report)
}
