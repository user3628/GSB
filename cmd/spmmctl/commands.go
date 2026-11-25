package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"spmmopt/engine"
	"spmmopt/samples"
	"spmmopt/spmm"
	"spmmopt/store"
)

type planOptions struct {
	samplesDir    string
	sample        string
	db            string
	genSeed       int64
	sum           string
	order         string
	skipZeros     bool
	cacheBytes    int64
	peakBytes     int64
	rowBlock      int
	colBlock      int
	errorSeed     uint64
	errorSamples  int
	absTol        float64
	relTol        float64
	interactiveMS float64
}

func addPlanFlags(fs *flag.FlagSet, includeSample bool) *planOptions {
	o := &planOptions{}
	fs.StringVar(&o.samplesDir, "samples", "artifacts/samples", "sample corpus directory")
	if includeSample {
		fs.StringVar(&o.sample, "sample", "square_p10", "sample name")
	}
	fs.StringVar(&o.db, "db", "artifacts/spmm.db", "SQLite database path")
	fs.Int64Var(&o.genSeed, "seed", samples.DefaultSeed, "fixed sample generation seed")
	fs.StringVar(&o.sum, "sum", string(spmm.SumCompensated), "sequential or compensated")
	fs.StringVar(&o.order, "order", string(spmm.OrderBlockOrder), "column_ascending or block_order")
	fs.BoolVar(&o.skipZeros, "skip-zeros", true, "skip explicit zero CSR values (false retains and counts them)")
	fs.Int64Var(&o.cacheBytes, "cache-bytes", spmm.DefaultCacheBudgetBytes, "block-selection cache budget in bytes")
	fs.Int64Var(&o.peakBytes, "peak-bytes", spmm.DefaultPeakMemoryBytes, "hard peak-memory budget in bytes")
	fs.IntVar(&o.rowBlock, "row-block", 0, "manual row block size (0 selects automatically)")
	fs.IntVar(&o.colBlock, "col-block", 0, "manual sparse accumulation column block size (0 selects automatically)")
	fs.Uint64Var(&o.errorSeed, "error-seed", spmm.DefaultErrorSeed, "deterministic error-sampling seed")
	fs.IntVar(&o.errorSamples, "error-samples", spmm.DefaultErrorSamples, "high-precision samples per output block")
	fs.Float64Var(&o.absTol, "abs-tol", spmm.DefaultAbsTolerance, "absolute sampled-error tolerance")
	fs.Float64Var(&o.relTol, "rel-tol", spmm.DefaultRelTolerance, "relative sampled-error tolerance")
	fs.Float64Var(&o.interactiveMS, "interactive-budget-ms", 50, "default interactive latency budget")
	return o
}

func ensureManifest(dir string, seed int64) (*samples.Manifest, error) {
	m, err := samples.LoadManifest(dir)
	if err == nil {
		return m, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return samples.GenerateAll(dir, seed)
}

func (o *planOptions) loadSample() (*samples.Manifest, *samples.Sample, error) {
	m, err := ensureManifest(o.samplesDir, o.genSeed)
	if err != nil {
		return nil, nil, err
	}
	s, err := m.Load(o.samplesDir, o.sample)
	if err != nil {
		return nil, nil, err
	}
	return m, s, nil
}

func (o *planOptions) selectPlan(s *samples.Sample) (*spmm.Plan, error) {
	return spmm.ChoosePlan(spmm.PlanRequest{
		Matrix: s.CSR, Dense: s.Dense, CacheBudgetBytes: o.cacheBytes, PeakMemoryBytes: o.peakBytes,
		SumStrategy: spmm.SumStrategy(o.sum), AccumulationOrder: spmm.AccumulationOrder(o.order),
		SkipExplicitZeros: o.skipZeros, RowBlockSize: o.rowBlock, ColBlockSize: o.colBlock,
		ErrorSeed: o.errorSeed, ErrorSampleCount: o.errorSamples,
		AbsTolerance: o.absTol, RelTolerance: o.relTol, InteractiveBudgetMS: o.interactiveMS,
	})
}

func planExpectation(ctx context.Context, st *store.Store, key string) (store.PlanExpectation, error) {
	head, err := st.Head(ctx, key)
	if err == nil {
		return store.PlanExpectation{Version: head.Version, Digest: head.Digest}, nil
	}
	if isStateMissing(err) {
		return store.PlanExpectation{}, nil
	}
	return store.PlanExpectation{}, err
}

func publishChosen(ctx context.Context, st *store.Store, p *spmm.Plan) (*store.PlanRecord, error) {
	expect, err := planExpectation(ctx, st, spmm.PlanKey(p))
	if err != nil {
		return nil, err
	}
	return st.PublishPlan(ctx, p, expect)
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	out := fs.String("out", "artifacts/samples", "output directory")
	seed := fs.Int64("seed", samples.DefaultSeed, "fixed generator seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	m, err := samples.GenerateAll(*out, *seed)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(struct {
		Directory string   `json:"directory"`
		Seed      int64    `json:"seed"`
		Samples   []string `json:"samples"`
	}{*out, *seed, m.Names()}, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdPlan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ContinueOnError)
	o := addPlanFlags(fs, true)
	resultFile := fs.String("result-file", "", "optional JSON result path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, s, err := o.loadSample()
	if err != nil {
		return err
	}
	p, err := o.selectPlan(s)
	if err != nil {
		return err
	}
	ctx := context.Background()
	st, err := store.Open(ctx, o.db)
	if err != nil {
		return err
	}
	defer st.Close()
	rec, err := publishChosen(ctx, st, p)
	if err != nil {
		return err
	}
	if err := writeJSON(*resultFile, rec); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rec, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdPublishOnce(args []string) error {
	fs := flag.NewFlagSet("publish-once", flag.ContinueOnError)
	o := addPlanFlags(fs, true)
	expectedVersion := fs.Int64("expect-version", 0, "expected current plan version")
	expectedDigest := fs.String("expect-digest", "", "expected current plan digest")
	delayMS := fs.Int("delay-ms", 0, "delay before acquiring the publication lock")
	resultFile := fs.String("result-file", "", "write success or structured error JSON here")
	if err := fs.Parse(args); err != nil {
		return err
	}
	_, s, err := o.loadSample()
	if err != nil {
		return err
	}
	p, err := o.selectPlan(s)
	if err != nil {
		return err
	}
	if *delayMS > 0 {
		time.Sleep(time.Duration(*delayMS) * time.Millisecond)
	}
	ctx := context.Background()
	st, err := store.Open(ctx, o.db)
	if err != nil {
		return err
	}
	defer st.Close()
	rec, pubErr := st.PublishPlan(ctx, p, store.PlanExpectation{Version: *expectedVersion, Digest: *expectedDigest})
	payload := map[string]any{"ok": pubErr == nil}
	if pubErr == nil {
		payload["plan_key"] = rec.PlanKey
		payload["version"] = rec.Version
		payload["digest"] = rec.Digest
	} else {
		payload["plan_key"] = spmm.PlanKey(p)
		if e, ok := spmm.AsError(pubErr); ok {
			payload["error"] = e
		} else {
			payload["error"] = pubErr.Error()
		}
	}
	if err := writeJSON(*resultFile, payload); err != nil {
		return err
	}
	b, _ := json.Marshal(payload)
	fmt.Println(string(b))
	return pubErr
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	o := addPlanFlags(fs, true)
	planKey := fs.String("plan-key", "", "published plan key (if empty, derives the key from plan flags)")
	start := fs.Int("start", 0, "range start row (inclusive)")
	end := fs.Int("end", -1, "range end row (exclusive; -1 means full matrix)")
	persist := fs.Bool("persist", true, "commit completed row-block units")
	gracefulAfter := fs.Int("graceful-after", 0, "request graceful stop after N committed units")
	hardExitBefore := fs.Int("hard-exit-before-commit", -1, "simulate forced termination before committing unit N")
	resultFile := fs.String("result-file", "", "write stable (elapsed-free) result JSON")
	quiet := fs.Bool("quiet", false, "suppress result JSON on stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	_, s, err := o.loadSample()
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, o.db)
	if err != nil {
		return err
	}
	defer st.Close()
	key := *planKey
	if key == "" {
		p, err := o.selectPlan(s)
		if err != nil {
			return err
		}
		key = spmm.PlanKey(p)
	}
	en, err := engine.Open(ctx, st, s.CSR, s.Dense, key)
	if err != nil {
		return err
	}
	stop := make(chan struct{})
	var stopOnce sync.Once
	requestStop := func() { stopOnce.Do(func() { close(stop) }) }
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	defer signal.Stop(sig)
	go func() {
		<-sig
		requestStop()
	}()
	hook := func(ev engine.ExecutionEvent) error {
		if *gracefulAfter > 0 && ev.Kind == engine.EventAfterCommit && ev.UnitIndex+1 >= *gracefulAfter {
			requestStop()
		}
		if *hardExitBefore >= 0 && ev.Kind == engine.EventBeforeCommit && ev.UnitIndex == *hardExitBefore {
			os.Exit(137)
		}
		return nil
	}
	runOpts := engine.RunOptions{Persist: *persist, Stop: stop, FaultHook: hook}
	var result *engine.Result
	if *end < 0 {
		*end = s.CSR.Rows
	}
	if *start == 0 && *end == s.CSR.Rows {
		result, err = en.Settle(ctx, runOpts)
	} else {
		result, err = en.SettleRange(ctx, *start, *end, runOpts)
	}
	if err != nil {
		return err
	}
	if *resultFile != "" {
		b, err := engine.StableJSON(result)
		if err != nil {
			return err
		}
		if err := writeBytes(*resultFile, b); err != nil {
			return err
		}
	}
	if !*quiet {
		b, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(b))
	}
	return nil
}

func cmdQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	o := addPlanFlags(fs, true)
	planKey := fs.String("plan-key", "", "published plan key (if empty, derives the key from plan flags)")
	start := fs.Int("start", 0, "query start row (inclusive)")
	end := fs.Int("end", -1, "query end row (exclusive; -1 means full matrix)")
	budgetMS := fs.Float64("latency-ms", 50, "interactive latency budget in milliseconds")
	queryID := fs.String("query-id", "", "optional stable query id")
	resultFile := fs.String("result-file", "", "optional JSON result path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	_, s, err := o.loadSample()
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, o.db)
	if err != nil {
		return err
	}
	defer st.Close()
	key := *planKey
	if key == "" {
		p, err := o.selectPlan(s)
		if err != nil {
			return err
		}
		key = spmm.PlanKey(p)
	}
	en, err := engine.Open(ctx, st, s.CSR, s.Dense, key)
	if err != nil {
		return err
	}
	if *end < 0 {
		*end = s.CSR.Rows
	}
	result, err := en.Query(ctx, engine.QueryOptions{
		RowStart: *start, RowEnd: *end, QueryID: *queryID,
		LatencyBudget: time.Duration(*budgetMS * float64(time.Millisecond)),
	})
	if err != nil {
		return err
	}
	if err := writeJSON(*resultFile, result); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(b))
	return nil
}

func cmdReconcile(args []string) error {
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	db := fs.String("db", "artifacts/spmm.db", "SQLite database path")
	key := fs.String("plan-key", "", "plan key to reconcile")
	version := fs.Int64("expected-version", 0, "externally observed plan version (0 omits expectation)")
	digest := fs.String("expected-digest", "", "externally observed plan digest")
	resultFile := fs.String("result-file", "", "optional JSON report path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *key == "" {
		return failUsage(fs, "--plan-key is required")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, *db)
	if err != nil {
		return err
	}
	defer st.Close()
	var expected *store.PlanRef
	if *version != 0 || *digest != "" {
		expected = &store.PlanRef{PlanKey: *key, Version: *version, Digest: *digest}
	}
	report, err := st.ReconcilePlan(ctx, *key, expected)
	if err != nil {
		return err
	}
	if err := writeJSON(*resultFile, report); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	return nil
}

type verifySampleReport struct {
	Name             string  `json:"name"`
	Shape            string  `json:"shape"`
	DeclaredSparsity float64 `json:"declared_sparsity"`
	ObservedSparsity float64 `json:"observed_sparsity"`
	PlanKey          string  `json:"plan_key"`
	PlanVersion      int64   `json:"plan_version"`
	PlanDigest       string  `json:"plan_digest"`
	RowBlockSize     int     `json:"row_block_size"`
	ColBlockSize     int     `json:"col_block_size"`
	ExecutionUnits   int     `json:"execution_units"`
	MaxPeakEstimate  int64   `json:"max_peak_estimate_bytes"`
	SettlementDigest string  `json:"settlement_canonical_digest"`
	QueryDigest      string  `json:"query_canonical_digest"`
	PathsMatch       bool    `json:"paths_match"`
	MaxAbsError      float64 `json:"max_abs_sampled_error"`
	MaxRelError      float64 `json:"max_rel_sampled_error"`
	ErrorSamples     int     `json:"error_samples"`
}

type verifyReport struct {
	AllMatch bool                 `json:"all_match"`
	Samples  []verifySampleReport `json:"samples"`
}

func verifyCorpus(ctx context.Context, o *planOptions, names []string) (*verifyReport, error) {
	m, err := ensureManifest(o.samplesDir, o.genSeed)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		names = m.Names()
	}
	st, err := store.Open(ctx, o.db)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	report := &verifyReport{AllMatch: true}
	for _, name := range names {
		s, err := m.Load(o.samplesDir, name)
		if err != nil {
			return nil, err
		}
		p, err := o.selectPlan(s)
		if err != nil {
			return nil, fmt.Errorf("%s: choose plan: %w", name, err)
		}
		rec, err := publishChosen(ctx, st, p)
		if err != nil {
			return nil, fmt.Errorf("%s: publish plan: %w", name, err)
		}
		en, err := engine.Open(ctx, st, s.CSR, s.Dense, rec.PlanKey)
		if err != nil {
			return nil, err
		}
		q, err := en.Query(ctx, engine.QueryOptions{RowStart: 0, RowEnd: s.CSR.Rows, LatencyBudget: 30 * time.Second})
		if err != nil {
			return nil, fmt.Errorf("%s: query: %w", name, err)
		}
		settled, err := en.Settle(ctx, engine.RunOptions{Persist: true})
		if err != nil {
			return nil, fmt.Errorf("%s: settle: %w", name, err)
		}
		matchErr := engine.ComparePaths(q, settled)
		sr := verifySampleReport{
			Name: name, Shape: s.Shape, DeclaredSparsity: s.DeclaredSparsity,
			ObservedSparsity: s.Stats.ObservedSparsity,
			PlanKey:          rec.PlanKey, PlanVersion: rec.Version, PlanDigest: rec.Digest,
			RowBlockSize: p.RowBlockSize, ColBlockSize: p.ColBlockSize,
			ExecutionUnits: len(p.Memory.Rows), MaxPeakEstimate: p.Memory.MaxEstimateBytes,
			SettlementDigest: settled.CanonicalDigest, QueryDigest: q.CanonicalDigest,
			PathsMatch: matchErr == nil,
		}
		for _, b := range settled.Blocks {
			if b.Error.MaxAbsDeviation > sr.MaxAbsError {
				sr.MaxAbsError = b.Error.MaxAbsDeviation
			}
			if b.Error.MaxRelativeDeviation > sr.MaxRelError {
				sr.MaxRelError = b.Error.MaxRelativeDeviation
			}
			sr.ErrorSamples += b.Error.SampleCount
			if b.Error.ReferenceMethod == "" || len(b.Error.Samples) != b.Error.SampleCount {
				sr.PathsMatch = false
			}
		}
		if matchErr != nil {
			report.AllMatch = false
			return nil, fmt.Errorf("%s: %w", name, matchErr)
		}
		report.Samples = append(report.Samples, sr)
	}
	return report, nil
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	o := addPlanFlags(fs, false)
	name := fs.String("sample", "", "verify one sample (default: all)")
	reportFile := fs.String("report", "artifacts/verification.json", "write the verification report here")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var names []string
	if *name != "" {
		names = []string{*name}
	}
	report, err := verifyCorpus(context.Background(), o, names)
	if err != nil {
		return err
	}
	if err := writeJSON(*reportFile, report); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	return nil
}
