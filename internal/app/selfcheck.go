package app

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"injectctl/internal/codec"
	"injectctl/internal/model"
	"injectctl/internal/parse"
	"injectctl/internal/report"
	"injectctl/internal/samplegen"
	"injectctl/internal/scheduler"
	"injectctl/internal/store"
)

// CheckResult is one anomaly-injection assertion outcome.
type CheckResult struct {
	Name   string `json:"name"`
	Code   string `json:"expected_code"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail"`
}

// SelfCheckReport is the deterministic output of the five injection checks.
type SelfCheckReport struct {
	AllPassed bool          `json:"all_passed"`
	Checks    []CheckResult `json:"checks"`
}

// RunSelfCheck executes the five mandated anomaly-injection scenarios.
func RunSelfCheck(workDir string) (SelfCheckReport, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return SelfCheckReport{}, err
	}
	out := SelfCheckReport{}
	out.Checks = append(out.Checks, checkEmptyPayload(filepath.Join(workDir, "c1")))
	out.Checks = append(out.Checks, checkContextOOB(filepath.Join(workDir, "c2")))
	out.Checks = append(out.Checks, checkSnapshotGap(filepath.Join(workDir, "c3")))
	out.Checks = append(out.Checks, checkConcurrency(filepath.Join(workDir, "c4")))
	out.Checks = append(out.Checks, checkEncodeLimit(filepath.Join(workDir, "c5")))

	out.AllPassed = true
	for _, c := range out.Checks {
		if !c.Passed {
			out.AllPassed = false
		}
	}
	return out, nil
}

func checkEmptyPayload(dir string) CheckResult {
	res := CheckResult{Name: "empty_payload", Code: model.CodeEmptyPayload}
	sample := &model.Sample{
		ID: "s-empty", Seq: 1, Surface: "sql",
		Landing:   model.LandingStringLiteral,
		Encodings: []string{model.EncodingSingleQuoteEscape},
	}
	verdict, err := codec.Adjudicate(sample, 0)
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	if verdict.Code == model.CodeEmptyPayload && verdict.RawOffset == -1 && verdict.LandedOffset == -1 {
		res.Passed = true
	} else {
		res.Detail = fmt.Sprintf("got code=%s offsets=(%d,%d)", verdict.Code, verdict.RawOffset, verdict.LandedOffset)
	}
	return res
}

func checkContextOOB(dir string) CheckResult {
	res := CheckResult{Name: "context_out_of_bounds", Code: model.CodeContextOutOfBounds}
	doc := model.ContextFile{
		ContextVersion: samplegen.ContextVersion,
		Declarations: []model.ContextDecl{{
			ID: "s-oob", Surface: "sql", Landing: model.LandingStringLiteral,
			Encodings: []string{model.EncodingSingleQuoteEscape},
		}},
	}
	blocks := []model.PayloadBlock{{
		ID: "s-oob", Surface: "command", Raw: "whoami", SourceOffset: 0,
	}}
	out := parse.BuildSamples(doc, blocks)
	if len(out.Samples) == 0 && len(out.Errors) == 1 &&
		out.Errors[0].Code == model.CodeContextOutOfBounds {
		res.Passed = true
	} else {
		res.Detail = fmt.Sprintf("samples=%d errors=%d", len(out.Samples), len(out.Errors))
	}
	return res
}

func checkSnapshotGap(dir string) CheckResult {
	res := CheckResult{Name: "snapshot_version_gap", Code: model.CodeSnapshotVersion}
	os.MkdirAll(dir, 0o755)
	st, err := store.Open(filepath.Join(dir, "gap.db"))
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	defer st.Close()

	verdicts := []model.Verdict{{SampleID: "s1", Seq: 1, Code: model.CodeOK}}
	for v := 1; v <= 3; v++ {
		prev := ""
		if v > 1 {
			chain, _ := st.LoadChain()
			last := chain.Snapshots[len(chain.Snapshots)-1]
			prev = store.ChainSummaryHash(last.SummaryHash, last.ContentHash)
		}
		snap := store.NewSnapshot(v, fmt.Sprintf("b%d", v), samplegen.ContextVersion, verdicts, nil, prev)
		if err := st.SaveSnapshot(snap); err != nil {
			res.Detail = err.Error()
			return res
		}
	}
	if err := st.ForceGap(2); err != nil {
		res.Detail = err.Error()
		return res
	}
	chain, err := st.LoadChain()
	if err == nil {
		res.Detail = "expected E_SNAPSHOT_VERSION but chain loaded"
		return res
	}
	ve, ok := err.(*store.VersionError)
	if ok && ve.Code == model.CodeSnapshotVersion && ve.LastContinuous == 1 &&
		chain.ErrorCode == model.CodeSnapshotVersion && len(chain.Snapshots) == 1 {
		res.Passed = true
	} else {
		res.Detail = fmt.Sprintf("err=%v chain=%+v", err, chain)
	}
	return res
}

func checkConcurrency(dir string) CheckResult {
	res := CheckResult{Name: "concurrent_batch_contention", Code: model.CodeOK}
	os.MkdirAll(dir, 0o755)
	st, err := store.Open(filepath.Join(dir, "contend.db"))
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	defer st.Close()

	sched := scheduler.New(scheduler.Config{
		MaxConcurrent: 2, MaxQueueDepth: 8,
		MaxSamplesPerBatch: 0, MaxEncodedBytes: 0,
	}, st)
	sched.Start()

	const batches = 6
	for b := 0; b < batches; b++ {
		var samples []model.Sample
		for i := 0; i < 3; i++ {
			samples = append(samples, model.Sample{
				ID: fmt.Sprintf("b%d-s%d", b, i), Seq: i + 1, Surface: "path",
				Landing:   model.LandingPathSegment,
				Encodings: []string{model.EncodingPathNormalize},
				Raw:       "plain-segment",
			})
		}
		if err := sched.Submit(scheduler.Job{
			BatchID:        fmt.Sprintf("contend-%02d", b),
			ContextVersion: samplegen.ContextVersion,
			Samples:        samples,
		}); err != nil {
			res.Detail = err.Error()
			return res
		}
	}
	sched.WaitIdle()
	sched.Shutdown()

	results := sched.AllResults()
	okCount := 0
	ids := map[string]bool{}
	for _, r := range results {
		ids[r.BatchID] = true
		if r.Code == model.CodeOK && len(r.Verdicts) == 3 {
			okCount++
		}
	}
	chain, err := st.LoadChain()
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	if okCount == batches && len(ids) == batches && len(chain.Snapshots) == batches {
		res.Passed = true
	} else {
		res.Detail = fmt.Sprintf("ok=%d unique=%d snapshots=%d", okCount, len(ids), len(chain.Snapshots))
	}
	return res
}

func checkEncodeLimit(dir string) CheckResult {
	res := CheckResult{Name: "encode_byte_limit", Code: model.CodeEncodeLimit}
	os.MkdirAll(dir, 0o755)
	st, err := store.Open(filepath.Join(dir, "limit.db"))
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	defer st.Close()

	big := strings.Repeat("A", 32) + "'"
	small := "tiny"

	bigJob := scheduler.Job{
		BatchID: "limit-batch", ContextVersion: samplegen.ContextVersion,
		Samples: []model.Sample{{
			ID: "big", Seq: 1, Surface: "sql",
			Landing:   model.LandingStringLiteral,
			Encodings: []string{model.EncodingSingleQuoteEscape},
			Raw:       big,
		}},
	}
	cleanJob := scheduler.Job{
		BatchID: "clean-batch", ContextVersion: samplegen.ContextVersion,
		Samples: []model.Sample{{
			ID: "small", Seq: 1, Surface: "sql",
			Landing:   model.LandingStringLiteral,
			Encodings: []string{model.EncodingSingleQuoteEscape},
			Raw:       small,
		}},
	}

	sched := scheduler.New(scheduler.Config{
		MaxConcurrent: 1, MaxQueueDepth: 4, MaxSamplesPerBatch: 10,
		MaxEncodedBytes: 16,
	}, st)
	sched.Start()
	if err := sched.Submit(bigJob); err != nil {
		res.Detail = err.Error()
		return res
	}
	if err := sched.Submit(cleanJob); err != nil {
		res.Detail = err.Error()
		return res
	}
	sched.WaitIdle()
	sched.Shutdown()

	bigRes, ok1 := sched.Result("limit-batch")
	cleanRes, ok2 := sched.Result("clean-batch")
	if !ok1 || !ok2 {
		res.Detail = "missing batch results"
		return res
	}
	if len(bigRes.Verdicts) == 1 && bigRes.Verdicts[0].Code == model.CodeEncodeLimit &&
		cleanRes.Code == model.CodeOK && cleanRes.Verdicts[0].Code == model.CodeOK {
		res.Passed = true
	} else {
		res.Detail = fmt.Sprintf("big=%+v clean=%+v", bigRes.Code, cleanRes.Code)
	}
	return res
}

// BudgetCheck verifies E_SAMPLE_BUDGET terminates only the offending batch.
func BudgetCheck(workDir string) CheckResult {
	res := CheckResult{Name: "sample_budget", Code: model.CodeSampleBudget}
	st, err := store.Open(filepath.Join(workDir, "budget.db"))
	if err != nil {
		res.Detail = err.Error()
		return res
	}
	defer st.Close()

	mkSamples := func(prefix string, n int) []model.Sample {
		var out []model.Sample
		for i := 0; i < n; i++ {
			out = append(out, model.Sample{
				ID: fmt.Sprintf("%s-%d", prefix, i), Seq: i + 1, Surface: "path",
				Landing:   model.LandingPathSegment,
				Encodings: []string{model.EncodingPathNormalize},
				Raw:       "seg",
			})
		}
		return out
	}
	sched := scheduler.New(scheduler.Config{
		MaxConcurrent: 1, MaxQueueDepth: 4, MaxSamplesPerBatch: 3,
	}, st)
	sched.Start()
	_ = sched.Submit(scheduler.Job{BatchID: "over-budget", Samples: mkSamples("o", 6)})
	_ = sched.Submit(scheduler.Job{BatchID: "within-budget", Samples: mkSamples("w", 2)})
	sched.WaitIdle()
	sched.Shutdown()

	over, _ := sched.Result("over-budget")
	within, _ := sched.Result("within-budget")
	if over.Code == model.CodeSampleBudget && within.Code == model.CodeOK && len(within.Verdicts) == 2 {
		res.Passed = true
	} else {
		res.Detail = fmt.Sprintf("over=%s within=%s", over.Code, within.Code)
	}
	return res
}

// SortCheckResults keeps report ordering stable.
func SortCheckResults(cs []CheckResult) {
	sort.Slice(cs, func(i, j int) bool { return cs[i].Name < cs[j].Name })
}

var _ = report.Document{}
