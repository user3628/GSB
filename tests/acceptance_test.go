package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"multirecall/internal/configx"
	"multirecall/internal/model"
	"multirecall/internal/orchestrator"
	"multirecall/internal/recall"
	"multirecall/internal/sample"
	"multirecall/internal/server"
	"multirecall/internal/store"
	"multirecall/internal/textnorm"
)

func TestNormalizationChineseBigramsAndFullWidthLatin(t *testing.T) {
	got := textnorm.Tokenize("  拍照　ＲＵＮＮＩＮＧ  shoes ")
	if got.Normalized != "拍照 running shoes" {
		t.Fatalf("unexpected normalized text: %q", got.Normalized)
	}
	terms := map[string]bool{}
	for _, term := range got.UniqueTerms {
		terms[term] = true
	}
	for _, want := range []string{"拍", "照", "拍照", "running", "shoes"} {
		if !terms[want] {
			t.Fatalf("missing token %q in %v", want, got.UniqueTerms)
		}
	}
}

func TestDefaultSampleThreeRoutesFusionTieAndStability(t *testing.T) {
	ds := sample.Generate()
	cfg := configx.DefaultSnapshot()
	eng := recall.NewEngine(ds, cfg)
	report, err := eng.RunAll(recall.RunOptions{Context: context.Background(), Workers: 1})
	if err != nil {
		t.Fatal(err)
	}

	foundUnstable := false
	foundTie := false
	for _, qr := range report.Queries {
		if qr.Error != nil {
			if qr.Query.QueryID == "Q900" && qr.Error.Code == model.CodeEmptyQuery {
				continue
			}
			t.Fatalf("query %s unexpected error: %v", qr.Query.QueryID, qr.Error)
		}
		if qr.Lexical == nil || qr.Category == nil || qr.Vector == nil {
			t.Fatalf("query %s missing route results", qr.Query.QueryID)
		}
		if qr.Lexical.Returned == 0 || qr.Category.Returned == 0 || qr.Vector.Returned == 0 {
			t.Fatalf("query %s has empty route: lex=%d cat=%d vec=%d", qr.Query.QueryID, qr.Lexical.Returned, qr.Category.Returned, qr.Vector.Returned)
		}
		if qr.Audit == nil || !qr.Audit.Passed {
			t.Fatalf("query %s numeric audit did not pass: %+v", qr.Query.QueryID, qr.Audit)
		}
		for _, sc := range qr.Audit.SamplingChecks {
			if !sc.DigitByDigitMatch {
				t.Fatalf("query %s sampling mismatch at %d: %s vs %s", qr.Query.QueryID, sc.Position, sc.ImplementationDigits, sc.ReferenceDigits)
			}
		}
		seenItems := map[string]bool{}
		var sameScoreIDs []string
		lastFP := int64(-1)
		for i, hit := range qr.Hits {
			if seenItems[hit.ItemID] {
				t.Fatalf("duplicate fused item %s", hit.ItemID)
			}
			seenItems[hit.ItemID] = true
			if hit.Rank != i+1 {
				t.Fatalf("rank mismatch at %d", i)
			}
			if hit.Stability != nil && hit.Stability.Unstable {
				foundUnstable = true
			}
			if hit.FusionScoreFP == lastFP {
				sameScoreIDs = append(sameScoreIDs, hit.ItemID)
			} else {
				if len(sameScoreIDs) >= 2 {
					foundTie = true
					for i := 1; i < len(sameScoreIDs); i++ {
						if sameScoreIDs[i-1] >= sameScoreIDs[i] {
							t.Fatalf("tie is not item_id ascending: %v", sameScoreIDs)
						}
					}
				}
				sameScoreIDs = []string{hit.ItemID}
			}
			lastFP = hit.FusionScoreFP
		}
		if len(sameScoreIDs) >= 2 {
			foundTie = true
		}
	}
	if !foundUnstable {
		t.Fatal("expected at least one adjudicated unstable adjacent rank")
	}
	if !foundTie {
		t.Fatal("expected at least one exact fusion tie to verify item_id ordering")
	}
}

func TestParallelOutputByteIdentical(t *testing.T) {
	ds := sample.Generate()
	cfg := configx.DefaultSnapshot()
	eng := recall.NewEngine(ds, cfg)
	seq, err := eng.RunAll(recall.RunOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	parallel, err := eng.RunAll(recall.RunOptions{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	a := recall.CanonicalReport(seq)
	b := recall.CanonicalReport(parallel)
	if !bytes.Equal(a, b) {
		t.Fatalf("sequential and parallel reports differ: digest %s vs %s", seq.ResultDigest, parallel.ResultDigest)
	}
}

func TestEmptyZeroTermsAndEmptyIndex(t *testing.T) {
	ds := sample.Generate()
	cfg := configx.DefaultSnapshot()
	eng := recall.NewEngine(ds, cfg)
	qr := eng.ProcessQuery(model.Query{QueryID: "Q-EMPTY-UNIT", RawQuery: "！！！   "}, nil)
	if qr.Error == nil || qr.Error.Code != model.CodeEmptyQuery || qr.Error.QueryID != "Q-EMPTY-UNIT" {
		t.Fatalf("unexpected empty result: %+v", qr.Error)
	}

	emptyEng := recall.NewEngine(model.Dataset{VectorDim: ds.VectorDim}, cfg)
	qr = emptyEng.ProcessQuery(model.Query{QueryID: "Q-EMPTY-INDEX", RawQuery: "手机"}, nil)
	if qr.Error == nil || qr.Error.Code != model.CodeEmptyQuery || qr.Error.QueryID != "Q-EMPTY-INDEX" {
		t.Fatalf("expected E_EMPTY_QUERY for empty index, got %+v", qr.Error)
	}
}

func TestVectorInvalidCases(t *testing.T) {
	q := sample.Generate().Queries[0]
	cases := []struct {
		name   string
		mutate func(*model.Item)
		check  func(*testing.T, *model.APIError)
	}{
		{name: "dimension", mutate: func(item *model.Item) { item.VectorF32 = item.VectorF32[:3] }, check: func(t *testing.T, e *model.APIError) {
			if e.Dimension != sample.VectorDim || e.ActualDimension != 3 {
				t.Fatalf("unexpected dimension fields: %+v", e)
			}
		}},
		{name: "nan", mutate: func(item *model.Item) { item.VectorF32[2] = float32(math.NaN()) }, check: func(t *testing.T, e *model.APIError) {
			if e.NanIndex != 2 || e.Dimension != sample.VectorDim {
				t.Fatalf("unexpected nan fields: %+v", e)
			}
		}},
		{name: "zero norm", mutate: func(item *model.Item) {
			for i := range item.VectorF32 {
				item.VectorF32[i] = 0
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ds := sample.Generate()
			tc.mutate(&ds.Items[0])
			eng := recall.NewEngine(ds, configx.DefaultSnapshot())
			qr := eng.ProcessQuery(q, nil)
			if qr.Error == nil || qr.Error.Code != model.CodeVectorInvalid {
				t.Fatalf("expected E_VECTOR_INVALID, got %+v", qr.Error)
			}
			if qr.Error.ItemID != ds.Items[0].ItemID || qr.Error.QueryID != q.QueryID {
				t.Fatalf("error does not identify item/query: %+v", qr.Error)
			}
			if tc.check != nil {
				tc.check(t, qr.Error)
			}
		})
	}
}

func TestZeroWeightsAndNumericMismatch(t *testing.T) {
	ds := sample.Generate()
	cfg := configx.DefaultSnapshot()
	cfg.Fusion.Weights = configx.RouteWeights{}
	eng := recall.NewEngine(ds, cfg)
	qr := eng.ProcessQuery(ds.Queries[0], nil)
	if qr.Error == nil || qr.Error.Code != model.CodeNoWeight {
		t.Fatalf("expected E_NO_WEIGHT, got %+v", qr.Error)
	}

	cfg = configx.DefaultSnapshot()
	cfg.Fusion.NumericTolerance = 1e-12
	configx.RecalcDigest(&cfg)
	eng = recall.NewEngine(ds, cfg)
	qr = eng.ProcessQuery(ds.Queries[0], nil)
	if qr.Error == nil || qr.Error.Code != model.CodeNumericMismatch || qr.Error.MaxDeviation <= 0 || qr.Error.SourceRoute == "" {
		t.Fatalf("expected E_NUMERIC_MISMATCH with deviation/source, got %+v", qr.Error)
	}
}

func TestRRFModeAuditPasses(t *testing.T) {
	ds := sample.Generate()
	cfg := configx.DefaultSnapshot()
	cfg.Fusion.Method = "reciprocal_rank"
	configx.RecalcDigest(&cfg)
	rep, err := recall.NewEngine(ds, cfg).RunAll(recall.RunOptions{Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, qr := range rep.Queries {
		if qr.Error != nil {
			if qr.Error.Code == model.CodeEmptyQuery {
				continue
			}
			t.Fatal(qr.Error)
		}
		if qr.Audit == nil || !qr.Audit.Passed {
			t.Fatalf("RRF audit failed for %s", qr.Query.QueryID)
		}
	}
}

func TestBudgetReductionMatchesReducedConfig(t *testing.T) {
	ds := sample.Generate()
	cfg := configx.DefaultSnapshot()
	cfg.Budget.GlobalEvaluationLimit = 40
	configx.RecalcDigest(&cfg)
	eng := recall.NewEngine(ds, cfg)
	report, err := eng.RunAll(recall.RunOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	plan := recall.AdjudicateBudget(cfg, len(ds.Queries))
	if len(plan.Reductions) == 0 {
		t.Fatal("expected budget reductions")
	}

	reducedCfg := recall.ReducedConfig(cfg, plan)
	reducedReport, err := recall.NewEngine(ds, reducedCfg).RunAll(recall.RunOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	a := recall.CanonicalResultBytes(report.Queries)
	b := recall.CanonicalResultBytes(reducedReport.Queries)
	if !bytes.Equal(a, b) {
		t.Fatalf("reduced budget result does not match from-zero reduced config: digests %s vs %s", recall.ReportDigest(report.Queries), recall.ReportDigest(reducedReport.Queries))
	}
}

func TestConfigStaleBaseRollbackAndReplace(t *testing.T) {
	current := configx.DefaultSnapshot()
	_, _, err := configx.ApplyFragment(current, configx.Fragment{BaseVersion: 99, CandidateDepth: &configx.RouteDepths{Lexical: 3}})
	var apiErr *model.APIError
	if !asAPIError(err, &apiErr) || apiErr.Code != model.CodeStaleBase {
		t.Fatalf("expected E_STALE_BASE, got %v", err)
	}

	weights := configx.RouteWeights{Lexical: 0, Category: 0, Vector: 0}
	next, kind, err := configx.ApplyFragment(current, configx.Fragment{BaseVersion: 1, FusionWeights: &weights})
	if err != nil || kind != "incremental_fragment" || next.ConfigVersion != 2 {
		t.Fatalf("fragment failed: %v %s", err, kind)
	}
	_, _, err = configx.ClassifyReplacement(next, configx.Snapshot{ConfigVersion: 1})
	if !asAPIError(err, &apiErr) || apiErr.Code != model.CodeStaleVersion {
		t.Fatalf("expected E_STALE_VERSION rollback, got %v", err)
	}
	incoming := configx.DefaultSnapshot()
	incoming.ConfigVersion = 3
	incoming.Fusion.Truncation.TopK = 3
	accepted, kind, err := configx.ClassifyReplacement(next, incoming)
	if err != nil || kind != "whole_replace" || accepted.ConfigVersion != 3 {
		t.Fatalf("replacement failed: %v %s", err, kind)
	}
	if stage := configx.EarliestAffectedStage(next, accepted); stage != "fuse" {
		t.Fatalf("expected fuse invalidation, got %s", stage)
	}
}

func asAPIError(err error, target **model.APIError) bool {
	for err != nil {
		if apiErr, ok := err.(*model.APIError); ok {
			*target = apiErr
			return true
		}
		type unwrapper interface{ Unwrap() []error }
		if u, ok := err.(unwrapper); ok {
			for _, child := range u.Unwrap() {
				if asAPIError(child, target) {
					return true
				}
			}
			return false
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
			continue
		}
		return false
	}
	return false
}

func TestSQLiteCheckpointRecoveryByteIdentical(t *testing.T) {
	ds := sample.Generate()
	cfg := configx.DefaultSnapshot()
	dbPath := filepath.Join(t.TempDir(), "checkpoints.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	report, err := orchestrator.New(recall.NewEngine(ds, cfg), st).RunAll(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	first := recall.CanonicalReport(report)
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	recovered, err := orchestrator.New(recall.NewEngine(ds, cfg), st2).RunAll(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	second := recall.CanonicalReport(recovered)
	if !bytes.Equal(first, second) {
		t.Fatal("checkpoint recovery report is not byte identical")
	}
	views, err := st2.ListCheckpoints(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	valid := map[string]int{}
	for _, v := range views {
		if v.Status == "valid" {
			valid[v.QueryID+"\x00"+v.Stage]++
		}
	}
	for key, count := range valid {
		if count != 1 {
			t.Fatalf("expected one valid record for %s, got %d", key, count)
		}
	}
}

func TestCLISIGKILLRestartByteIdentical(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL recovery test is WSL/Linux specific")
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "multirecall")
	build := exec.Command("go", "build", "-o", bin, "./cmd/multirecall")
	build.Dir = root
	build.Env = append(os.Environ(), "GOPROXY=off")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v\n%s", err, out)
	}
	data := filepath.Join(dir, "data")
	clean := filepath.Join(dir, "clean.json")
	recovered := filepath.Join(dir, "recovered.json")
	dbPath := filepath.Join(dir, "cp.db")
	run := func(out, db string, kill int) error {
		args := []string{"run", "--data", data, "--out", out, "--mode", "sequential"}
		if db != "" {
			args = append(args, "--db", db)
		}
		if kill > 0 {
			args = append(args, "--kill-after-commits", "3")
		}
		cmd := exec.Command(bin, args...)
		return cmd.Run()
	}
	if err := run(clean, "", 0); err != nil {
		t.Fatal(err)
	}
	if err := run(filepath.Join(dir, "killed.json"), dbPath, 3); err == nil {
		t.Fatal("expected SIGKILL termination error")
	}
	if err := run(recovered, dbPath, 0); err != nil {
		t.Fatal(err)
	}
	a, err := os.ReadFile(clean)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(recovered)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("SIGKILL restart output is not byte identical to continuous run")
	}
}

func TestHTTPLoopbackEndpoints(t *testing.T) {
	app, err := server.NewApp(t.TempDir(), false)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(app.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(ts.URL + "/v1/recall?query_id=Q001")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Q001 status %d", resp.StatusCode)
	}
	var okReport recall.QueryReport
	if err := json.NewDecoder(resp.Body).Decode(&okReport); err != nil {
		t.Fatal(err)
	}
	if len(okReport.Hits) == 0 || okReport.Audit == nil {
		t.Fatal("expected hits and audit over HTTP")
	}

	resp, err = http.Get(ts.URL + "/v1/recall?query_id=Q900")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("empty query status %d", resp.StatusCode)
	}
	var errReport recall.QueryReport
	if err := json.NewDecoder(resp.Body).Decode(&errReport); err != nil {
		t.Fatal(err)
	}
	if errReport.Error == nil || errReport.Error.Code != model.CodeEmptyQuery {
		t.Fatalf("expected E_EMPTY_QUERY over HTTP, got %+v", errReport.Error)
	}

	body := bytes.NewBufferString(`{"base_version":999}`)
	resp, err = http.Post(ts.URL+"/v1/config/fragment", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale base status %d", resp.StatusCode)
	}
	var stale struct {
		Error *model.APIError `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stale); err != nil {
		t.Fatal(err)
	}
	if stale.Error == nil || stale.Error.Code != model.CodeStaleBase {
		t.Fatalf("expected E_STALE_BASE, got %+v", stale.Error)
	}
}

func TestConfigFragmentInvalidatesAndRebuildsCheckpoints(t *testing.T) {
	dir := t.TempDir()
	app, err := server.NewApp(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	ts := httptest.NewServer(app.Handler())
	defer ts.Close()

	post := func(path, body string) (*http.Response, map[string]any) {
		resp, err := http.Post(ts.URL+path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var decoded map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		return resp, decoded
	}

	resp, batch := post("/v1/recall/batch", `{"workers":1,"use_checkpoint":true}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("batch status %d", resp.StatusCode)
	}
	if batch["query_count"].(float64) != 10 {
		t.Fatalf("unexpected batch query count: %v", batch["query_count"])
	}
	views, err := app.DB.ListCheckpoints(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	validBefore := 0
	for _, v := range views {
		if v.Status == "valid" {
			validBefore++
		}
	}
	if validBefore != 65 {
		t.Fatalf("expected 65 valid stage records, got %d", validBefore)
	}

	resp, changed := post("/v1/config/fragment", `{"base_version":1,"fusion_weights":{"lexical":2.0,"category":1.0,"vector":1.5}}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fragment status %d body=%v", resp.StatusCode, changed)
	}
	if changed["earliest_affected_stage"] != "fuse" {
		t.Fatalf("unexpected affected stage: %v", changed["earliest_affected_stage"])
	}
	if changed["invalidated_checkpoints"].(float64) != 18 {
		t.Fatalf("expected 18 fuse/audit records invalidated, got %v", changed["invalidated_checkpoints"])
	}
	views, _ = app.DB.ListCheckpoints(context.Background())
	validAfterInvalidation := 0
	for _, v := range views {
		if v.Status == "valid" {
			validAfterInvalidation++
		}
	}
	if validAfterInvalidation != 47 {
		t.Fatalf("expected 47 valid records after invalidation, got %d", validAfterInvalidation)
	}

	resp, rebuilt := post("/v1/recall/batch", `{"workers":2,"use_checkpoint":true}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rebuild batch status %d", resp.StatusCode)
	}
	if cfg, ok := rebuilt["config"].(map[string]any); !ok || cfg["config_version"].(float64) != 2 {
		t.Fatalf("rebuilt batch did not use config v2: %v", rebuilt["config"])
	}
	views, _ = app.DB.ListCheckpoints(context.Background())
	validRebuilt := 0
	versions := map[int64]bool{}
	for _, v := range views {
		if v.Status == "valid" {
			validRebuilt++
			versions[v.ConfigVersion] = true
		}
	}
	if validRebuilt != 65 {
		t.Fatalf("expected 65 rebuilt valid records, got %d", validRebuilt)
	}
	if !versions[2] || versions[1] {
		t.Fatalf("expected all valid checkpoints at config v2, got versions=%v", versions)
	}
}
